package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/orchestrator/cmd/internal/cmdutil"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// fphBenchOptions configures the FPH-vs-FPR comparison mode.
type fphBenchOptions struct {
	enabled    bool
	workload   string
	iterations int
	delay      time.Duration
}

// fphBenchDrainTimeout is the FPH drain timeout used in the FPR+FPH arm of the
// bench. Plenty for any single drain on hugetlbfs-2M-backed VMs.
const fphBenchDrainTimeout = 5 * time.Second

// fphBenchSample is a single iteration's measurement.
type fphBenchSample struct {
	memfileBytes int64
	resume       time.Duration
	pause        time.Duration
	err          error
}

// fphBench runs the workload N times under FPR-only and N times under FPR+FPH,
// uploading each snapshot to local storage and measuring the resulting memfile
// data size. Smaller is better — pages reclaimed by FPH/FPR show up as Empty
// mappings in the memfile header and therefore consume no data bytes.
func (r *runner) fphBench(ctx context.Context, opts fphBenchOptions) error {
	if opts.iterations <= 0 {
		return errors.New("fph-bench: iterations must be > 0")
	}

	fmt.Printf("📊 FPH bench (%d iterations per arm)\n", opts.iterations)
	fmt.Printf("   Workload: %s\n", opts.workload)
	if opts.delay > 0 {
		fmt.Printf("   Pause delay: %s (lets continuous FPR settle before drain)\n", opts.delay)
	}
	fmt.Println()

	noFph := make([]fphBenchSample, 0, opts.iterations)
	withFph := make([]fphBenchSample, 0, opts.iterations)

	for i := range opts.iterations {
		if ctx.Err() != nil {
			break
		}
		fmt.Printf("[%d/%d] FPR-only  ... ", i+1, opts.iterations)
		s := r.fphBenchOnce(ctx, opts, false)
		noFph = append(noFph, s)
		fmt.Println(fphBenchSampleString(s))

		if ctx.Err() != nil {
			break
		}
		fmt.Printf("[%d/%d] FPR + FPH ... ", i+1, opts.iterations)
		s = r.fphBenchOnce(ctx, opts, true)
		withFph = append(withFph, s)
		fmt.Println(fphBenchSampleString(s))
	}

	fmt.Println()
	printFphBenchSummary("FPR-only", noFph)
	printFphBenchSummary("FPR + FPH", withFph)
	printFphBenchDelta(noFph, withFph)

	return firstErr(noFph, withFph)
}

// fphBenchOnce runs one resume → workload → optional delay → pause cycle and
// returns the resulting memfile data size and timings. Toggles the FPH LD flags
// in the offline store so the path is identical to production.
func (r *runner) fphBenchOnce(ctx context.Context, opts fphBenchOptions, withFph bool) fphBenchSample {
	if withFph {
		featureflags.NewBoolFlag("free-page-hinting-install", true)
		featureflags.NewIntFlag("free-page-hinting-timeout-ms", int(fphBenchDrainTimeout/time.Millisecond))
	} else {
		featureflags.NewBoolFlag("free-page-hinting-install", false)
		featureflags.NewIntFlag("free-page-hinting-timeout-ms", 0)
	}

	buildID := uuid.New().String()
	runtime := sandbox.RuntimeMetadata{
		TemplateID:  r.buildID,
		TeamID:      "local",
		SandboxID:   fmt.Sprintf("fph-bench-%d", time.Now().UnixNano()),
		ExecutionID: fmt.Sprintf("fph-bench-exec-%d", time.Now().UnixNano()),
	}

	t0 := time.Now()
	sbx, err := r.factory.ResumeSandbox(ctx, r.tmpl, r.sbxConfig, runtime, t0, t0.Add(24*time.Hour), nil)
	resumeDur := time.Since(t0)
	if err != nil {
		return fphBenchSample{resume: resumeDur, err: fmt.Errorf("resume: %w", err)}
	}
	defer sbx.Close(context.WithoutCancel(ctx))

	if err := runCommandInSandbox(ctx, sbx, opts.workload); err != nil {
		return fphBenchSample{resume: resumeDur, err: fmt.Errorf("workload: %w", err)}
	}

	if opts.delay > 0 {
		select {
		case <-ctx.Done():
			return fphBenchSample{resume: resumeDur, err: ctx.Err()}
		case <-time.After(opts.delay):
		}
	}

	origMeta, err := r.tmpl.Metadata()
	if err != nil {
		return fphBenchSample{resume: resumeDur, err: fmt.Errorf("metadata: %w", err)}
	}
	newMeta := origMeta
	newMeta.Template.BuildID = buildID

	pauseStart := time.Now()
	snapshot, err := sbx.Pause(ctx, newMeta)
	pauseDur := time.Since(pauseStart)
	if err != nil {
		return fphBenchSample{resume: resumeDur, pause: pauseDur, err: fmt.Errorf("pause: %w", err)}
	}
	defer snapshot.Close(context.WithoutCancel(ctx))

	upload, err := sandbox.NewUpload(ctx, nil, snapshot, r.storage, storage.CompressConfig{}, nil, "", nil)
	if err != nil {
		return fphBenchSample{resume: resumeDur, pause: pauseDur, err: fmt.Errorf("upload prepare: %w", err)}
	}
	if err := upload.Run(ctx); err != nil {
		return fphBenchSample{resume: resumeDur, pause: pauseDur, err: fmt.Errorf("upload: %w", err)}
	}

	memfileBytes, err := readLocalMemfileSize(buildID)
	if err != nil {
		return fphBenchSample{resume: resumeDur, pause: pauseDur, err: fmt.Errorf("memfile size: %w", err)}
	}

	// Reclaim the snapshot dir so a long bench doesn't fill up the disk.
	cleanupLocalBuild(buildID)

	return fphBenchSample{memfileBytes: memfileBytes, resume: resumeDur, pause: pauseDur}
}

// readLocalMemfileSize returns the on-disk size of the memfile data layer for
// a given buildID written to the local storage base path.
func readLocalMemfileSize(buildID string) (int64, error) {
	basePath := os.Getenv("LOCAL_TEMPLATE_STORAGE_BASE_PATH")
	if basePath == "" {
		return 0, errors.New("LOCAL_TEMPLATE_STORAGE_BASE_PATH not set; -fph-bench requires local storage")
	}
	path := filepath.Join(basePath, buildID, storage.MemfileName)

	return cmdutil.GetActualFileSize(path)
}

// cleanupLocalBuild removes the per-build snapshot directory so a long bench
// run doesn't accumulate hundreds of MB of memfile artifacts.
func cleanupLocalBuild(buildID string) {
	basePath := os.Getenv("LOCAL_TEMPLATE_STORAGE_BASE_PATH")
	if basePath == "" {
		return
	}
	_ = os.RemoveAll(filepath.Join(basePath, buildID))
}

func fphBenchSampleString(s fphBenchSample) string {
	if s.err != nil {
		return fmt.Sprintf("❌ %v", s.err)
	}

	return fmt.Sprintf("memfile=%s  (resume %s, pause %s)",
		fmtBytes(s.memfileBytes), fmtDur(s.resume), fmtDur(s.pause))
}

func printFphBenchSummary(label string, samples []fphBenchSample) {
	ok := successfulSamples(samples)
	if len(ok) == 0 {
		fmt.Printf("   %-9s: no successful samples\n", label)

		return
	}

	bytesAvg, bytesStd := meanStd(intSlice(ok, func(s fphBenchSample) int64 { return s.memfileBytes }))
	pauseAvg, _ := meanStd(intSlice(ok, func(s fphBenchSample) int64 { return int64(s.pause) }))

	fmt.Printf("   %-9s: memfile %s ± %s  pause avg %s  (n=%d)\n",
		label,
		fmtBytes(int64(bytesAvg)), fmtBytes(int64(bytesStd)),
		fmtDur(time.Duration(pauseAvg)),
		len(ok))
}

func printFphBenchDelta(noFph, withFph []fphBenchSample) {
	a := successfulSamples(noFph)
	b := successfulSamples(withFph)
	if len(a) == 0 || len(b) == 0 {
		return
	}

	noFphAvg, _ := meanStd(intSlice(a, func(s fphBenchSample) int64 { return s.memfileBytes }))
	withFphAvg, _ := meanStd(intSlice(b, func(s fphBenchSample) int64 { return s.memfileBytes }))

	delta := noFphAvg - withFphAvg
	pct := 0.0
	if noFphAvg > 0 {
		pct = delta / noFphAvg * 100
	}

	pauseNoFph, _ := meanStd(intSlice(a, func(s fphBenchSample) int64 { return int64(s.pause) }))
	pauseWithFph, _ := meanStd(intSlice(b, func(s fphBenchSample) int64 { return int64(s.pause) }))
	pauseDelta := pauseWithFph - pauseNoFph

	fmt.Println()
	fmt.Printf("   FPH freed extra: %s  (%.1f%% of FPR-only memfile)\n", fmtBytes(int64(delta)), pct)
	fmt.Printf("   Pause overhead : %s\n", fmtDur(time.Duration(pauseDelta)))
}

func successfulSamples(samples []fphBenchSample) []fphBenchSample {
	out := samples[:0:0]
	for _, s := range samples {
		if s.err == nil {
			out = append(out, s)
		}
	}

	return out
}

func intSlice[T any](items []T, f func(T) int64) []int64 {
	out := make([]int64, len(items))
	for i, x := range items {
		out[i] = f(x)
	}

	return out
}

func meanStd(xs []int64) (mean, std float64) {
	if len(xs) == 0 {
		return 0, 0
	}
	var sum float64
	for _, x := range xs {
		sum += float64(x)
	}
	mean = sum / float64(len(xs))

	if len(xs) < 2 {
		return mean, 0
	}
	var v float64
	for _, x := range xs {
		d := float64(x) - mean
		v += d * d
	}
	std = math.Sqrt(v / float64(len(xs)-1))

	return mean, std
}

func fmtBytes(n int64) string {
	if n < 0 {
		return "-" + fmtBytes(-n)
	}
	switch {
	case n < 1<<10:
		return fmt.Sprintf("%d B", n)
	case n < 1<<20:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	case n < 1<<30:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	default:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	}
}

func firstErr(groups ...[]fphBenchSample) error {
	for _, g := range groups {
		for _, s := range g {
			if s.err != nil {
				return s.err
			}
		}
	}

	return nil
}
