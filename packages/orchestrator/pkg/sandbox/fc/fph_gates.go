package fc

import (
	"github.com/e2b-dev/infra/packages/shared/pkg/fcversion"
)

// FCSupportsFreePageHinting reports whether the FC version's API exposes
// virtio-balloon free-page-hinting. Kernel-side eligibility (and the race-fix
// requirement) is targeted via LaunchDarkly with kernel-version context.
func FCSupportsFreePageHinting(fcVersion string) bool {
	info, err := fcversion.New(fcVersion)
	if err != nil {
		return false
	}

	return info.HasFreePageHinting()
}
