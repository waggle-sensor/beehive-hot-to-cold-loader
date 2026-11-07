// disk.go keeps native spill inside a fixed budget. The cap is 16 GiB after
// an 8 GiB reserve, and a full spill directory stops the job for a later retry.

package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"beehive-hot-to-cold-loader/internal/control"
)

const stagingReserve uint64 = 8 << 30 // stagingReserve is the amount of free space to reserve for the staging directory
const maxSpill uint64 = 16 << 30 // maxSpill is the maximum amount of free space to spill

// stagingSpace reports free bytes on the filesystem that will hold path.
// It walks to an existing parent when the spill directory has not been created.
func stagingSpace(path string) (uint64, error) {
	if path == "" {
		path = "."
	}
	for {
		var s syscall.Statfs_t
		e := syscall.Statfs(path, &s)
		if e == nil {
			free := uint64(s.Bavail) * uint64(s.Bsize)
			if _, e := spillBudget(free); e != nil {
				return free, e
			}
			return free, nil
		}
		if !os.IsNotExist(e) {
			return 0, e
		}
		parent := filepath.Dir(path)
		if parent == path {
			return 0, e
		}
		path = parent
	}
}

// capacityError maps DuckDB spill and disk-full failures onto ErrStagingCapacity.
func capacityError(e error) error {
	if e == nil {
		return nil
	}
	s := strings.ToLower(e.Error())
	if strings.Contains(s, "max_temp_directory_size") || strings.Contains(s, "failed to offload") || strings.Contains(s, "no space left on device") {
		return fmt.Errorf("%w: %v", control.ErrStagingCapacity, e)
	}
	return e
}

// spillBudget is min(16 GiB, free-8 GiB). Less than the reserve refuses the command.
func spillBudget(free uint64) (uint64, error) {
	if free <= stagingReserve {
		return 0, fmt.Errorf("%w: %d bytes available; reserve is %d", control.ErrStagingCapacity, free, stagingReserve)
	}
	return min(maxSpill, free-stagingReserve), nil
}
