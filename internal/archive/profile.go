// profile.go records one attempt's wall time and CPU. RSS figures are
// process-lifetime high-water marks, so they are not a per-step peak.

package archive

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"beehive-hot-to-cold-loader/internal/control"
)

var processStarted = time.Now().UTC()

type stepUsage struct {
	Self, Children syscall.Rusage
	Available      bool
}

// readStepUsage samples this process and every child it has reaped.
func readStepUsage() stepUsage {
	var u stepUsage
	a := syscall.Getrusage(syscall.RUSAGE_SELF, &u.Self)
	b := syscall.Getrusage(syscall.RUSAGE_CHILDREN, &u.Children)
	u.Available = a == nil && b == nil
	return u
}

// cpuSeconds converts Timeval to seconds.
func cpuSeconds(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }

// rssBytes returns Maxrss in bytes. Darwin already reports bytes; Linux reports KiB.
func rssBytes(r syscall.Rusage) int64 {
	if runtime.GOOS == "darwin" {
		return r.Maxrss
	}
	return r.Maxrss * 1024
}

// Profiles describe attempts, including failures. RSS values are process-lifetime
// high-water marks, not per-step peaks or a simultaneous parent/child sum.
func profileStep(job *control.Job, state string, began time.Time, before stepUsage, operationError error) error {
	if job.Work == "" || errors.Is(operationError, ErrWaiting) {
		return nil
	}
	after := readStepUsage()
	report := map[string]any{
		"job": job.ID, "day": job.Day, "mode": job.Mode, "state_before": state, "state_after": job.State,
		"started_at": began.UTC().Format(time.RFC3339Nano), "elapsed_seconds": time.Since(began).Seconds(),
		"operation_failed": operationError != nil, "usage_available": before.Available && after.Available,
		"self_user_cpu_seconds":    cpuSeconds(after.Self.Utime) - cpuSeconds(before.Self.Utime),
		"self_system_cpu_seconds":  cpuSeconds(after.Self.Stime) - cpuSeconds(before.Self.Stime),
		"child_user_cpu_seconds":   cpuSeconds(after.Children.Utime) - cpuSeconds(before.Children.Utime),
		"child_system_cpu_seconds": cpuSeconds(after.Children.Stime) - cpuSeconds(before.Children.Stime),
		"self_peak_rss_bytes":      rssBytes(after.Self), "largest_child_peak_rss_bytes": rssBytes(after.Children),
		"rss_scope":          "separate process-lifetime parent and largest reaped child high-water marks; not a combined peak",
		"process_started_at": processStarted.Format(time.RFC3339Nano),
	}
	f, e := os.CreateTemp(job.Work, "step-"+state+"-*.json.pending")
	if e != nil {
		return e
	}
	pending := f.Name()
	defer os.Remove(pending)
	if e = json.NewEncoder(f).Encode(report); e != nil {
		f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	target := pending[:len(pending)-len(".pending")]
	if e = os.Rename(pending, target); e != nil {
		return e
	}
	directory, e := os.Open(filepath.Dir(target))
	if e != nil {
		return e
	}
	defer directory.Close()
	if e = directory.Sync(); e != nil {
		return fmt.Errorf("sync step profile: %w", e)
	}
	return nil
}
