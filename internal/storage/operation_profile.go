// operation_profile.go writes one JSON report per worker operation. A failed
// attempt is kept. Writing the report cannot change the operation's result.

package storage

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"beehive-hot-to-cold-loader/internal/control"
)

// operationStep is a step in an operation profile.
type operationStep struct {
	Name            string             `json:"name"` // the name of the step
	Seconds         float64            `json:"seconds"` // the seconds of the step
	Failed          bool               `json:"failed"` // whether the step failed
	UsageAvailable  bool               `json:"usage_available"` // whether usage is available
	SelfCPUSeconds  float64            `json:"self_cpu_seconds"` // the CPU seconds of the step
	ChildCPUSeconds float64            `json:"child_cpu_seconds"` // the CPU seconds of the step
	Comparison      *ComparisonProfile `json:"comparison,omitempty"` // the comparison profile of the step
}

// operationProfile is a profile of an operation.
type operationProfile struct {
	Job       string    `json:"job"` // the job of the profile
	Day       string    `json:"day"` // the day of the profile
	State     string    `json:"state"` // the state of the profile
	Operation string    `json:"operation"` // the operation of the profile
	StartedAt time.Time `json:"started_at"` // the started at time of the profile
	Threads      int             `json:"threads"` // the threads of the profile
	Memory       string          `json:"memory"` // the memory of the profile
	Passed       bool            `json:"passed"` // whether the profile passed
	TotalSeconds float64         `json:"total_seconds"` // the total seconds of the profile
	Steps        []operationStep `json:"steps"` // the steps of the profile
	CPUScope     string          `json:"cpu_scope"` // the CPU scope of the profile
	began        time.Time // the began time of the profile
}

// newOperationProfile starts a report for this job state and DuckDB session.
func newOperationProfile(job *control.Job, operation string, duck DuckDB) *operationProfile {
	now := time.Now()
	memory := duck.Memory
	if memory == "" {
		memory = "3GB"
	}
	return &operationProfile{Job: job.ID, Day: job.Day, State: job.State, Operation: operation, StartedAt: now.UTC(), Threads: duck.threadCount(), Memory: memory, CPUScope: "per-operation self and reaped-child CPU deltas; self includes other goroutines; nested comparison timings overlap step wall time; report serialization excluded", began: now}
}

// operationCPU returns accumulated user+system CPU for this process and its children.
func operationCPU() (self, child float64, available bool) {
	var a, b syscall.Rusage
	ea := syscall.Getrusage(syscall.RUSAGE_SELF, &a)
	eb := syscall.Getrusage(syscall.RUSAGE_CHILDREN, &b)
	seconds := func(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }
	return seconds(a.Utime) + seconds(a.Stime), seconds(b.Utime) + seconds(b.Stime), ea == nil && eb == nil
}

// measure appends one named step, including a step that fails.
func (p *operationProfile) measure(name string, fn func() error) error {
	began := time.Now()
	self, child, ok := operationCPU()
	e := fn()
	afterSelf, afterChild, afterOK := operationCPU()
	p.Steps = append(p.Steps, operationStep{Name: name, Seconds: time.Since(began).Seconds(), Failed: e != nil, UsageAvailable: ok && afterOK, SelfCPUSeconds: afterSelf - self, ChildCPUSeconds: afterChild - child})
	return e
}

// compare is measure plus the comparison profile captured during fn.
func (p *operationProfile) compare(duck DuckDB, name string, fn func(DuckDB) error) error {
	var detail ComparisonProfile
	duck.comparisonProfile = &detail
	e := p.measure(name, func() error { return fn(duck) })
	p.Steps[len(p.Steps)-1].Comparison = &detail
	return e
}

// save publishes the report with a unique name so a retry does not erase the previous attempt.
func (p *operationProfile) save(work string, operationError error) error {
	p.TotalSeconds = time.Since(p.began).Seconds()
	p.Passed = operationError == nil
	// Unique atomic reports retain failed/retried attempts; no SQL/error text,
	// signed URLs or tokens are serialized. State is data, not a path component.
	f, e := os.CreateTemp(work, "operation-"+p.Operation+"-*.json.pending")
	if e != nil {
		return e
	}
	pending := f.Name()
	defer os.Remove(pending)
	defer f.Close()
	if e = json.NewEncoder(f).Encode(p); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(pending, strings.TrimSuffix(pending, ".pending")); e != nil {
		return e
	}
	directory, e := os.Open(filepath.Dir(pending))
	if e != nil {
		return e
	}
	defer directory.Close()
	return directory.Sync()
}
func (p *operationProfile) finish(work string, operationError error) {
	// Observability must not retry an already completed external delete or change
	// a verification/state transition. The capacity harness requires the evidence.
	if e := p.save(work, operationError); e != nil {
		log.Printf("operation profile job %s: %v", p.Job, e)
	}
}
