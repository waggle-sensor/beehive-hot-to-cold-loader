// archive.go packs/unpacks verified recovery bundles and performs offline
// restore/approve under an exclusive writer.lock.
package statebackup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"syscall"
)

// Pack writes only inventoried files and the manifest, never arbitrary scratch.
func Pack(ctx context.Context, root, dest string) error {
	m, e := Verify(ctx, root)
	if e != nil {
		return e
	}
	out, e := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer out.Close()
	gz, e := gzip.NewWriterLevel(out, gzip.BestSpeed)
	if e != nil {
		return e
	}
	tw := tar.NewWriter(gz)
	names := []string{ManifestName}
	for name := range m.Files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		info, err := os.Stat(filepath.Join(root, name))
		if err != nil {
			return err
		}
		header := &tar.Header{Name: name, Mode: int64(info.Mode().Perm()), Size: info.Size(), Typeflag: tar.TypeReg}
		if e = tw.WriteHeader(header); e != nil {
			return e
		}
		f, err := os.Open(filepath.Join(root, name))
		if err != nil {
			return err
		}
		_, err = io.Copy(tw, contextReader{ctx, f})
		f.Close()
		if err != nil {
			return err
		}
	}
	if e = tw.Close(); e != nil {
		return e
	}
	if e = gz.Close(); e != nil {
		return e
	}
	return out.Sync()
}

// Unpack accepts only flat regular-file paths. Implied parent directories are
// created by us; archive links/devices/duplicate paths are rejected.
func Unpack(ctx context.Context, archive, target string) error {
	if e := CheckPath(target); e != nil {
		return e
	}
	if e := os.Mkdir(target, 0700); e != nil {
		return e
	}
	f, e := os.Open(archive)
	if e != nil {
		return e
	}
	defer f.Close()
	gz, e := gzip.NewReader(f)
	if e != nil {
		return e
	}
	defer gz.Close()
	gz.Multistream(false)
	tr := tar.NewReader(gz)
	seen := map[string]bool{}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if !SafeName(h.Name) || h.Typeflag != tar.TypeReg || seen[h.Name] || h.Size < 0 {
			return errors.New("unsafe archive entry")
		}
		seen[h.Name] = true
		p := filepath.Join(target, filepath.FromSlash(h.Name))
		if err = os.MkdirAll(filepath.Dir(p), 0700); err != nil {
			return err
		}
		out, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(h.Mode)&0700)
		if err != nil {
			return err
		}
		_, err = io.Copy(out, contextReader{ctx, tr})
		if err == nil {
			err = out.Sync()
		}
		closeErr := out.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
	}
	// Read past tar EOF to verify the gzip trailer/checksum as well.
	if _, e = io.Copy(io.Discard, contextReader{ctx, gz}); e != nil {
		return e
	}
	inventory, e := Load(target)
	if e != nil {
		return e
	}
	if e = createDirectories(target, inventory); e != nil {
		return e
	}
	m, e := Verify(ctx, target)
	if e != nil {
		return e
	}
	if len(seen) != len(m.Files)+1 {
		return errors.New("unexpected files in backup archive")
	}
	return nil
}

func lockState(root string) (*os.File, error) {
	if e := CheckPath(root); e != nil {
		return nil, e
	}
	info, e := os.Lstat(root)
	if e != nil {
		return nil, e
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("state root must be a real directory")
	}
	p := filepath.Join(root, "writer.lock")
	if st, err := os.Lstat(p); err == nil && !st.Mode().IsRegular() {
		return nil, errors.New("unsafe lock file")
	}
	f, e := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		f.Close()
		return nil, errors.New("state is in use; stop the coordinator first")
	}
	return f, nil
}

// Restore requires an empty precreated target and immediately installs the
// fail-closed marker. A failed/partial extraction remains blocked on startup.
func Restore(ctx context.Context, bundle, target string) error {
	target, e := filepath.Abs(target)
	if e != nil {
		return e
	}
	if e = CheckPath(target); e != nil {
		return e
	}
	lock, e := lockState(target)
	if e != nil {
		return e
	}
	defer lock.Close()
	entries, e := os.ReadDir(target)
	if e != nil {
		return e
	}
	for _, d := range entries {
		if d.Name() != "writer.lock" {
			return errors.New("restore target must be empty")
		}
	}
	if e = AtomicJSON(filepath.Join(target, PendingName), map[string]any{"pending": true, "bundle": filepath.Base(bundle)}); e != nil {
		return e
	}
	tmp, e := os.MkdirTemp(filepath.Dir(target), ".restore-*")
	if e != nil {
		return e
	}
	defer os.RemoveAll(tmp)
	extracted := filepath.Join(tmp, "snapshot")
	if e = Unpack(ctx, bundle, extracted); e != nil {
		return e
	}
	m, e := Verify(ctx, extracted)
	if e != nil {
		return e
	}
	if target != m.StatePath {
		return fmt.Errorf("restore requires original mount path %s", m.StatePath)
	}
	if os.Geteuid() != 0 && (os.Geteuid() != m.UID || os.Getegid() != m.GID) {
		return fmt.Errorf("restore must run as original UID/GID %d:%d or root", m.UID, m.GID)
	}
	if e = createDirectories(target, m); e != nil {
		return e
	}
	for name, entry := range m.Files {
		if name == PendingName || name == "writer.lock" || name == "recovery-approved.json" {
			return errors.New("reserved recovery filename")
		}
		dest := filepath.Join(target, name)
		if e = os.MkdirAll(filepath.Dir(dest), 0700); e != nil {
			return e
		}
		if e = copyFile(ctx, filepath.Join(extracted, name), dest, os.FileMode(entry.Mode)&0777); e != nil {
			return e
		}
	}
	for _, name := range []string{ManifestName} {
		if e = copyFile(ctx, filepath.Join(extracted, name), filepath.Join(target, name), 0600); e != nil {
			return e
		}
	}
	if e = AtomicJSON(filepath.Join(target, PendingName), map[string]any{"id": m.ID, "pending": true, "cutoff": m.CreatedAt}); e != nil {
		return e
	}
	if os.Geteuid() == 0 {
		// Deployment state has one owner. Also fix writer.lock so a non-root
		// coordinator can take its existing lock after privileged recovery.
		if e = filepath.WalkDir(target, func(p string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			return os.Chown(p, m.UID, m.GID)
		}); e != nil {
			return e
		}
	}
	for name, mode := range m.Directories {
		if e = os.Chmod(filepath.Join(target, filepath.FromSlash(name)), os.FileMode(mode)); e != nil {
			return e
		}
	}
	return syncDir(target)
}

func createDirectories(root string, m *Manifest) error {
	for name, mode := range m.Directories {
		if !SafeName(name) || mode > 0777 {
			return errors.New("unsafe directory inventory")
		}
		if e := os.MkdirAll(filepath.Join(root, filepath.FromSlash(name)), 0700); e != nil {
			return e
		}
	}
	return nil
}

// Approve is deliberately separate from offline extraction. Its remote client
// is read-only and validates publication and all captured object references.
func Approve(ctx context.Context, target string, remote Remote, acceptLoss bool) error {
	if !acceptLoss {
		return errors.New("explicit acceptance of post-backup data loss is required")
	}
	lock, e := lockState(target)
	if e != nil {
		return e
	}
	defer lock.Close()
	if _, e = os.Lstat(filepath.Join(target, PendingName)); e != nil {
		return e
	}
	m, e := Verify(ctx, target)
	if e != nil {
		return e
	}
	if target != m.StatePath {
		return errors.New("restore mount path differs")
	}
	if e = remote.CheckReferences(ctx, m, m.References); e != nil {
		return e
	}
	if e = AtomicJSON(filepath.Join(target, "recovery-approved.json"), map[string]any{"id": m.ID, "cutoff": m.CreatedAt, "accepted_post_backup_loss": true}); e != nil {
		return e
	}
	if os.Geteuid() == 0 {
		if e = os.Chown(filepath.Join(target, "recovery-approved.json"), m.UID, m.GID); e != nil {
			return e
		}
	}
	if e = os.Remove(filepath.Join(target, PendingName)); e != nil {
		return e
	}
	return syncDir(target)
}

func StartupCheck(root string) error {
	if _, e := os.Lstat(filepath.Join(root, PendingName)); e == nil {
		return errors.New("restored state is pending offline verification/approval")
	} else if !os.IsNotExist(e) {
		return e
	}
	return nil
}

func IdentityKey(m *Manifest) string {
	data, _ := json.Marshal([]string{m.Lakehouse, m.Bucket, m.SourceBucketID, m.SourceOrg, m.Namespace, m.Table})
	return hashBytes(data)[:32]
}
