package platformops

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
)

var archiveSHA256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func validArchiveSHA256(value string) bool {
	return archiveSHA256Pattern.MatchString(value)
}

// openVerifiedRestoreArchive snapshots the exact bytes covered by verification.
// Comparing lstat with fstat rejects symlinks and inode replacement during open;
// the unlinked snapshot also prevents later path or same-inode mutation from
// changing the bytes inherited by pg_restore.
func openVerifiedRestoreArchive(path, expectedSHA256 string) (*os.File, error) {
	if !validArchiveSHA256(expectedSHA256) {
		return nil, ErrInvalidArgument
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrInvalidArgument
		}
		return nil, fmt.Errorf("inspect backup artifact path: %w", err)
	}
	if !pathInfo.Mode().IsRegular() {
		return nil, errors.New("backup artifact must be a regular file, not a symlink")
	}
	source, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open backup artifact: %w", err)
	}
	defer source.Close()
	openedInfo, err := source.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect opened backup artifact: %w", err)
	}
	if !openedInfo.Mode().IsRegular() || !os.SameFile(pathInfo, openedInfo) {
		return nil, errors.New("backup artifact changed while it was opened")
	}

	snapshot, err := os.CreateTemp(filepath.Dir(path), ".supaflow-archive-snapshot-*.dump")
	if err != nil {
		return nil, fmt.Errorf("create private backup artifact snapshot: %w", err)
	}
	verified := false
	defer func() {
		if !verified {
			_ = snapshot.Close()
		}
	}()
	if err = os.Remove(snapshot.Name()); err != nil {
		return nil, fmt.Errorf("unlink private backup artifact snapshot: %w", err)
	}
	expected, err := hex.DecodeString(expectedSHA256)
	if err != nil {
		return nil, ErrInvalidArgument
	}
	digest := sha256.New()
	if _, err = io.Copy(io.MultiWriter(digest, snapshot), source); err != nil {
		return nil, fmt.Errorf("snapshot and hash backup artifact: %w", err)
	}
	if subtle.ConstantTimeCompare(digest.Sum(nil), expected) != 1 {
		return nil, errors.New("backup artifact SHA-256 does not match the trusted digest")
	}
	if _, err = snapshot.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind verified backup artifact snapshot: %w", err)
	}
	verified = true
	return snapshot, nil
}
