package qualification

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
)

// AppendInitramfs retains Ubuntu's initramfs/modules and appends a small newc
// member containing our static PID 1. Linux unpacks concatenated initramfs
// members; rdinit selects this explicitly guarded disposable-guest entry point.
func AppendInitramfs(source, binary, target string) (result error) {
	original, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, original.Close()) }()
	st, err := original.Stat()
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() || st.Size() <= 0 || st.Size() > 128<<20 {
		return errors.New("qualification: initramfs must be a regular file below 128 MiB")
	}
	init, err := os.ReadFile(binary)
	if err != nil {
		return err
	}
	if len(init) == 0 || len(init) > 32<<20 {
		return errors.New("qualification: static init must be below 32 MiB")
	}
	out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	copyN, copyErr := io.Copy(out, io.LimitReader(original, st.Size()+1))
	if copyErr == nil && copyN != st.Size() {
		copyErr = errors.New("qualification: initramfs length changed")
	}
	if copyErr != nil {
		return errors.Join(copyErr, out.Close())
	}
	gz := gzip.NewWriter(out)
	err = newc(gz, "tarp-init", init, 0100755)
	if err == nil {
		err = newc(gz, "TRAILER!!!", nil, 0)
	}
	err = errors.Join(err, gz.Close())
	if err == nil {
		err = out.Sync()
	}
	return errors.Join(err, out.Close())
}

func newc(w io.Writer, name string, data []byte, mode uint32) error {
	header := fmt.Sprintf("070701%08x%08x%08x%08x%08x%08x%08x%08x%08x%08x%08x%08x%08x",
		1, mode, 0, 0, 1, 0, len(data), 0, 0, 0, 0, len(name)+1, 0)
	for _, part := range [][]byte{
		[]byte(header), append([]byte(name), 0), make([]byte, (4-(110+len(name)+1)%4)%4),
		data, make([]byte, (4-len(data)%4)%4),
	} {
		if err := writeFull(w, part); err != nil {
			return err
		}
	}
	return nil
}
