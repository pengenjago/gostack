package s3

import "io"

// progressReader wraps an io.Reader to track upload progress
type progressReader struct {
	reader      io.Reader
	callback    ProgressCallback
	total       int64
	transferred int64
}

// Read implements io.Reader interface with progress tracking
func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.reader.Read(p)
	if n > 0 {
		pr.transferred += int64(n)
		if pr.callback != nil {
			pr.callback(pr.transferred, pr.total)
		}
	}
	return n, err
}

// progressWriterAt wraps an io.WriterAt to track download progress
type progressWriterAt struct {
	writer      io.WriterAt
	callback    ProgressCallback
	total       int64
	transferred int64
}

// WriteAt implements io.WriterAt interface with progress tracking
func (pw *progressWriterAt) WriteAt(p []byte, off int64) (int, error) {
	n, err := pw.writer.WriteAt(p, off)
	if n > 0 {
		pw.transferred += int64(n)
		if pw.callback != nil {
			pw.callback(pw.transferred, pw.total)
		}
	}
	return n, err
}