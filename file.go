package goswarm

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

// withOpenFile opens the file specified by pathname and invokes the provided
// callback with the open file handle.  It returns the error from the provided
// callback, or if that error is nil, returns the result of closing the open
// file handle.  In all cases it closes the file handle prior to returning.
func withOpenFile(pathname string, callback func(fh *os.File) error) (err error) {
	fh, err := os.Open(pathname)
	if err != nil {
		return err
	}
	defer func() {
		if err2 := fh.Close(); err == nil {
			err = err2
		}
	}()
	err = callback(fh)
	return
}

// withTempFile creates a temporary file in the same directory as finalPathname,
// invokes the callback with an open file handle and pathname for the temporary
// file.  When the callback returns no errors, renames the temporary file to the
// final pathname.
func withTempFile(finalPathname string, callback func(tempFile *os.File, tempPathname string) error) error {
	fh, err := ioutil.TempFile(filepath.Dir(finalPathname), filepath.Base(finalPathname))
	if err != nil {
		return fmt.Errorf("cannot create temporary file: %s", err)
	}

	tempPathname := fh.Name()
	callbackError := callback(fh, tempPathname)
	closeError := fh.Close()

	if callbackError != nil {
		return callbackError // skip rename; ignore close error; consider removing temp file
	}

	if closeError != nil {
		return fmt.Errorf("cannot close temporary file: %s", closeError) // skip rename; consider removing temp file
	}

	if err = os.Chmod(tempPathname, 0644); err != nil {
		// Only log the error because the file is still worthwhile and can be
		// used to recover the cache database.
		log.Printf("[WARNING] cannot change file permissions for temporary file: %s", err)
	}

	if err = os.Rename(tempPathname, finalPathname); err != nil {
		return fmt.Errorf("cannot rename temporary file: %s", err)
	}

	return nil
}
