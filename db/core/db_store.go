package core

import (
	"fmt"
	"os"
	"path/filepath"
)

type User struct {
	id       int
	name     string
	email    string
	password string
}

func SaveData(path string, data []byte) error {
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer fp.Close()

	_, err = fp.Write(data)

	if err != nil {
		return err
	}

	return fp.Sync()

}

func SaveData2(path string, data []byte) (string, error) {
	tmp := fmt.Sprintf("%s.tmp.%d", path, randomInt())

	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)
	if err != nil {
		return tmp, err
	}
	defer func() {
		if cerr := fp.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	if _, err = fp.Write(data); err != nil {
		return tmp, err
	}

	if err = fp.Sync(); err != nil {
		return tmp, err
	}

	if err = os.Rename(tmp, path); err != nil {
		return tmp, err
	}

	dirFd, err := os.Open(filepath.Dir(path))
	if err != nil {
		return tmp, err
	}
	defer func() {
		if cerr := dirFd.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	if err = dirFd.Sync(); err != nil {
		return tmp, err
	}

	return tmp, nil
}
