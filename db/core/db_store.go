package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand"
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

func randomInt() int {
	return rand.Int()
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

// ~ so how basicaklly the log writer work out is it basiaclly take a fd and also the data and then also perform  some check sums

func LogWriter(fs *os.File, data []byte) error {
	size := uint32(len(data))

	buf := new(bytes.Buffer)

	// ~ we also have to perform the crc check for the validation
	check := crc32.ChecksumIEEE(data)

	binary.Write(buf, binary.LittleEndian, size)
	binary.Write(buf, binary.LittleEndian, check)

	buf.Write(data)

	_, err := fs.Write(buf.Bytes())

	return err
}

// ~ bloom filter implementation

var arr = make([]int, 0, 10)
