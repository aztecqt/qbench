package local

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aztecqt/dagger/util"
)

var LocalDataPath = ""

func Init(localDataPath string) {
	LocalDataPath = localDataPath
}

func OpenZipOrRawFile(path string) (io.ReadCloser, error) {
	pathz := path + ".zlib"
	if f, err := util.OpenCompressedFile_Zlib(pathz); err == nil {
		return f, nil
	} else if f, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm); err == nil {
		return f, nil
	} else {
		return nil, err
	}
}

func LoadZipOrRawFile(path string) (*bytes.Buffer, error) {
	pathz := path + ".zlib"
	if bf, n := util.LoadCompressedFile_Zlib(pathz); n > 0 {
		return bf, nil
	} else if b, err := os.ReadFile(path); err == nil {
		return bytes.NewBuffer(b), nil
	} else {
		return nil, err
	}
}

func LoadZipOrRawFileWithCache(path string, group string) (*bytes.Buffer, error) {
	if len(group) == 0 {
		return LoadZipOrRawFile(path)
	} else {
		cachePath := fmt.Sprintf("./temp/zipcache/%s/%s", group, util.ConverToFileName(path, "cache"))
		if b, err := os.ReadFile(cachePath); err == nil {
			return bytes.NewBuffer(b), nil
		} else {
			b, err := LoadZipOrRawFile(path)
			util.MakeSureDirForFile(cachePath)
			os.WriteFile(cachePath, b.Bytes(), os.ModePerm)
			return b, err
		}
	}
}

// 获取一个目录下，以instId为命名的文件夹名
func GetInstIdsOfDir(dir string) []string {
	instIds := []string{}
	if des, err := os.ReadDir(dir); err == nil {
		for _, de := range des {
			if de.IsDir() && strings.Count(de.Name(), "_") > 0 {
				instIds = append(instIds, de.Name())
			}
		}
	}
	return instIds
}

// 假设一个目录中的文件，都是以日期格式排列的
// 那么这个函数返回日期范围
func GetTimeRangeOfDir(dir string) (t0, t1 time.Time, ok bool) {
	t0 = time.Time{}
	t1 = time.Time{}
	ok = false
	if des, err := os.ReadDir(dir); err == nil {
		for i := 0; i < len(des) && t0.IsZero(); i++ {
			if !des[i].IsDir() {
				dateStr := des[i].Name()[:10]
				if t, err := time.Parse(time.DateOnly, dateStr); err == nil {
					t0 = t
				}
			}
		}

		for i := len(des) - 1; i >= 0 && t1.IsZero(); i-- {
			if !des[i].IsDir() {
				dateStr := des[i].Name()[:10]
				if t, err := time.Parse(time.DateOnly, dateStr); err == nil {
					t1 = t.AddDate(0, 0, 1).Add(-time.Millisecond)
				}
			}
		}

		ok = !t0.IsZero() && !t1.IsZero()
	}

	return
}
