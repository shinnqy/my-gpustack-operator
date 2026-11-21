// Copyright 2018 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package osx

import (
	"fmt"
	"os"
	"path/filepath"
)

type MmapFile struct {
	f *os.File
	b []byte
}

func OpenMmapFile(path string) (*MmapFile, error) {
	return OpenMmapFileWithSize(path, 0)
}

func OpenMmapFileWithSize(path string, size int) (*MmapFile, error) {
	p := filepath.Clean(path)
	p = InlineTilde(p)

	f, err := os.Open(p)
	if err != nil {
		return nil, fmt.Errorf("try lock file: %w", err)
	}
	if size <= 0 {
		info, err := f.Stat()
		if err != nil {
			Close(f)
			return nil, fmt.Errorf("stat: %w", err)
		}
		size = int(info.Size())
	}

	b, err := mmap(f, size)
	if err != nil {
		Close(f)
		return nil, fmt.Errorf("mmap, size %d: %w", size, err)
	}

	return &MmapFile{f: f, b: b}, nil
}

func (f *MmapFile) Bytes() []byte {
	return f.b
}

func (f *MmapFile) Len() int64 {
	return int64(len(f.b))
}

func (f *MmapFile) Close() error {
	err0 := munmap(f.b)
	err1 := f.f.Close()

	if err0 != nil {
		return err0
	}
	return err1
}
