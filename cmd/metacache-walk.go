// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"

	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
)

// WalkDirOptions provides options for WalkDir operations.
type WalkDirOptions struct {
	// Bucket to scanner
	Bucket string

	// Directory inside the bucket.
	BaseDir string

	// Do a full recursive scan.
	Recursive bool

	// ReportNotFound will return errFileNotFound if all disks reports the BaseDir cannot be found.
	ReportNotFound bool

	// FilterPrefix will only return results with given prefix within folder.
	// Should never contain a slash.
	FilterPrefix string

	// ForwardTo will forward to the given object path.
	ForwardTo string
}

// WalkDir will traverse a directory and return all entries found.
// On success a sorted meta cache stream will be returned.
// Metadata has data stripped, if any.
func (s *xlStorage) WalkDir(ctx context.Context, opts WalkDirOptions, wr io.Writer) (err error) {
	// Verify if volume is valid and it exists.
	// 拼接diskPath和桶名，获取桶的完整路径
	volumeDir, err := s.getVolDir(opts.Bucket)
	if err != nil {
		return err
	}

	// Stat a volume entry.
	// 通过系统调用判断桶路径能否访问
	if err = Access(volumeDir); err != nil {
		if osIsNotExist(err) {
			return errVolumeNotFound
		} else if isSysErrIO(err) {
			return errFaultyDisk
		}
		return err
	}

	// Use a small block size to start sending quickly
	// 新建metacacheWriter对象，块大小为16KiB
	// metacacheWriter是个包装类，里面封装了传入的Writer
	// 同时还定义了metacacheWriter的creator方法属性
	w := newMetacacheWriter(wr, 16<<10)
	w.reuseBlocks = true // We are not sharing results, so reuse buffers.
	defer w.Close()
	out, err := w.stream()
	if err != nil {
		return err
	}
	defer close(out)

	// Fast exit track to check if we are listing an object with
	// a trailing slash, this will avoid to list the object content.
	if HasSuffix(opts.BaseDir, SlashSeparator) {
		metadata, err := s.readMetadata(ctx, pathJoin(volumeDir,
			opts.BaseDir[:len(opts.BaseDir)-1]+globalDirSuffix,
			xlStorageFormatFile))
		if err == nil {
			// if baseDir is already a directory object, consider it
			// as part of the list call, this is a AWS S3 specific
			// behavior.
			out <- metaCacheEntry{
				name:     opts.BaseDir,
				metadata: metadata,
			}
		} else {
			st, sterr := Lstat(pathJoin(volumeDir, opts.BaseDir, xlStorageFormatFile))
			if sterr == nil && st.Mode().IsRegular() {
				return errFileNotFound
			}
		}
	}

	prefix := opts.FilterPrefix
	var scanDir func(path string) error

	fmt.Printf("opts.FilterPrefix -> prefix: %s\n", prefix)

	// 关于current的取值，用下面的例子进行说明，例：
	// 桶		bkt
	// path1	 ┖ aaa
	// path2        ┖ bbb
	// path3           ┖ ccc
	// 下面是进入各个path的current值
	// 桶:		空字符串
	// path1:	aaa/
	// path2:	aaa/bbb/
	// path3:	aaa/bbb/ccc/
	scanDir = func(current string) error {
		// Skip forward, if requested...
		forward := ""
		if len(opts.ForwardTo) > 0 && strings.HasPrefix(opts.ForwardTo, current) {
			forward = strings.TrimPrefix(opts.ForwardTo, current)
			// Trim further directories and trailing slash.
			if idx := strings.IndexByte(forward, '/'); idx > 0 {
				forward = forward[:idx]
			}
		}
		if contextCanceled(ctx) {
			return ctx.Err()
		}

		s.walkMu.Lock()
		// 返回指定路径下的所有文件夹名称，并且文件夹名后面加上左斜杠，无递归，返回结果集按创建时间升序排序
		// 例： entries: [go1.17.7.linux-amd64.tar.gz/ uploadTestFile2M/ renameData.txt/ mc/ aaaaaa/ uploadTestFile15b/]
		entries, err := s.ListDir(ctx, opts.Bucket, current, -1)
		s.walkMu.Unlock()
		if err != nil {
			// Folder could have gone away in-between
			if err != errVolumeNotFound && err != errFileNotFound {
				logger.LogIf(ctx, err)
			}
			if opts.ReportNotFound && err == errFileNotFound && current == opts.BaseDir {
				return errFileNotFound
			}
			// Forward some errors?
			return nil
		}
		if len(entries) == 0 {
			return nil
		}
		dirObjects := make(map[string]struct{})
		// 默认情况下，该循环将entries中每个字符串最后的左斜杠去掉，只保留对象名称或次级目录名称
		for i, entry := range entries {
			if len(prefix) > 0 && !strings.HasPrefix(entry, prefix) {
				// Do do not retain the file, since it doesn't
				// match the prefix.
				entries[i] = ""
				continue
			}
			if len(forward) > 0 && entry < forward {
				// Do do not retain the file, since its
				// lexially smaller than 'forward'
				entries[i] = ""
				continue
			}
			if strings.HasSuffix(entry, slashSeparator) {
				if strings.HasSuffix(entry, globalDirSuffixWithSlash) {
					// Add without extension so it is sorted correctly.
					entry = strings.TrimSuffix(entry, globalDirSuffixWithSlash) + slashSeparator
					dirObjects[entry] = struct{}{}
					entries[i] = entry
					continue
				}
				// Trim slash, since we don't know if this is folder or object.
				// 删除字符串的最后一个左斜杠
				entries[i] = entries[i][:len(entry)-1]
				continue
			}
			// Do do not retain the file.
			entries[i] = ""

			if contextCanceled(ctx) {
				return ctx.Err()
			}
			// If root was an object return it as such.
			if HasSuffix(entry, xlStorageFormatFile) {
				var meta metaCacheEntry
				s.walkReadMu.Lock()
				meta.metadata, err = s.readMetadata(ctx, pathJoin(volumeDir, current, entry))
				s.walkReadMu.Unlock()
				if err != nil {
					logger.LogIf(ctx, err)
					continue
				}
				meta.name = strings.TrimSuffix(entry, xlStorageFormatFile)
				meta.name = strings.TrimSuffix(meta.name, SlashSeparator)
				meta.name = pathJoin(current, meta.name)
				meta.name = decodeDirObject(meta.name)
				out <- meta
				return nil
			}
			// Check legacy.
			if HasSuffix(entry, xlStorageFormatFileV1) {
				var meta metaCacheEntry
				s.walkReadMu.Lock()
				meta.metadata, err = xioutil.ReadFile(pathJoin(volumeDir, current, entry))
				s.walkReadMu.Unlock()
				if err != nil {
					logger.LogIf(ctx, err)
					continue
				}
				meta.name = strings.TrimSuffix(entry, xlStorageFormatFileV1)
				meta.name = strings.TrimSuffix(meta.name, SlashSeparator)
				meta.name = pathJoin(current, meta.name)
				out <- meta
				return nil
			}
			// Skip all other files.
		}

		// Process in sort order.
		sort.Strings(entries)
		dirStack := make([]string, 0, 5)
		prefix = "" // Remove prefix after first level as we have already filtered the list.
		if len(forward) > 0 {
			// Conservative forwarding. Entries may be either objects or prefixes.
			for i, entry := range entries {
				if entry >= forward || strings.HasPrefix(forward, entry) {
					entries = entries[i:]
					break
				}
			}
		}

		for _, entry := range entries {
			if entry == "" {
				continue
			}
			if contextCanceled(ctx) {
				return ctx.Err()
			}
			meta := metaCacheEntry{name: PathJoin(current, entry)}

			// If directory entry on stack before this, pop it now.
			for len(dirStack) > 0 && dirStack[len(dirStack)-1] < meta.name {
				pop := dirStack[len(dirStack)-1]
				out <- metaCacheEntry{name: pop}
				if opts.Recursive {
					// Scan folder we found. Should be in correct sort order where we are.
					forward = ""
					if len(opts.ForwardTo) > 0 && strings.HasPrefix(opts.ForwardTo, pop) {
						forward = strings.TrimPrefix(opts.ForwardTo, pop)
					}
					logger.LogIf(ctx, scanDir(pop))
				}
				dirStack = dirStack[:len(dirStack)-1]
			}

			// All objects will be returned as directories, there has been no object check yet.
			// Check it by attempting to read metadata.
			_, isDirObj := dirObjects[entry]
			if isDirObj {
				meta.name = meta.name[:len(meta.name)-1] + globalDirSuffixWithSlash
			}

			s.walkReadMu.Lock()
			// 读取对象的xl.meta文件，将文件内容写入metaCacheEntry结构体的metadata属性
			meta.metadata, err = s.readMetadata(ctx, pathJoin(volumeDir, meta.name, xlStorageFormatFile))
			s.walkReadMu.Unlock()
			switch {
			case err == nil:
				// It was an object
				if isDirObj {
					meta.name = strings.TrimSuffix(meta.name, globalDirSuffixWithSlash) + slashSeparator
				}
				// 将数据推入stream方法返回的协程中，由stream中启动的协程进行处理
				out <- meta
			case osIsNotExist(err), isSysErrIsDir(err):
				// 无法读取的情况，尝试读取老版本的元数据文件xl.json，读取成功的场合将数据推入stream方法返回的协程中
				meta.metadata, err = xioutil.ReadFile(pathJoin(volumeDir, meta.name, xlStorageFormatFileV1))
				if err == nil {
					// It was an object
					out <- meta
					continue
				}

				// NOT an object, append to stack (with slash)
				// If dirObject, but no metadata (which is unexpected) we skip it.
				if !isDirObj {
					// pathJoin(volumeDir, meta.name+slashSeparator 返回对象文件夹的完整路径
					// 判断指定文件夹下面是否只有文件夹，如果全是文件夹表明该文件夹是次级目录
					// 如果指定文件夹下面是否只有文件夹，将该文件夹写入dirStack变量
					if !isDirEmpty(pathJoin(volumeDir, meta.name+slashSeparator)) {
						dirStack = append(dirStack, meta.name+slashSeparator)
					}
				}
			case isSysErrNotDir(err):
				// skip
			default:
				logger.LogIf(ctx, err)
			}
		}

		// If directory entry left on stack, pop it now.
		// 遍历dirStack
		for len(dirStack) > 0 {
			pop := dirStack[len(dirStack)-1]
			// 将次级目录的数据推入stream方法返回的协程中
			out <- metaCacheEntry{name: pop}
			// 如果查询选项中设置有递归，则递归调用scanDir
			if opts.Recursive {
				// Scan folder we found. Should be in correct sort order where we are.
				logger.LogIf(ctx, scanDir(pop))
			}
			dirStack = dirStack[:len(dirStack)-1]
		}
		return nil
	}

	// Stream output.
	return scanDir(opts.BaseDir)
}

func (p *xlStorageDiskIDCheck) WalkDir(ctx context.Context, opts WalkDirOptions, wr io.Writer) error {
	defer p.updateStorageMetrics(storageMetricWalkDir, opts.Bucket, opts.BaseDir)()

	if contextCanceled(ctx) {
		return ctx.Err()
	}

	if err := p.checkDiskStale(); err != nil {
		return err
	}

	return p.storage.WalkDir(ctx, opts, wr)
}

// WalkDir will traverse a directory and return all entries found.
// On success a meta cache stream will be returned, that should be closed when done.
func (client *storageRESTClient) WalkDir(ctx context.Context, opts WalkDirOptions, wr io.Writer) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, opts.Bucket)
	values.Set(storageRESTDirPath, opts.BaseDir)
	values.Set(storageRESTRecursive, strconv.FormatBool(opts.Recursive))
	values.Set(storageRESTReportNotFound, strconv.FormatBool(opts.ReportNotFound))
	values.Set(storageRESTPrefixFilter, opts.FilterPrefix)
	values.Set(storageRESTForwardFilter, opts.ForwardTo)
	respBody, err := client.call(ctx, storageRESTMethodWalkDir, values, nil, -1)
	if err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	defer xhttp.DrainBody(respBody)
	return waitForHTTPStream(respBody, wr)
}

// WalkDirHandler - remote caller to list files and folders in a requested directory path.
func (s *storageRESTServer) WalkDirHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	volume := r.Form.Get(storageRESTVolume)
	dirPath := r.Form.Get(storageRESTDirPath)
	recursive, err := strconv.ParseBool(r.Form.Get(storageRESTRecursive))
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	var reportNotFound bool
	if v := r.Form.Get(storageRESTReportNotFound); v != "" {
		reportNotFound, err = strconv.ParseBool(v)
		if err != nil {
			s.writeErrorResponse(w, err)
			return
		}
	}

	prefix := r.Form.Get(storageRESTPrefixFilter)
	forward := r.Form.Get(storageRESTForwardFilter)
	writer := streamHTTPResponse(w)
	defer func() {
		if r := recover(); r != nil {
			debug.PrintStack()
			writer.CloseWithError(fmt.Errorf("panic: %v", r))
		}
	}()
	writer.CloseWithError(s.storage.WalkDir(r.Context(), WalkDirOptions{
		Bucket:         volume,
		BaseDir:        dirPath,
		Recursive:      recursive,
		ReportNotFound: reportNotFound,
		FilterPrefix:   prefix,
		ForwardTo:      forward,
	}, writer))
}
