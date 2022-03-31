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
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/hash"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/console"
)

type listPathOptions struct {
	// ID of the listing.
	// This will be used to persist the list.
	ID string

	// Bucket of the listing.
	Bucket string

	// Directory inside the bucket.
	BaseDir string

	// Scan/return only content with prefix.
	Prefix string

	// FilterPrefix will return only results with this prefix when scanning.
	// Should never contain a slash.
	// Prefix should still be set.
	FilterPrefix string

	// Marker to resume listing.
	// The response will be the first entry >= this object name.
	Marker string

	// Limit the number of results.
	Limit int

	// The number of disks to ask. Special values:
	// 0 uses default number of disks.
	// -1 use at least 50% of disks or at least the default number.
	AskDisks int

	// InclDeleted will keep all entries where latest version is a delete marker.
	InclDeleted bool

	// Scan recursively.
	// If false only main directory will be scanned.
	// Should always be true if Separator is n SlashSeparator.
	Recursive bool

	// Separator to use.
	Separator string

	// Create indicates that the lister should not attempt to load an existing cache.
	Create bool

	// Include pure directories.
	IncludeDirectories bool

	// Transient is set if the cache is transient due to an error or being a reserved bucket.
	// This means the cache metadata will not be persisted on disk.
	// A transient result will never be returned from the cache so knowing the list id is required.
	Transient bool

	// Versioned is this a ListObjectVersions call.
	Versioned bool

	// pool and set of where the cache is located.
	pool, set int
}

func init() {
	gob.Register(listPathOptions{})
}

// newMetacache constructs a new metacache from the options.
func (o listPathOptions) newMetacache() metacache {
	return metacache{
		id:          o.ID,
		bucket:      o.Bucket,
		root:        o.BaseDir,
		recursive:   o.Recursive,
		status:      scanStateStarted,
		error:       "",
		started:     UTCNow(),
		lastHandout: UTCNow(),
		lastUpdate:  UTCNow(),
		ended:       time.Time{},
		dataVersion: metacacheStreamVersion,
		filter:      o.FilterPrefix,
	}
}

func (o *listPathOptions) debugf(format string, data ...interface{}) {
	if serverDebugLog {
		console.Debugf(format+"\n", data...)
	}
}

func (o *listPathOptions) debugln(data ...interface{}) {
	if serverDebugLog {
		console.Debugln(data...)
	}
}

// gatherResults will collect all results on the input channel and filter results according to the options.
// Caller should close the channel when done.
// The returned function will return the results once there is enough or input is closed,
// or the context is canceled.
func (o *listPathOptions) gatherResults(ctx context.Context, in <-chan metaCacheEntry) func() (metaCacheEntriesSorted, error) {
	resultsDone := make(chan metaCacheEntriesSorted)
	// Copy so we can mutate
	resCh := resultsDone
	var done bool
	var mu sync.Mutex
	resErr := io.EOF

	go func() {
		var results metaCacheEntriesSorted
		var returned bool
		for entry := range in {
			if returned {
				// past limit
				continue
			}
			mu.Lock()
			returned = done
			mu.Unlock()
			if returned {
				resCh = nil
				continue
			}
			if !o.IncludeDirectories && (entry.isDir() || (!o.Versioned && entry.isObjectDir() && entry.isLatestDeletemarker())) {
				continue
			}
			if o.Marker != "" && entry.name < o.Marker {
				continue
			}
			if !strings.HasPrefix(entry.name, o.Prefix) {
				continue
			}
			if !o.Recursive && !entry.isInDir(o.Prefix, o.Separator) {
				continue
			}
			if !o.InclDeleted && entry.isObject() && entry.isLatestDeletemarker() && !entry.isObjectDir() {
				continue
			}
			if o.Limit > 0 && results.len() >= o.Limit {
				// We have enough and we have more.
				// Do not return io.EOF
				if resCh != nil {
					resErr = nil
					resCh <- results
					resCh = nil
					returned = true
				}
				continue
			}
			results.o = append(results.o, entry)
		}
		if resCh != nil {
			resErr = io.EOF
			resCh <- results
		}
	}()
	return func() (metaCacheEntriesSorted, error) {
		select {
		case <-ctx.Done():
			mu.Lock()
			done = true
			mu.Unlock()
			return metaCacheEntriesSorted{}, ctx.Err()
		case r := <-resultsDone:
			return r, resErr
		}
	}
}

// findFirstPart will find the part with 0 being the first that corresponds to the marker in the options.
// io.ErrUnexpectedEOF is returned if the place containing the marker hasn't been scanned yet.
// io.EOF indicates the marker is beyond the end of the stream and does not exist.
func (o *listPathOptions) findFirstPart(fi FileInfo) (int, error) {
	search := o.Marker
	if search == "" {
		search = o.Prefix
	}
	if search == "" {
		return 0, nil
	}
	o.debugln("searching for ", search)
	var tmp metacacheBlock
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	i := 0
	for {
		partKey := fmt.Sprintf("%s-metacache-part-%d", ReservedMetadataPrefixLower, i)
		v, ok := fi.Metadata[partKey]
		if !ok {
			o.debugln("no match in metadata, waiting")
			return -1, io.ErrUnexpectedEOF
		}
		err := json.Unmarshal([]byte(v), &tmp)
		if !ok {
			logger.LogIf(context.Background(), err)
			return -1, err
		}
		if tmp.First == "" && tmp.Last == "" && tmp.EOS {
			return 0, errFileNotFound
		}
		if tmp.First >= search {
			o.debugln("First >= search", v)
			return i, nil
		}
		if tmp.Last >= search {
			o.debugln("Last >= search", v)
			return i, nil
		}
		if tmp.EOS {
			o.debugln("no match, at EOS", v)
			return -3, io.EOF
		}
		o.debugln("First ", tmp.First, "<", search, " search", i)
		i++
	}
}

// updateMetacacheListing will update the metacache listing.
func (o *listPathOptions) updateMetacacheListing(m metacache, rpc *peerRESTClient) (metacache, error) {
	if rpc == nil {
		return localMetacacheMgr.updateCacheEntry(m)
	}
	return rpc.UpdateMetacacheListing(context.Background(), m)
}

func getMetacacheBlockInfo(fi FileInfo, block int) (*metacacheBlock, error) {
	var tmp metacacheBlock
	partKey := fmt.Sprintf("%s-metacache-part-%d", ReservedMetadataPrefixLower, block)
	v, ok := fi.Metadata[partKey]
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	return &tmp, json.Unmarshal([]byte(v), &tmp)
}

const metacachePrefix = ".metacache"

func metacachePrefixForID(bucket, id string) string {
	return pathJoin(bucketMetaPrefix, bucket, metacachePrefix, id)
}

// objectPath returns the object path of the cache.
func (o *listPathOptions) objectPath(block int) string {
	return pathJoin(metacachePrefixForID(o.Bucket, o.ID), "block-"+strconv.Itoa(block)+".s2")
}

func (o *listPathOptions) SetFilter() {
	switch {
	case metacacheSharePrefix:
		return
	case o.Prefix == o.BaseDir:
		// No additional prefix
		return
	}
	// Remove basedir.
	o.FilterPrefix = strings.TrimPrefix(o.Prefix, o.BaseDir)
	// Remove leading and trailing slashes.
	o.FilterPrefix = strings.Trim(o.FilterPrefix, slashSeparator)

	if strings.Contains(o.FilterPrefix, slashSeparator) {
		// Sanity check, should not happen.
		o.FilterPrefix = ""
	}
}

// filter will apply the options and return the number of objects requested by the limit.
// Will return io.EOF if there are no more entries with the same filter.
// The last entry can be used as a marker to resume the listing.
func (r *metacacheReader) filter(o listPathOptions) (entries metaCacheEntriesSorted, err error) {
	// Forward to prefix, if any
	err = r.forwardTo(o.Prefix)
	if err != nil {
		return entries, err
	}
	if o.Marker != "" {
		err = r.forwardTo(o.Marker)
		if err != nil {
			return entries, err
		}
	}
	o.debugln("forwarded to ", o.Prefix, "marker:", o.Marker, "sep:", o.Separator)

	// Filter
	if !o.Recursive {
		entries.o = make(metaCacheEntries, 0, o.Limit)
		pastPrefix := false
		err := r.readFn(func(entry metaCacheEntry) bool {
			if o.Prefix != "" && !strings.HasPrefix(entry.name, o.Prefix) {
				// We are past the prefix, don't continue.
				pastPrefix = true
				return false
			}
			if !o.IncludeDirectories && (entry.isDir() || (!o.Versioned && entry.isObjectDir() && entry.isLatestDeletemarker())) {
				return true
			}
			if !entry.isInDir(o.Prefix, o.Separator) {
				return true
			}
			if !o.InclDeleted && entry.isObject() && entry.isLatestDeletemarker() && !entry.isObjectDir() {
				return entries.len() < o.Limit
			}
			entries.o = append(entries.o, entry)
			return entries.len() < o.Limit
		})
		if (err != nil && err.Error() == io.EOF.Error()) || pastPrefix || r.nextEOF() {
			return entries, io.EOF
		}
		return entries, err
	}

	// We should not need to filter more.
	return r.readN(o.Limit, o.InclDeleted, o.IncludeDirectories, o.Versioned, o.Prefix)
}

func (er *erasureObjects) streamMetadataParts(ctx context.Context, o listPathOptions) (entries metaCacheEntriesSorted, err error) {
	retries := 0
	rpc := globalNotificationSys.restClientFromHash(o.Bucket)

	for {
		if contextCanceled(ctx) {
			return entries, ctx.Err()
		}

		// If many failures, check the cache state.
		if retries > 10 {
			err := o.checkMetacacheState(ctx, rpc)
			if err != nil {
				return entries, fmt.Errorf("remote listing canceled: %w", err)
			}
			retries = 1
		}

		const retryDelay = 250 * time.Millisecond
		// All operations are performed without locks, so we must be careful and allow for failures.
		// Read metadata associated with the object from a disk.
		if retries > 0 {
			for _, disk := range er.getDisks() {
				if disk == nil {
					continue
				}
				if !disk.IsOnline() {
					continue
				}
				_, err := disk.ReadVersion(ctx, minioMetaBucket,
					o.objectPath(0), "", false)
				if err != nil {
					time.Sleep(retryDelay)
					retries++
					continue
				}
				break
			}
		}

		// Load first part metadata...
		// Read metadata associated with the object from all disks.
		fi, metaArr, onlineDisks, err := er.getObjectFileInfo(ctx, minioMetaBucket, o.objectPath(0), ObjectOptions{}, true)
		if err != nil {
			switch toObjectErr(err, minioMetaBucket, o.objectPath(0)).(type) {
			case ObjectNotFound:
				retries++
				time.Sleep(retryDelay)
				continue
			case InsufficientReadQuorum:
				retries++
				time.Sleep(retryDelay)
				continue
			default:
				return entries, fmt.Errorf("reading first part metadata: %w", err)
			}
		}

		partN, err := o.findFirstPart(fi)
		switch {
		case err == nil:
		case errors.Is(err, io.ErrUnexpectedEOF):
			if retries == 10 {
				err := o.checkMetacacheState(ctx, rpc)
				if err != nil {
					return entries, fmt.Errorf("remote listing canceled: %w", err)
				}
				retries = -1
			}
			retries++
			time.Sleep(retryDelay)
			continue
		case errors.Is(err, io.EOF):
			return entries, io.EOF
		}

		// We got a stream to start at.
		loadedPart := 0
		for {
			if contextCanceled(ctx) {
				return entries, ctx.Err()
			}

			if partN != loadedPart {
				if retries > 10 {
					err := o.checkMetacacheState(ctx, rpc)
					if err != nil {
						return entries, fmt.Errorf("waiting for next part %d: %w", partN, err)
					}
					retries = 1
				}

				if retries > 0 {
					// Load from one disk only
					for _, disk := range er.getDisks() {
						if disk == nil {
							continue
						}
						if !disk.IsOnline() {
							continue
						}
						_, err := disk.ReadVersion(ctx, minioMetaBucket,
							o.objectPath(partN), "", false)
						if err != nil {
							time.Sleep(retryDelay)
							retries++
							continue
						}
						break
					}
				}

				// Load partN metadata...
				fi, metaArr, onlineDisks, err = er.getObjectFileInfo(ctx, minioMetaBucket, o.objectPath(partN), ObjectOptions{}, true)
				if err != nil {
					time.Sleep(retryDelay)
					retries++
					continue
				}
				loadedPart = partN
				bi, err := getMetacacheBlockInfo(fi, partN)
				logger.LogIf(ctx, err)
				if err == nil {
					if bi.pastPrefix(o.Prefix) {
						return entries, io.EOF
					}
				}
			}

			pr, pw := io.Pipe()
			go func() {
				werr := er.getObjectWithFileInfo(ctx, minioMetaBucket, o.objectPath(partN), 0,
					fi.Size, pw, fi, metaArr, onlineDisks)
				pw.CloseWithError(werr)
			}()

			tmp := newMetacacheReader(pr)
			e, err := tmp.filter(o)
			pr.CloseWithError(err)
			entries.o = append(entries.o, e.o...)
			if o.Limit > 0 && entries.len() > o.Limit {
				entries.truncate(o.Limit)
				return entries, nil
			}
			if err == nil {
				// We stopped within the listing, we are done for now...
				return entries, nil
			}
			if err != nil && err.Error() != io.EOF.Error() {
				switch toObjectErr(err, minioMetaBucket, o.objectPath(partN)).(type) {
				case ObjectNotFound:
					retries++
					time.Sleep(retryDelay)
					continue
				case InsufficientReadQuorum:
					retries++
					time.Sleep(retryDelay)
					continue
				default:
					logger.LogIf(ctx, err)
					return entries, err
				}
			}

			// We finished at the end of the block.
			// And should not expect any more results.
			bi, err := getMetacacheBlockInfo(fi, partN)
			logger.LogIf(ctx, err)
			if err != nil || bi.EOS {
				// We are done and there are no more parts.
				return entries, io.EOF
			}
			if bi.endedPrefix(o.Prefix) {
				// Nothing more for prefix.
				return entries, io.EOF
			}
			partN++
			retries = 0
		}
	}
}

// Will return io.EOF if continuing would not yield more results.
func (er *erasureObjects) listPath(ctx context.Context, o listPathOptions, results chan<- metaCacheEntry) (err error) {
	defer close(results)
	o.debugf(color.Green("listPath:")+" with options: %#v", o)

	// 默认值为-1
	askDisks := o.AskDisks
	// 默认值的场合为-2
	listingQuorum := o.AskDisks - 1
	disks := er.getDisks()
	var fallbackDisks []StorageAPI

	// Special case: ask all disks if the drive count is 4
	// 默认值情况下，将askDisks设置为当前纠删集中所有在线磁盘数量，listingQuorum为为当前纠删集中二分之一在线盘数量
	// 例，为当前纠删集中所有在线盘数量为10，askDisks为10，listingQuorum为5
	if askDisks <= 0 || er.setDriveCount == 4 {
		askDisks = len(disks)          // with 'strict' quorum list on all online disks.
		listingQuorum = len(disks) / 2 // keep this such that we can list all objects with different quorum ratio.
	}
	if askDisks > 0 && len(disks) > askDisks {
		rand.Shuffle(len(disks), func(i, j int) {
			disks[i], disks[j] = disks[j], disks[i]
		})
		fallbackDisks = disks[askDisks:]
		disks = disks[:askDisks]
	}

	// How to resolve results.
	resolver := metadataResolutionParams{
		dirQuorum: listingQuorum,
		objQuorum: listingQuorum,
		bucket:    o.Bucket,
	}

	ctxDone := ctx.Done()
	return listPathRaw(ctx, listPathRawOptions{
		disks:         disks,
		fallbackDisks: fallbackDisks,
		bucket:        o.Bucket,
		path:          o.BaseDir,
		recursive:     o.Recursive,
		filterPrefix:  o.FilterPrefix,
		minDisks:      listingQuorum,
		forwardTo:     o.Marker,
		agreed: func(entry metaCacheEntry) {
			select {
			case <-ctxDone:
			case results <- entry:
			}
		},
		partial: func(entries metaCacheEntries, nAgreed int, errs []error) {
			// Results Disagree :-(
			entry, ok := entries.resolve(&resolver)
			if ok {
				select {
				case <-ctxDone:
				case results <- *entry:
				}
			}
		},
	})
}

type metaCacheRPC struct {
	o      listPathOptions
	mu     sync.Mutex
	meta   *metacache
	rpc    *peerRESTClient
	cancel context.CancelFunc
}

func (m *metaCacheRPC) setErr(err string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	meta := *m.meta
	if meta.status != scanStateError {
		meta.error = err
		meta.status = scanStateError
	} else {
		// An error is already set.
		return
	}
	meta, _ = m.o.updateMetacacheListing(meta, m.rpc)
	*m.meta = meta
}

func (er *erasureObjects) saveMetaCacheStream(ctx context.Context, mc *metaCacheRPC, entries <-chan metaCacheEntry) (err error) {
	o := mc.o
	o.debugf(color.Green("saveMetaCacheStream:")+" with options: %#v", o)

	metaMu := &mc.mu
	rpc := mc.rpc
	cancel := mc.cancel
	defer func() {
		o.debugln(color.Green("saveMetaCacheStream:")+"err:", err)
		if err != nil && !errors.Is(err, io.EOF) {
			go mc.setErr(err.Error())
			cancel()
		}
	}()

	defer cancel()
	// Save continuous updates
	go func() {
		var err error
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		var exit bool
		for !exit {
			select {
			case <-ticker.C:
			case <-ctx.Done():
				exit = true
			}
			metaMu.Lock()
			meta := *mc.meta
			meta, err = o.updateMetacacheListing(meta, rpc)
			if err == nil && time.Since(meta.lastHandout) > metacacheMaxClientWait {
				cancel()
				exit = true
				meta.status = scanStateError
				meta.error = fmt.Sprintf("listing canceled since time since last handout was %v ago", time.Since(meta.lastHandout).Round(time.Second))
				o.debugln(color.Green("saveMetaCacheStream: ") + meta.error)
				meta, err = o.updateMetacacheListing(meta, rpc)
			}
			if err == nil {
				*mc.meta = meta
				if meta.status == scanStateError {
					cancel()
					exit = true
				}
			}
			metaMu.Unlock()
		}
	}()

	const retryDelay = 200 * time.Millisecond
	const maxTries = 5

	// Keep destination...
	// Write results to disk.
	bw := newMetacacheBlockWriter(entries, func(b *metacacheBlock) error {
		// if the block is 0 bytes and its a first block skip it.
		// skip only this for Transient caches.
		if len(b.data) == 0 && b.n == 0 && o.Transient {
			return nil
		}
		o.debugln(color.Green("saveMetaCacheStream:")+" saving block", b.n, "to", o.objectPath(b.n))
		r, err := hash.NewReader(bytes.NewReader(b.data), int64(len(b.data)), "", "", int64(len(b.data)))
		logger.LogIf(ctx, err)
		custom := b.headerKV()
		_, err = er.putMetacacheObject(ctx, o.objectPath(b.n), NewPutObjReader(r), ObjectOptions{
			UserDefined: custom,
		})
		if err != nil {
			mc.setErr(err.Error())
			cancel()
			return err
		}
		if b.n == 0 {
			return nil
		}
		// Update block 0 metadata.
		var retries int
		for {
			meta := b.headerKV()
			fi := FileInfo{
				Metadata: make(map[string]string, len(meta)),
			}
			for k, v := range meta {
				fi.Metadata[k] = v
			}
			err := er.updateObjectMeta(ctx, minioMetaBucket, o.objectPath(0), fi, er.getDisks())
			if err == nil {
				break
			}
			switch err.(type) {
			case ObjectNotFound:
				return err
			case StorageErr:
				return err
			case InsufficientReadQuorum:
			default:
				logger.LogIf(ctx, err)
			}
			if retries >= maxTries {
				return err
			}
			retries++
			time.Sleep(retryDelay)
		}
		return nil
	})

	// Blocks while consuming entries or an error occurs.
	err = bw.Close()
	if err != nil {
		mc.setErr(err.Error())
	}
	metaMu.Lock()
	defer metaMu.Unlock()
	if mc.meta.error != "" {
		return err
	}
	// Save success
	mc.meta.status = scanStateSuccess
	meta, err := o.updateMetacacheListing(*mc.meta, rpc)
	if err == nil {
		*mc.meta = meta
	}
	return nil
}

type listPathRawOptions struct {
	disks         []StorageAPI
	fallbackDisks []StorageAPI
	bucket, path  string
	recursive     bool

	// Only return results with this prefix.
	filterPrefix string

	// Forward to this prefix before returning results.
	forwardTo string

	// Minimum number of good disks to continue.
	// An error will be returned if this many disks returned an error.
	minDisks       int
	reportNotFound bool

	// Callbacks with results:
	// If set to nil, it will not be called.

	// agreed is called if all disks agreed.
	agreed func(entry metaCacheEntry)

	// partial will be called when there is disagreement between disks.
	// if disk did not return any result, but also haven't errored
	// the entry will be empty and errs will
	partial func(entries metaCacheEntries, nAgreed int, errs []error)

	// finished will be called when all streams have finished and
	// more than one disk returned an error.
	// Will not be called if everything operates as expected.
	finished func(errs []error)
}

// listPathRaw will list a path on the provided drives.
// See listPathRawOptions on how results are delivered.
// Directories are always returned.
// Cache will be bypassed.
// Context cancellation will be respected but may take a while to effectuate.
func listPathRaw(ctx context.Context, opts listPathRawOptions) (err error) {
	disks := opts.disks
	if len(disks) == 0 {
		return fmt.Errorf("listPathRaw: 0 drives provided")
	}

	// Cancel upstream if we finish before we expect.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fallback := func(err error) bool {
		switch err.(type) {
		case StorageErr:
			// all supported disk errors
			// attempt a fallback.
			return true
		}
		return false
	}
	// 当前纠删集磁盘数量
	askDisks := len(disks)
	// 创建一个与当前纠删集磁盘数量相同的metacacheReader切片
	readers := make([]*metacacheReader, askDisks)
	// 遍历当前纠删集磁盘
	for i := range disks {

		// 创建一个通道，返回r（PipeReader）和w（PipeWriter）
		// r被用于创建metacacheReader
		// w被作为参数传入WalkDir方法，用于写入指定路径下的xl.meta文件
		// 写入操作在下面的协程中进行
		r, w := io.Pipe()

		// Make sure we close the pipe so blocked writes doesn't stay around.
		defer r.CloseWithError(context.Canceled)

		// 向metacacheReader切片中写入新创建的metacacheReader对象
		readers[i] = newMetacacheReader(r)
		// 获取磁盘的实例
		d := disks[i]

		// Send request to each disk.
		go func() {
			var werr error
			if d == nil {
				werr = errDiskNotFound
			} else {
				werr = d.WalkDir(ctx, WalkDirOptions{
					Bucket:         opts.bucket,
					BaseDir:        opts.path,
					Recursive:      opts.recursive,
					ReportNotFound: opts.reportNotFound,
					FilterPrefix:   opts.filterPrefix,
					ForwardTo:      opts.forwardTo,
				}, w)
			}

			// fallback only when set.
			if len(opts.fallbackDisks) > 0 && fallback(werr) {
				// This fallback is only set when
				// askDisks is less than total
				// number of disks per set.
				for _, fd := range opts.fallbackDisks {
					if fd == nil {
						continue
					}
					werr = fd.WalkDir(ctx, WalkDirOptions{
						Bucket:         opts.bucket,
						BaseDir:        opts.path,
						Recursive:      opts.recursive,
						ReportNotFound: opts.reportNotFound,
						FilterPrefix:   opts.filterPrefix,
						ForwardTo:      opts.forwardTo,
					}, w)
					if werr == nil {
						break
					}
				}
			}
			w.CloseWithError(werr)

			if werr != io.EOF && werr != nil &&
				werr.Error() != errFileNotFound.Error() &&
				werr.Error() != errVolumeNotFound.Error() &&
				werr.Error() != errDiskNotFound.Error() &&
				werr.Error() != errUnformattedDisk.Error() &&
				!errors.Is(werr, context.Canceled) {
				logger.LogIf(ctx, werr)
			}
		}()
	}

	// 创建一个metaCacheEntry切片，长度为当前纠删集磁盘数
	topEntries := make(metaCacheEntries, len(readers))
	// 创建一个error的切片，长度当前纠删集磁盘数
	errs := make([]error, len(readers))
	for {
		// Get the top entry from each
		var current metaCacheEntry
		var atEOF, fnf, hasErr, agree int

		// 先将metaCacheEntry切片填满
		for i := range topEntries {
			topEntries[i] = metaCacheEntry{}
		}
		if contextCanceled(ctx) {
			return ctx.Err()
		}

		// 遍历reader的切片
		// 因为reader是在上面的循环中通过walkDir方法写入pipe的，
		// 所以这个这个循环外层用了一个无限制的for循环，直到从所有reader中读完数据为止。
		for i, r := range readers {
			if errs[i] != nil {
				hasErr++
				continue
			}

			// 从pipe中读取数据数据
			entry, err := r.peek()

			switch err {

			// 读到EOF，表示已经有一个盘已经读完了，然后对临时变量atEOF进行加1计数，并继续对下一块盘的数据
			case io.EOF:
				atEOF++
				continue

			// nil表示读取没有发生错误
			case nil:

			// 默认情况下判断错误类型，
			// 如果是errFileNotFound/errVolumeNotFound/errUnformattedDisk/errDiskNotFound，
			// 则对临时变量atEOF和fnf（file not found）进行加1计数并继续读取下一块盘的数据
			// 如果不是上述错误类型，对临时变量hasErr进行加1计数，并继续读取下一块盘的数据
			default:
				switch err.Error() {
				case errFileNotFound.Error(),
					errVolumeNotFound.Error(),
					errUnformattedDisk.Error(),
					errDiskNotFound.Error():
					atEOF++
					fnf++
					continue
				}
				hasErr++
				errs[i] = err
				continue
			}

			// If no current, add it.
			// 没有发生读取错误的场合，判断临时变量current.name是否为空
			// 如果为空，则表示在这个内层循环中有正常读到一块的数据
			// 然后将读到的数据加入到metaCacheEntry切片中，同时将读到的数据赋值给current临时变量
			// 再对临时变量agree进行加1计数，再继续读下一块盘
			// 如果临时变量current.name不为空，表示在之前已经有一块正常读到了数据
			if current.name == "" {
				topEntries[i] = entry
				current = entry
				agree++
				continue
			}

			// If exact match, we agree.
			// 如果临时变量current与本次读到的数据相匹配
			// 匹配正常的场合，将本次读到的数据加入到metaCacheEntry切片中
			// 同时将临时变量agree进行加1计数，再继续读下一块盘
			if _, ok := current.matches(&entry, true); ok {
				topEntries[i] = entry
				agree++
				continue
			}
			// If only the name matches we didn't agree, but add it for resolution.
			// 如果临时变量current与本次读到的数据有相同的对象名称
			// 将本次读到的数据加入到metaCacheEntry切片中，再继续读下一块盘
			if entry.name == current.name {
				topEntries[i] = entry
				continue
			}
			// We got different entries
			// 如果临时变量current与本次读到的数据的对象名称不一致，再继续读下一块盘
			if entry.name > current.name {
				continue
			}
			// We got a new, better current.
			// Clear existing entries.
			for i := range topEntries[:i] {
				topEntries[i] = metaCacheEntry{}
			}
			agree = 1
			current = entry
			topEntries[i] = entry
		}

		// Stop if we exceed number of bad disks
		if hasErr > len(disks)-opts.minDisks && hasErr > 0 {
			if opts.finished != nil {
				opts.finished(errs)
			}
			var combinedErr []string
			for i, err := range errs {
				if err != nil {
					if disks[i] != nil {
						combinedErr = append(combinedErr,
							fmt.Sprintf("disk %s returned: %s", disks[i], err))
					} else {
						combinedErr = append(combinedErr, err.Error())
					}
				}
			}
			return errors.New(strings.Join(combinedErr, ", "))
		}

		// Break if all at EOF or error.
		if atEOF+hasErr == len(readers) {
			if hasErr > 0 && opts.finished != nil {
				opts.finished(errs)
			}
			break
		}
		if fnf == len(readers) {
			return errFileNotFound
		}
		if agree == len(readers) {
			// Everybody agreed
			for _, r := range readers {
				r.skip(1)
			}
			if opts.agreed != nil {
				opts.agreed(current)
			}
			continue
		}
		if opts.partial != nil {
			opts.partial(topEntries, agree, errs)
		}
		// Skip the inputs we used.
		for i, r := range readers {
			if topEntries[i].name != "" {
				r.skip(1)
			}
		}
	}
	return nil
}
