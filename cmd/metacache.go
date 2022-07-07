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
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/minio/minio/internal/logger"
)

type scanStatus uint8

const (
	scanStateNone scanStatus = iota
	scanStateStarted
	scanStateSuccess
	scanStateError

	// Time in which the initiator of a scan must have reported back.
	metacacheMaxRunningAge = time.Minute

	// Max time between client calls before dropping an async cache listing.
	metacacheMaxClientWait = 3 * time.Minute

	// metacacheBlockSize is the number of file/directory entries to have in each block.
	metacacheBlockSize = 5000

	// metacacheSharePrefix controls whether prefixes on dirty paths are always shared.
	// This will make `test/a` and `test/b` share listings if they are concurrent.
	// Enabling this will make cache sharing more likely and cause less IO,
	// but may cause additional latency to some calls.
	metacacheSharePrefix = false
)

//go:generate msgp -file $GOFILE -unexported

// metacache contains a tracked cache entry.
type metacache struct {
	// do not re-arrange the struct this struct has been ordered to use less
	// space - if you do so please run https://github.com/orijtech/structslop
	// and verify if your changes are optimal.
	ended        time.Time  `msg:"end"`
	started      time.Time  `msg:"st"`
	lastHandout  time.Time  `msg:"lh"`
	lastUpdate   time.Time  `msg:"u"`
	bucket       string     `msg:"b"`
	filter       string     `msg:"flt"`
	id           string     `msg:"id"`
	error        string     `msg:"err"`
	root         string     `msg:"root"`
	fileNotFound bool       `msg:"fnf"`
	status       scanStatus `msg:"stat"`
	recursive    bool       `msg:"rec"`
	dataVersion  uint8      `msg:"v"`
}

func (m *metacache) finished() bool {
	return !m.ended.IsZero()
}

// worthKeeping indicates if the cache by itself is worth keeping.
func (m *metacache) worthKeeping() bool {
	if m == nil {
		return false
	}
	cache := m
	switch {
	case !cache.finished() && time.Since(cache.lastUpdate) > metacacheMaxRunningAge:
		// Not finished and update for metacacheMaxRunningAge, discard it.
		return false
	case cache.finished() && time.Since(cache.lastHandout) > 5*metacacheMaxClientWait:
		// Keep for 15 minutes after we last saw the client.
		// Since the cache is finished keeping it a bit longer doesn't hurt us.
		return false
	case cache.status == scanStateError || cache.status == scanStateNone:
		// Remove failed listings after 5 minutes.
		return time.Since(cache.lastUpdate) > 5*time.Minute
	}
	return true
}

// baseDirFromPrefix will return the base directory given an object path.
// For example an object with name prefix/folder/object.ext will return `prefix/folder/`.
func baseDirFromPrefix(prefix string) string {
	b := path.Dir(prefix)
	if b == "." || b == "./" || b == "/" {
		b = ""
	}
	if !strings.Contains(prefix, slashSeparator) {
		b = ""
	}
	if len(b) > 0 && !strings.HasSuffix(b, slashSeparator) {
		b += slashSeparator
	}
	return b
}

// update cache with new status.
// The updates are conditional so multiple callers can update with different states.
func (m *metacache) update(update metacache) {

	// 最后更新时间
	m.lastUpdate = UTCNow()

	if m.lastHandout.After(m.lastHandout) {
		m.lastHandout = UTCNow()
	}
	// 如果原有metacache中的状态为开始扫描，并且更新metacache的状态为已经扫描成功
	// 更新metacache的结束时间
	if m.status == scanStateStarted && update.status == scanStateSuccess {
		m.ended = UTCNow()
	}

	// 如果原有metacache中的状态为开始扫描，并且更新metacache的状态不是开始扫描
	// 更新原有metacache的状态为更新metacache的状态
	if m.status == scanStateStarted && update.status != scanStateStarted {
		m.status = update.status
	}

	// 如果原有metacache中的状态为开始扫描，并且lastHandout到当前时间大于3分钟，则设置错误扫描状态和信息
	if m.status == scanStateStarted && time.Since(m.lastHandout) > metacacheMaxClientWait {
		// Drop if client hasn't been seen for 3 minutes.
		m.status = scanStateError
		m.error = "client not seen"
	}

	// 如果原有metacache中的错误信息为空，并且更新metacache的错误信息不为空
	// 设置原有metacache中的错误信息为更新metacache的错误信息
	// 设置原有metacache中的状态为扫描错误状态
	// 设置原有metacache中的结束时间为当前时间
	if m.error == "" && update.error != "" {
		m.error = update.error
		m.status = scanStateError
		m.ended = UTCNow()
	}

	// 如果原有metacache中的文件未发现状态或更新metacache中的文件为发现状态任意一个为true
	// 设置原有metacache中的文件未发现状态为true，否则为false
	m.fileNotFound = m.fileNotFound || update.fileNotFound
}

// delete all cache data on disks.
func (m *metacache) delete(ctx context.Context) {
	if m.bucket == "" || m.id == "" {
		logger.LogIf(ctx, fmt.Errorf("metacache.delete: bucket (%s) or id (%s) empty", m.bucket, m.id))
	}
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		logger.LogIf(ctx, errors.New("metacache.delete: no object layer"))
		return
	}
	ez, ok := objAPI.(*erasureServerPools)
	if !ok {
		logger.LogIf(ctx, errors.New("metacache.delete: expected objAPI to be *erasureServerPools"))
		return
	}
	ez.renameAll(ctx, minioMetaBucket, metacachePrefixForID(m.bucket, m.id))
}
