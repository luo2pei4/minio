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
	pathutil "path"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/internal/dsync"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/lsync"
)

// local lock servers
var globalLockServer *localLocker

// RWLocker - locker interface to introduce GetRLock, RUnlock.
type RWLocker interface {
	GetLock(ctx context.Context, timeout *dynamicTimeout) (lkCtx LockContext, timedOutErr error)
	Unlock(cancel context.CancelFunc)
	GetRLock(ctx context.Context, timeout *dynamicTimeout) (lkCtx LockContext, timedOutErr error)
	RUnlock(cancel context.CancelFunc)
}

// LockContext lock context holds the lock backed context and canceler for the context.
type LockContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// Context returns lock context
func (l LockContext) Context() context.Context {
	return l.ctx
}

// Cancel function calls cancel() function
func (l LockContext) Cancel() {
	if l.cancel != nil {
		l.cancel()
	}
}

// newNSLock - return a new name space lock map.
// 返回nsLockMap结构体实例的指针
// 如果是分布式锁，不实例nsLockMap结构体中的lockMap数据项，直接返回
func newNSLock(isDistErasure bool) *nsLockMap {
	nsMutex := nsLockMap{
		isDistErasure: isDistErasure,
	}
	if isDistErasure {
		return &nsMutex
	}
	nsMutex.lockMap = make(map[string]*nsLock)
	return &nsMutex
}

// nsLock - provides primitives for locking critical namespace regions.
type nsLock struct {
	ref int32
	*lsync.LRWMutex
}

// nsLockMap - namespace lock map, provides primitives to Lock,
// Unlock, RLock and RUnlock.
type nsLockMap struct {
	// Indicates if namespace is part of a distributed setup.
	isDistErasure bool
	lockMap       map[string]*nsLock
	lockMapMutex  sync.Mutex
}

// Lock the namespace resource.
func (n *nsLockMap) lock(ctx context.Context, volume string, path string, lockSource, opsID string, readLock bool, timeout time.Duration) (locked bool) {
	// 拼接文件的路径
	resource := pathJoin(volume, path)

	n.lockMapMutex.Lock()
	// 如果资源的路径在lockMap中不存在，则新增一个nsLock的实例
	nsLk, found := n.lockMap[resource]
	if !found {
		nsLk = &nsLock{
			LRWMutex: lsync.NewLRWMutex(),
		}
		// Add a count to indicate that a parallel unlock doesn't clear this entry.
	}
	nsLk.ref++
	n.lockMap[resource] = nsLk
	n.lockMapMutex.Unlock()

	// Locking here will block (until timeout).
	if readLock {
		locked = nsLk.GetRLock(ctx, opsID, lockSource, timeout)
	} else {
		locked = nsLk.GetLock(ctx, opsID, lockSource, timeout)
	}

	if !locked { // We failed to get the lock
		// Decrement ref count since we failed to get the lock
		n.lockMapMutex.Lock()
		n.lockMap[resource].ref--
		if n.lockMap[resource].ref < 0 {
			logger.CriticalIf(GlobalContext, errors.New("resource reference count was lower than 0"))
		}
		if n.lockMap[resource].ref == 0 {
			// Remove from the map if there are no more references.
			delete(n.lockMap, resource)
		}
		n.lockMapMutex.Unlock()
	}

	return
}

// Unlock the namespace resource.
func (n *nsLockMap) unlock(volume string, path string, readLock bool) {
	resource := pathJoin(volume, path)

	n.lockMapMutex.Lock()
	defer n.lockMapMutex.Unlock()
	if _, found := n.lockMap[resource]; !found {
		return
	}
	if readLock {
		n.lockMap[resource].RUnlock()
	} else {
		n.lockMap[resource].Unlock()
	}
	n.lockMap[resource].ref--
	if n.lockMap[resource].ref < 0 {
		logger.CriticalIf(GlobalContext, errors.New("resource reference count was lower than 0"))
	}
	if n.lockMap[resource].ref == 0 {
		// Remove from the map if there are no more references.
		delete(n.lockMap, resource)
	}
}

// dsync's distributed lock instance.
type distLockInstance struct {
	rwMutex *dsync.DRWMutex
	opsID   string
}

// Lock - block until write lock is taken or timeout has occurred.
func (di *distLockInstance) GetLock(ctx context.Context, timeout *dynamicTimeout) (LockContext, error) {
	lockSource := getSource(2)
	start := UTCNow()

	newCtx, cancel := context.WithCancel(ctx)
	if !di.rwMutex.GetLock(newCtx, cancel, di.opsID, lockSource, dsync.Options{
		Timeout: timeout.Timeout(),
	}) {
		timeout.LogFailure()
		cancel()
		return LockContext{ctx: ctx, cancel: func() {}}, OperationTimedOut{}
	}
	timeout.LogSuccess(UTCNow().Sub(start))
	return LockContext{ctx: newCtx, cancel: cancel}, nil
}

// Unlock - block until write lock is released.
func (di *distLockInstance) Unlock(cancel context.CancelFunc) {
	if cancel != nil {
		cancel()
	}
	di.rwMutex.Unlock()
}

// RLock - block until read lock is taken or timeout has occurred.
func (di *distLockInstance) GetRLock(ctx context.Context, timeout *dynamicTimeout) (LockContext, error) {
	lockSource := getSource(2)
	start := UTCNow()

	newCtx, cancel := context.WithCancel(ctx)
	if !di.rwMutex.GetRLock(ctx, cancel, di.opsID, lockSource, dsync.Options{
		Timeout: timeout.Timeout(),
	}) {
		timeout.LogFailure()
		cancel()
		return LockContext{ctx: ctx, cancel: func() {}}, OperationTimedOut{}
	}
	timeout.LogSuccess(UTCNow().Sub(start))
	return LockContext{ctx: newCtx, cancel: cancel}, nil
}

// RUnlock - block until read lock is released.
func (di *distLockInstance) RUnlock(cancel context.CancelFunc) {
	if cancel != nil {
		cancel()
	}
	di.rwMutex.RUnlock()
}

// localLockInstance - frontend/top-level interface for namespace locks.
type localLockInstance struct {
	ns     *nsLockMap
	volume string
	paths  []string
	opsID  string
}

// NewNSLock - returns a lock instance for a given volume and
// path. The returned lockInstance object encapsulates the nsLockMap,
// volume, path and operation ID.
//
//	返回读写锁对象。
//	分布式模式下返回distLockInstance对象指针，
//	非分布式模式下返回localLockInstance对象指针
func (n *nsLockMap) NewNSLock(lockers func() ([]dsync.NetLocker, string), volume string, paths ...string) RWLocker {
	opsID := mustGetUUID()
	if n.isDistErasure {
		drwmutex := dsync.NewDRWMutex(&dsync.Dsync{
			GetLockers: lockers,
		}, pathsJoinPrefix(volume, paths...)...)
		return &distLockInstance{drwmutex, opsID}
	}
	sort.Strings(paths)
	return &localLockInstance{n, volume, paths, opsID}
}

// Lock - block until write lock is taken or timeout has occurred.
func (li *localLockInstance) GetLock(ctx context.Context, timeout *dynamicTimeout) (_ LockContext, timedOutErr error) {
	// 返回上溯两个调用栈的信息
	lockSource := getSource(2)
	start := UTCNow()
	const readLock = false
	success := make([]int, len(li.paths))
	// 对所有资源加锁，如果一个加锁失败，就释放已经加锁的资源
	// 对每个资源进行加锁时有重试机制，只有超时的时候才会报错，所以如果一个资源加锁失败，在释放完已加锁资源后，返回的是超时错误
	for i, path := range li.paths {
		if !li.ns.lock(ctx, li.volume, path, lockSource, li.opsID, readLock, timeout.Timeout()) {
			timeout.LogFailure()
			for si, sint := range success {
				if sint == 1 {
					li.ns.unlock(li.volume, li.paths[si], readLock)
				}
			}
			return LockContext{}, OperationTimedOut{}
		}
		success[i] = 1
	}
	timeout.LogSuccess(UTCNow().Sub(start))
	return LockContext{ctx: ctx, cancel: func() {}}, nil
}

// Unlock - block until write lock is released.
func (li *localLockInstance) Unlock(cancel context.CancelFunc) {
	if cancel != nil {
		cancel()
	}
	const readLock = false
	for _, path := range li.paths {
		li.ns.unlock(li.volume, path, readLock)
	}
}

// RLock - block until read lock is taken or timeout has occurred.
func (li *localLockInstance) GetRLock(ctx context.Context, timeout *dynamicTimeout) (_ LockContext, timedOutErr error) {
	lockSource := getSource(2)
	start := UTCNow()
	const readLock = true
	success := make([]int, len(li.paths))
	for i, path := range li.paths {
		if !li.ns.lock(ctx, li.volume, path, lockSource, li.opsID, readLock, timeout.Timeout()) {
			timeout.LogFailure()
			for si, sint := range success {
				if sint == 1 {
					li.ns.unlock(li.volume, li.paths[si], readLock)
				}
			}
			return LockContext{}, OperationTimedOut{}
		}
		success[i] = 1
	}
	timeout.LogSuccess(UTCNow().Sub(start))
	return LockContext{ctx: ctx, cancel: func() {}}, nil
}

// RUnlock - block until read lock is released.
func (li *localLockInstance) RUnlock(cancel context.CancelFunc) {
	if cancel != nil {
		cancel()
	}
	const readLock = true
	for _, path := range li.paths {
		li.ns.unlock(li.volume, path, readLock)
	}
}

func getSource(n int) string {
	var funcName string
	pc, filename, lineNum, ok := runtime.Caller(n)
	if ok {
		filename = pathutil.Base(filename)
		funcName = strings.TrimPrefix(runtime.FuncForPC(pc).Name(),
			"github.com/minio/minio/cmd.")
	} else {
		filename = "<unknown>"
		lineNum = 0
	}

	return fmt.Sprintf("[%s:%d:%s()]", filename, lineNum, funcName)
}
