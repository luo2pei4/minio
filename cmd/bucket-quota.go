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
	"encoding/json"
	"fmt"
	"time"

	"github.com/minio/madmin-go"
)

// BucketQuotaSys - map of bucket and quota configuration.
// 桶和配额配置的map
type BucketQuotaSys struct {
	bucketStorageCache timedValue
}

// Get - Get quota configuration.
func (sys *BucketQuotaSys) Get(ctx context.Context, bucketName string) (*madmin.BucketQuota, error) {
	if globalIsGateway {
		objAPI := newObjectLayerFn()
		if objAPI == nil {
			return nil, errServerNotInitialized
		}
		return &madmin.BucketQuota{}, nil
	}

	return globalBucketMetadataSys.GetQuotaConfig(ctx, bucketName)
}

// NewBucketQuotaSys returns initialized BucketQuotaSys
func NewBucketQuotaSys() *BucketQuotaSys {
	return &BucketQuotaSys{}
}

// Init initialize bucket quota.
func (sys *BucketQuotaSys) Init(objAPI ObjectLayer) {
	sys.bucketStorageCache.Once.Do(func() {
		sys.bucketStorageCache.TTL = 1 * time.Second
		sys.bucketStorageCache.Update = func() (interface{}, error) {
			ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
			defer done()

			return loadDataUsageFromBackend(ctx, objAPI)
		}
	})
}

// GetBucketUsageInfo return bucket usage info for a given bucket
func (sys *BucketQuotaSys) GetBucketUsageInfo(bucket string) (BucketUsageInfo, error) {
	v, err := sys.bucketStorageCache.Get()
	if err != nil {
		return BucketUsageInfo{}, err
	}

	dui, ok := v.(DataUsageInfo)
	if !ok {
		return BucketUsageInfo{}, fmt.Errorf("internal error: Unexpected DUI data type: %T", v)
	}

	bui := dui.BucketsUsage[bucket]
	return bui, nil
}

// parseBucketQuota parses BucketQuota from json
func parseBucketQuota(bucket string, data []byte) (quotaCfg *madmin.BucketQuota, err error) {
	quotaCfg = &madmin.BucketQuota{}
	if err = json.Unmarshal(data, quotaCfg); err != nil {
		return quotaCfg, err
	}
	if !quotaCfg.IsValid() {
		return quotaCfg, fmt.Errorf("Invalid quota config %#v", quotaCfg)
	}
	return
}

// 判断配额hard模式下是否新增对象大小是否会超过配额
func (sys *BucketQuotaSys) enforceQuotaHard(ctx context.Context, bucket string, size int64) error {
	if size < 0 {
		return nil
	}

	// 获取桶的配额信息
	q, err := sys.Get(ctx, bucket)
	if err != nil {
		return err
	}

	// 判断配额类型是否为hard，且配额大于0
	if q != nil && q.Type == madmin.HardQuota && q.Quota > 0 {
		// hard模式的场合，获取桶的使用量
		bui, err := sys.GetBucketUsageInfo(bucket)
		if err != nil {
			return err
		}

		// 如果使用量大于0，并且已使用量+对象大小大于等于配额大小，返回错误
		if bui.Size > 0 && ((bui.Size + uint64(size)) >= q.Quota) {
			return BucketQuotaExceeded{Bucket: bucket}
		}
	}

	return nil
}

// 判断配额hard模式下是否新增对象大小是否会超过配额
func enforceBucketQuotaHard(ctx context.Context, bucket string, size int64) error {
	if globalBucketQuotaSys == nil {
		return nil
	}
	return globalBucketQuotaSys.enforceQuotaHard(ctx, bucket, size)
}
