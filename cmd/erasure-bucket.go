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

	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/sync/errgroup"
)

// list all errors that can be ignore in a bucket operation.
var bucketOpIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied, errUnformattedDisk)

// list all errors that can be ignored in a bucket metadata operation.
var bucketMetadataOpIgnoredErrs = append(bucketOpIgnoredErrs, errVolumeNotFound)

// Bucket operations

// MakeBucket - make a bucket.
func (er erasureObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts BucketOptions) error {
	defer NSUpdated(bucket, slashSeparator)

	// Verify if bucket is valid.
	if err := s3utils.CheckValidBucketNameStrict(bucket); err != nil {
		return BucketNameInvalid{Bucket: bucket}
	}

	storageDisks := er.getDisks()

	g := errgroup.WithNErrs(len(storageDisks))

	// Make a volume entry on all underlying storage disks.
	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if storageDisks[index] != nil {
				if err := storageDisks[index].MakeVol(ctx, bucket); err != nil {
					if !errors.Is(err, errVolumeExists) {
						logger.LogIf(ctx, err)
					}
					return err
				}
				return nil
			}
			return errDiskNotFound
		}, index)
	}

	err := reduceWriteQuorumErrs(ctx, g.Wait(), bucketOpIgnoredErrs, er.defaultWQuorum())
	return toObjectErr(err, bucket)
}

func undoDeleteBucket(storageDisks []StorageAPI, bucket string) {
	g := errgroup.WithNErrs(len(storageDisks))
	// Undo previous make bucket entry on all underlying storage disks.
	for index := range storageDisks {
		if storageDisks[index] == nil {
			continue
		}
		index := index
		g.Go(func() error {
			_ = storageDisks[index].MakeVol(context.Background(), bucket)
			return nil
		}, index)
	}

	// Wait for all make vol to finish.
	g.Wait()
}

// getBucketInfo - returns the BucketInfo from one of the load balanced disks.
func (er erasureObjects) getBucketInfo(ctx context.Context, bucketName string) (bucketInfo BucketInfo, err error) {
	storageDisks := er.getDisks()

	g := errgroup.WithNErrs(len(storageDisks))
	bucketsInfo := make([]BucketInfo, len(storageDisks))
	// Undo previous make bucket entry on all underlying storage disks.
	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if storageDisks[index] == nil {
				return errDiskNotFound
			}
			volInfo, err := storageDisks[index].StatVol(ctx, bucketName)
			if err != nil {
				return err
			}
			bucketsInfo[index] = BucketInfo(volInfo)
			return nil
		}, index)
	}

	errs := g.Wait()

	for i, err := range errs {
		if err == nil {
			return bucketsInfo[i], nil
		}
	}

	// If all our errors were ignored, then we try to
	// reduce to one error based on read quorum.
	// `nil` is deliberately passed for ignoredErrs
	// because these errors were already ignored.
	return BucketInfo{}, reduceReadQuorumErrs(ctx, errs, nil, er.defaultRQuorum())
}

// GetBucketInfo - returns BucketInfo for a bucket.
func (er erasureObjects) GetBucketInfo(ctx context.Context, bucket string) (bi BucketInfo, e error) {
	bucketInfo, err := er.getBucketInfo(ctx, bucket)
	if err != nil {
		return bi, toObjectErr(err, bucket)
	}
	return bucketInfo, nil
}

// DeleteBucket - deletes a bucket.
func (er erasureObjects) DeleteBucket(ctx context.Context, bucket string, opts DeleteBucketOptions) error {
	// Collect if all disks report volume not found.
	defer NSUpdated(bucket, slashSeparator)

	// 获取单个set中的所有硬盘信息
	storageDisks := er.getDisks()

	g := errgroup.WithNErrs(len(storageDisks))

	// 遍历每个硬盘，并行删除硬盘上的桶数据
	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if storageDisks[index] != nil {
				return storageDisks[index].DeleteVol(ctx, bucket, opts.Force)
			}
			return errDiskNotFound
		}, index)
	}

	// Wait for all the delete vols to finish.
	// 获取每块盘上桶删除操作的异常
	dErrs := g.Wait()

	// 如果指定了强制删除，但set中任一一块盘删除桶发生异常时，做undo操作并返回删除时产生的异常
	if opts.Force {
		for _, err := range dErrs {
			if err != nil {
				undoDeleteBucket(storageDisks, bucket)
				return toObjectErr(err, bucket)
			}
		}

		return nil
	}

	// 判断删桶操作时产生的异常数量是否超过写仲裁数
	// 如果超过写仲裁数，做undo操作
	err := reduceWriteQuorumErrs(ctx, dErrs, bucketOpIgnoredErrs, er.defaultWQuorum())
	if err == errErasureWriteQuorum && !opts.NoRecreate {
		undoDeleteBucket(storageDisks, bucket)
	}

	// 删桶操作产生的异常未超过写仲裁数，或在当前set中没有找到要删除的桶
	if err == nil || errors.Is(err, errVolumeNotFound) {
		var purgedDangling bool
		// At this point we have `err == nil` but some errors might be `errVolumeNotEmpty`
		// we should proceed to attempt a force delete of such buckets.
		// 删桶操作返回的所有异常信息虽然通过reduceWriteQuorumErrs函数被判断为成功删除或无法找到要删除的桶
		// 但是在返回的异常情况中可能存在桶里有数据所以无法删除的异常
		// 针对这种异常，需要将这些桶目录通过rename的方式放入垃圾筒中
		for index, err := range dErrs {
			if err == errVolumeNotEmpty && storageDisks[index] != nil {
				storageDisks[index].RenameFile(ctx, bucket, "", minioMetaTmpDeletedBucket, mustGetUUID())
				purgedDangling = true
			}
		}
		// if we purged dangling buckets, ignore errVolumeNotFound error.
		if purgedDangling {
			err = nil
		}

	}

	return toObjectErr(err, bucket)
}

// IsNotificationSupported returns whether bucket notification is applicable for this layer.
func (er erasureObjects) IsNotificationSupported() bool {
	return true
}

// IsListenSupported returns whether listen bucket notification is applicable for this layer.
func (er erasureObjects) IsListenSupported() bool {
	return true
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (er erasureObjects) IsEncryptionSupported() bool {
	return true
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (er erasureObjects) IsCompressionSupported() bool {
	return true
}

// IsTaggingSupported indicates whether erasureObjects implements tagging support.
func (er erasureObjects) IsTaggingSupported() bool {
	return true
}
