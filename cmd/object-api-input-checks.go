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
	"runtime"
	"strings"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio/internal/logger"
)

// Checks on GetObject arguments, bucket and object.
func checkGetObjArgs(ctx context.Context, bucket, object string) error {
	return checkBucketAndObjectNames(ctx, bucket, object)
}

// Checks on DeleteObject arguments, bucket and object.
func checkDelObjArgs(ctx context.Context, bucket, object string) error {
	return checkBucketAndObjectNames(ctx, bucket, object)
}

// Checks bucket and object name validity, returns nil if both are valid.
func checkBucketAndObjectNames(ctx context.Context, bucket, object string) error {
	// Verify if bucket is valid.
	if !isMinioMetaBucketName(bucket) && s3utils.CheckValidBucketName(bucket) != nil {
		logger.LogIf(ctx, BucketNameInvalid{Bucket: bucket})
		return BucketNameInvalid{Bucket: bucket}
	}
	// Verify if object is valid.
	if len(object) == 0 {
		logger.LogIf(ctx, ObjectNameInvalid{Bucket: bucket, Object: object})
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	if !IsValidObjectPrefix(object) {
		logger.LogIf(ctx, ObjectNameInvalid{Bucket: bucket, Object: object})
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	if runtime.GOOS == globalWindowsOSName && strings.Contains(object, "\\") {
		// Objects cannot be contain \ in Windows and is listed as `Characters to Avoid`.
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	return nil
}

// Checks for all ListObjects arguments validity.
func checkListObjsArgs(ctx context.Context, bucket, prefix, marker string, obj getBucketInfoI) error {
	// Verify if bucket exists before validating object name.
	// This is done on purpose since the order of errors is
	// important here bucket does not exist error should
	// happen before we return an error for invalid object name.
	// FIXME: should be moved to handler layer.
	// 检查桶是否存在
	if err := checkBucketExist(ctx, bucket, obj); err != nil {
		return err
	}
	// Validates object prefix validity after bucket exists.
	// 检查prefix是否有效
	if !IsValidObjectPrefix(prefix) {
		logger.LogIf(ctx, ObjectNameInvalid{
			Bucket: bucket,
			Object: prefix,
		})
		return ObjectNameInvalid{
			Bucket: bucket,
			Object: prefix,
		}
	}
	// Verify if marker has prefix.
	// 当marker存在的场合，检查marker中是否包含了prefix
	if marker != "" && !HasPrefix(marker, prefix) {
		logger.LogIf(ctx, InvalidMarkerPrefixCombination{
			Marker: marker,
			Prefix: prefix,
		})
		return InvalidMarkerPrefixCombination{
			Marker: marker,
			Prefix: prefix,
		}
	}
	return nil
}

// Checks for all ListMultipartUploads arguments validity.
func checkListMultipartArgs(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, obj ObjectLayer) error {
	if err := checkListObjsArgs(ctx, bucket, prefix, keyMarker, obj); err != nil {
		return err
	}
	if uploadIDMarker != "" {
		if HasSuffix(keyMarker, SlashSeparator) {

			logger.LogIf(ctx, InvalidUploadIDKeyCombination{
				UploadIDMarker: uploadIDMarker,
				KeyMarker:      keyMarker,
			})
			return InvalidUploadIDKeyCombination{
				UploadIDMarker: uploadIDMarker,
				KeyMarker:      keyMarker,
			}
		}
		if _, err := uuid.Parse(uploadIDMarker); err != nil {
			logger.LogIf(ctx, err)
			return MalformedUploadID{
				UploadID: uploadIDMarker,
			}
		}
	}
	return nil
}

// Checks for NewMultipartUpload arguments validity, also validates if bucket exists.
func checkNewMultipartArgs(ctx context.Context, bucket, object string, obj ObjectLayer) error {
	return checkObjectArgs(ctx, bucket, object, obj)
}

// Checks for PutObjectPart arguments validity, also validates if bucket exists.
func checkPutObjectPartArgs(ctx context.Context, bucket, object string, obj ObjectLayer) error {
	return checkObjectArgs(ctx, bucket, object, obj)
}

// Checks for ListParts arguments validity, also validates if bucket exists.
func checkListPartsArgs(ctx context.Context, bucket, object string, obj ObjectLayer) error {
	return checkObjectArgs(ctx, bucket, object, obj)
}

// Checks for CompleteMultipartUpload arguments validity, also validates if bucket exists.
func checkCompleteMultipartArgs(ctx context.Context, bucket, object string, obj ObjectLayer) error {
	return checkObjectArgs(ctx, bucket, object, obj)
}

// Checks for AbortMultipartUpload arguments validity, also validates if bucket exists.
func checkAbortMultipartArgs(ctx context.Context, bucket, object string, obj ObjectLayer) error {
	return checkObjectArgs(ctx, bucket, object, obj)
}

// Checks Object arguments validity, also validates if bucket exists.
func checkObjectArgs(ctx context.Context, bucket, object string, obj ObjectLayer) error {
	// Verify if bucket exists before validating object name.
	// This is done on purpose since the order of errors is
	// important here bucket does not exist error should
	// happen before we return an error for invalid object name.
	// FIXME: should be moved to handler layer.
	if err := checkBucketExist(ctx, bucket, obj); err != nil {
		return err
	}

	if err := checkObjectNameForLengthAndSlash(bucket, object); err != nil {
		return err
	}

	// Validates object name validity after bucket exists.
	if !IsValidObjectName(object) {
		return ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		}
	}

	return nil
}

// Checks for PutObject arguments validity, also validates if bucket exists.
// 检查上传对象的参数，主要验证桶是否存在，对象名称是否有无效的字符串。
// 如果检查失败，返回错误信息，检查成功的场合返回nil
func checkPutObjectArgs(ctx context.Context, bucket, object string, obj getBucketInfoI) error {
	// Verify if bucket exists before validating object name.
	// This is done on purpose since the order of errors is
	// important here bucket does not exist error should
	// happen before we return an error for invalid object name.
	// FIXME: should be moved to handler layer.
	if err := checkBucketExist(ctx, bucket, obj); err != nil {
		return err
	}

	// 检查对象名称的长度是否超过1024个字符，检查对象名称是否包含前缀“/”
	if err := checkObjectNameForLengthAndSlash(bucket, object); err != nil {
		return err
	}
	// 如果对象名称是否是空字符串，或者对象名中包含无效的前缀，则返回对象名无效的错误
	if len(object) == 0 ||
		!IsValidObjectPrefix(object) {
		return ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		}
	}
	return nil
}

type getBucketInfoI interface {
	GetBucketInfo(ctx context.Context, bucket string) (bucketInfo BucketInfo, err error)
}

// Checks whether bucket exists and returns appropriate error if not.
// 尝试获取桶的信息，如果可以正常获取则返回nil，如果获取桶信息发生错误，则返回错误
func checkBucketExist(ctx context.Context, bucket string, obj getBucketInfoI) error {
	// 集群和纠删模式下，调用的是erasureServerPool结构体的GetBucketInfo方法
	_, err := obj.GetBucketInfo(ctx, bucket)
	if err != nil {
		return err
	}
	return nil
}
