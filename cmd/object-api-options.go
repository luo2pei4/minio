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
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/minio/minio/internal/crypto"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
)

// set encryption options for pass through to backend in the case of gateway and UserDefined metadata
// 在网关和 UserDefined 元数据的情况下设置传递到后端的加密选项
func getDefaultOpts(header http.Header, copySource bool, metadata map[string]string) (opts ObjectOptions, err error) {
	var clientKey [32]byte
	var sse encrypt.ServerSide

	opts = ObjectOptions{UserDefined: metadata}
	// 当前文件中getOpts函数调用的场合，copySource为false
	if copySource {
		if crypto.SSECopy.IsRequested(header) {
			clientKey, err = crypto.SSECopy.ParseHTTP(header)
			if err != nil {
				return
			}
			if sse, err = encrypt.NewSSEC(clientKey[:]); err != nil {
				return
			}
			opts.ServerSideEncryption = encrypt.SSECopy(sse)
			return
		}
		return
	}

	// 判断请求头中是否含有SSE-C相关的参数
	if crypto.SSEC.IsRequested(header) {
		// 返回X-Amz-Server-Side-Encryption-Customer-Key参数值的解码结果
		clientKey, err = crypto.SSEC.ParseHTTP(header)
		if err != nil {
			return
		}
		if sse, err = encrypt.NewSSEC(clientKey[:]); err != nil {
			return
		}
		// 设置服务端加密信息并返回
		opts.ServerSideEncryption = sse
		return
	}
	if crypto.S3.IsRequested(header) || (metadata != nil && crypto.S3.IsEncrypted(metadata)) {
		opts.ServerSideEncryption = encrypt.NewSSE()
	}
	// 请求头中包含X-Minio-Source-Proxy-Request参数的场合
	if v, ok := header[xhttp.MinIOSourceProxyRequest]; ok {
		opts.ProxyHeaderSet = true
		opts.ProxyRequest = strings.Join(v, "") == "true"
	}
	// 请求头中包含X-Minio-Source-Replication-Request参数的场合，表明该请求是一个复制操作的请求
	if _, ok := header[xhttp.MinIOSourceReplicationRequest]; ok {
		opts.ReplicationRequest = true
	}
	return
}

// get ObjectOptions for GET calls from encryption headers
func getOpts(ctx context.Context, r *http.Request, bucket, object string) (ObjectOptions, error) {
	var (
		encryption encrypt.ServerSide
		opts       ObjectOptions
	)

	var partNumber int
	var err error
	// 检查请求中是否带有partNumber参数（在url的query部分），
	// 如果设置了partNumber，判断该参数的值是否是数字且小于等于0，
	// 如果不是数值或小于等于0，则返回错误
	if pn := r.Form.Get(xhttp.PartNumber); pn != "" {
		partNumber, err = strconv.Atoi(pn)
		if err != nil {
			return opts, err
		}
		if partNumber <= 0 {
			return opts, errInvalidArgument
		}
	}

	// 判断请求中是否带有versionId参数，
	// 如果带有versionId参数，且该参数的值不为空或"null"，则进一步判断是否是有效的UUID
	// 如果不是有效的UUID则返回错误
	vid := strings.TrimSpace(r.Form.Get(xhttp.VersionID))
	if vid != "" && vid != nullVersionID {
		_, err := uuid.Parse(vid)
		if err != nil {
			logger.LogIf(ctx, err)
			return opts, InvalidVersionID{
				Bucket:    bucket,
				Object:    object,
				VersionID: vid,
			}
		}
	}

	// ******网关的场景暂不做解析******
	if GlobalGatewaySSE.SSEC() && crypto.SSEC.IsRequested(r.Header) {
		key, err := crypto.SSEC.ParseHTTP(r.Header)
		if err != nil {
			return opts, err
		}
		derivedKey := deriveClientKey(key, bucket, object)
		encryption, err = encrypt.NewSSEC(derivedKey[:])
		logger.CriticalIf(ctx, err)
		return ObjectOptions{
			ServerSideEncryption: encryption,
			VersionID:            vid,
			PartNumber:           partNumber,
		}, nil
	}

	// 判断请求头中是否含有强制删除（x-minio-force-delete）设置，
	// 如果有强制删除设置，将该设置的值转换成bool类型值。
	// 强制删除设置的值true时，表示该对象可以被强制删除（包含指定前缀删除的场景）。
	deletePrefix := false
	if d := r.Header.Get(xhttp.MinIOForceDelete); d != "" {
		if b, err := strconv.ParseBool(d); err == nil {
			deletePrefix = b
		} else {
			return opts, err
		}
	}

	// default case of passing encryption headers to backend
	// 设置加密相关内容
	opts, err = getDefaultOpts(r.Header, false, nil)
	if err != nil {
		return opts, err
	}
	opts.DeletePrefix = deletePrefix
	opts.PartNumber = partNumber
	opts.VersionID = vid
	// 检查请求头中是否有x-minio-source-deletemarker参数。
	// 如果有的话，解析该参数的值，如果是true和false以外的值则返回参数错误
	// 该参数主要用于标明是否在用户端保留删除标识
	delMarker := strings.TrimSpace(r.Header.Get(xhttp.MinIOSourceDeleteMarker))
	if delMarker != "" {
		switch delMarker {
		case "true":
			opts.DeleteMarker = true
		case "false":
		default:
			err = fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceDeleteMarker, fmt.Errorf("DeleteMarker should be true or false"))
			logger.LogIf(ctx, err)
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    err,
			}
		}
	}
	return opts, nil
}

func delOpts(ctx context.Context, r *http.Request, bucket, object string) (opts ObjectOptions, err error) {
	versioned := globalBucketVersioningSys.Enabled(bucket)
	opts, err = getOpts(ctx, r, bucket, object)
	if err != nil {
		return opts, err
	}
	opts.Versioned = versioned
	opts.VersionSuspended = globalBucketVersioningSys.Suspended(bucket)
	delMarker := strings.TrimSpace(r.Header.Get(xhttp.MinIOSourceDeleteMarker))
	if delMarker != "" {
		switch delMarker {
		case "true":
			opts.DeleteMarker = true
		case "false":
		default:
			err = fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceDeleteMarker, fmt.Errorf("DeleteMarker should be true or false"))
			logger.LogIf(ctx, err)
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    err,
			}
		}
	}

	mtime := strings.TrimSpace(r.Header.Get(xhttp.MinIOSourceMTime))
	if mtime != "" {
		opts.MTime, err = time.Parse(time.RFC3339Nano, mtime)
		if err != nil {
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceMTime, err),
			}
		}
	} else {
		opts.MTime = UTCNow()
	}
	return opts, nil
}

// get ObjectOptions for PUT calls from encryption headers and metadata
func putOpts(ctx context.Context, r *http.Request, bucket, object string, metadata map[string]string) (opts ObjectOptions, err error) {
	// 获取桶是否指定多版本
	versioned := globalBucketVersioningSys.Enabled(bucket)
	// 获取桶的多版本支持是否被挂起
	versionSuspended := globalBucketVersioningSys.Suspended(bucket)
	// 从请求中获取版本ID
	vid := strings.TrimSpace(r.Form.Get(xhttp.VersionID))
	// 版本ID不为空切不等于"null"的场合
	if vid != "" && vid != nullVersionID {
		// 解析版本ID，判断版本ID是否是有效的ID
		_, err := uuid.Parse(vid)
		if err != nil {
			logger.LogIf(ctx, err)
			return opts, InvalidVersionID{
				Bucket:    bucket,
				Object:    object,
				VersionID: vid,
			}
		}
		// 如果桶不支持多版本但请求中又设置了版本ID，返回错误。
		if !versioned {
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    fmt.Errorf("VersionID specified %s, but versioning not enabled on  %s", opts.VersionID, bucket),
			}
		}
	}
	// 从请求头中获取参数x-minio-source-mtime的值
	mtimeStr := strings.TrimSpace(r.Header.Get(xhttp.MinIOSourceMTime))
	// 获取当前时间
	mtime := UTCNow()
	// x-minio-source-mtime的值不为空的情况下，解析该值是否是正确的时间格式。
	// 保存解析后的时间，如果不是有效的时间格式，返回错误
	if mtimeStr != "" {
		mtime, err = time.Parse(time.RFC3339, mtimeStr)
		if err != nil {
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceMTime, err),
			}
		}
	}
	// 从请求头中获取参数X-Minio-Source-Replication-Retention-Timestamp的值
	retaintimeStr := strings.TrimSpace(r.Header.Get(xhttp.MinIOSourceObjectRetentionTimestamp))
	// 先将源对象的mtime复制给retaintimestmp
	retaintimestmp := mtime
	// 如果X-Minio-Source-Replication-Retention-Timestamp的值不为空，解析该值是否是正确的时间格式
	// 保存解析后的时间，如果不是有效时间格式，返货错误
	if retaintimeStr != "" {
		retaintimestmp, err = time.Parse(time.RFC3339, retaintimeStr)
		if err != nil {
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceObjectRetentionTimestamp, err),
			}
		}
	}

	// 解析请求头中X-Minio-Source-Replication-LegalHold-Timestamp参数的值
	lholdtimeStr := strings.TrimSpace(r.Header.Get(xhttp.MinIOSourceObjectLegalHoldTimestamp))
	lholdtimestmp := mtime
	if lholdtimeStr != "" {
		lholdtimestmp, err = time.Parse(time.RFC3339, lholdtimeStr)
		if err != nil {
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceObjectLegalHoldTimestamp, err),
			}
		}
	}
	// 解析请求头中X-Minio-Source-Replication-Tagging-Timestamp参数的值
	tagtimeStr := strings.TrimSpace(r.Header.Get(xhttp.MinIOSourceTaggingTimestamp))
	taggingtimestmp := mtime
	if tagtimeStr != "" {
		taggingtimestmp, err = time.Parse(time.RFC3339, tagtimeStr)
		if err != nil {
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceTaggingTimestamp, err),
			}
		}
	}

	// 解析请求头中x-minio-source-etag参数的值
	// 如果该参数的值不为空，保存到metadata中。
	// PutObject请求的场合，在传入该函数前至少已经保存了Content-Type和Content-Encoding两个参数的值
	// 此处保存对象的标签值
	etag := strings.TrimSpace(r.Header.Get(xhttp.MinIOSourceETag))
	if etag != "" {
		if metadata == nil {
			metadata = make(map[string]string, 1)
		}
		metadata["etag"] = etag
	}

	// In the case of multipart custom format, the metadata needs to be checked in addition to header to see if it
	// is SSE-S3 encrypted, primarily because S3 protocol does not require SSE-S3 headers in PutObjectPart calls
	if GlobalGatewaySSE.SSES3() && (crypto.S3.IsRequested(r.Header) || crypto.S3.IsEncrypted(metadata)) {
		return ObjectOptions{
			ServerSideEncryption: encrypt.NewSSE(),
			UserDefined:          metadata,
			VersionID:            vid,
			Versioned:            versioned,
			VersionSuspended:     versionSuspended,
			MTime:                mtime,
		}, nil
	}
	if GlobalGatewaySSE.SSEC() && crypto.SSEC.IsRequested(r.Header) {
		opts, err = getOpts(ctx, r, bucket, object)
		opts.VersionID = vid
		opts.Versioned = versioned
		opts.VersionSuspended = versionSuspended
		opts.UserDefined = metadata
		return
	}
	// 判断请求头中是否包含有任一一种SSE-KMS的参数
	if crypto.S3KMS.IsRequested(r.Header) {
		// 解析请求头中的SSE-KMS相关参数，返回SSE-KMS的KeyID，context。
		keyID, context, err := crypto.S3KMS.ParseHTTP(r.Header)
		if err != nil {
			return ObjectOptions{}, err
		}
		// 用解析的KeyID和Context创建新的kms结构体实例，该结构体实现了服务器端接口
		sseKms, err := encrypt.NewSSEKMS(keyID, context)
		if err != nil {
			return ObjectOptions{}, err
		}
		return ObjectOptions{
			ServerSideEncryption: sseKms,
			UserDefined:          metadata,
			VersionID:            vid,
			Versioned:            versioned,
			VersionSuspended:     versionSuspended,
			MTime:                mtime,
		}, nil
	}
	// default case of passing encryption headers and UserDefined metadata to backend
	opts, err = getDefaultOpts(r.Header, false, metadata)
	if err != nil {
		return opts, err
	}
	opts.VersionID = vid
	opts.Versioned = versioned
	opts.VersionSuspended = versionSuspended
	opts.MTime = mtime
	opts.ReplicationSourceLegalholdTimestamp = lholdtimestmp
	opts.ReplicationSourceRetentionTimestamp = retaintimestmp
	opts.ReplicationSourceTaggingTimestamp = taggingtimestmp
	return opts, nil
}

// get ObjectOptions for Copy calls with encryption headers provided on the target side and source side metadata
func copyDstOpts(ctx context.Context, r *http.Request, bucket, object string, metadata map[string]string) (opts ObjectOptions, err error) {
	return putOpts(ctx, r, bucket, object, metadata)
}

// get ObjectOptions for Copy calls with encryption headers provided on the source side
func copySrcOpts(ctx context.Context, r *http.Request, bucket, object string) (ObjectOptions, error) {
	var (
		ssec encrypt.ServerSide
		opts ObjectOptions
	)

	if GlobalGatewaySSE.SSEC() && crypto.SSECopy.IsRequested(r.Header) {
		key, err := crypto.SSECopy.ParseHTTP(r.Header)
		if err != nil {
			return opts, err
		}
		derivedKey := deriveClientKey(key, bucket, object)
		ssec, err = encrypt.NewSSEC(derivedKey[:])
		if err != nil {
			return opts, err
		}
		return ObjectOptions{ServerSideEncryption: encrypt.SSECopy(ssec)}, nil
	}

	// default case of passing encryption headers to backend
	opts, err := getDefaultOpts(r.Header, false, nil)
	if err != nil {
		return opts, err
	}
	return opts, nil
}

// get ObjectOptions for CompleteMultipart calls
func completeMultipartOpts(ctx context.Context, r *http.Request, bucket, object string) (opts ObjectOptions, err error) {
	mtimeStr := strings.TrimSpace(r.Header.Get(xhttp.MinIOSourceMTime))
	mtime := UTCNow()
	if mtimeStr != "" {
		mtime, err = time.Parse(time.RFC3339, mtimeStr)
		if err != nil {
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    fmt.Errorf("Unable to parse %s, failed with %w", xhttp.MinIOSourceMTime, err),
			}
		}
	}
	opts.MTime = mtime
	opts.UserDefined = make(map[string]string)
	return opts, nil
}
