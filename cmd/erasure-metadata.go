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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio/internal/bucket/replication"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/sync/errgroup"
)

// Object was stored with additional erasure codes due to degraded system at upload time
const minIOErasureUpgraded = "x-minio-internal-erasure-upgraded"

const erasureAlgorithm = "rs-vandermonde"

// byObjectPartNumber is a collection satisfying sort.Interface.
type byObjectPartNumber []ObjectPartInfo

func (t byObjectPartNumber) Len() int           { return len(t) }
func (t byObjectPartNumber) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t byObjectPartNumber) Less(i, j int) bool { return t[i].Number < t[j].Number }

// AddChecksumInfo adds a checksum of a part.
func (e *ErasureInfo) AddChecksumInfo(ckSumInfo ChecksumInfo) {
	for i, sum := range e.Checksums {
		if sum.PartNumber == ckSumInfo.PartNumber {
			e.Checksums[i] = ckSumInfo
			return
		}
	}
	e.Checksums = append(e.Checksums, ckSumInfo)
}

// GetChecksumInfo - get checksum of a part.
func (e ErasureInfo) GetChecksumInfo(partNumber int) (ckSum ChecksumInfo) {
	for _, sum := range e.Checksums {
		if sum.PartNumber == partNumber {
			// Return the checksum
			return sum
		}
	}
	return ChecksumInfo{}
}

// ShardFileSize - returns final erasure size from original size.
func (e ErasureInfo) ShardFileSize(totalLength int64) int64 {
	if totalLength == 0 {
		return 0
	}
	if totalLength == -1 {
		return -1
	}
	numShards := totalLength / e.BlockSize
	lastBlockSize := totalLength % e.BlockSize
	lastShardSize := ceilFrac(lastBlockSize, int64(e.DataBlocks))
	return numShards*e.ShardSize() + lastShardSize
}

// ShardSize - returns actual shared size from erasure blockSize.
func (e ErasureInfo) ShardSize() int64 {
	return ceilFrac(e.BlockSize, int64(e.DataBlocks))
}

// IsValid - tells if erasure info fields are valid.
// 判断FileInfo实例是否有效，针对一下内容进行验证
//  1. Index
//  2. 数据盘和冗余盘数量
//  3. Distribution长度
func (fi FileInfo) IsValid() bool {
	if fi.Deleted {
		// Delete marker has no data, no need to check
		// for erasure coding information
		return true
	}
	dataBlocks := fi.Erasure.DataBlocks
	parityBlocks := fi.Erasure.ParityBlocks
	// FileInfo实例的纠删信息中的Index是否正确，做以下判断：
	//  1. 大于0
	//  2. Index小于等于数据盘数量+冗余盘数量
	//  3. 纠删信息中的Distribution的长度等于数据盘数量+冗余盘数量
	correctIndexes := (fi.Erasure.Index > 0 &&
		fi.Erasure.Index <= dataBlocks+parityBlocks &&
		len(fi.Erasure.Distribution) == (dataBlocks+parityBlocks))
	// 返回以下判断式并集
	// 1. 数据盘数量大于等于冗余盘数量
	// 2. 数据盘数量不为0
	// 3. 冗余盘数量不为0
	// 4. Index信息正确与否
	return ((dataBlocks >= parityBlocks) &&
		(dataBlocks != 0) && (parityBlocks != 0) &&
		correctIndexes)
}

// ToObjectInfo - Converts metadata to object info.
// 用对象的元数据信息创建对象信息，返回ObjectInfo结构体实例
func (fi FileInfo) ToObjectInfo(bucket, object string) ObjectInfo {
	object = decodeDirObject(object)
	versionID := fi.VersionID
	if (globalBucketVersioningSys.Enabled(bucket) || globalBucketVersioningSys.Suspended(bucket)) && versionID == "" {
		versionID = nullVersionID
	}

	objInfo := ObjectInfo{
		IsDir:            HasSuffix(object, SlashSeparator),
		Bucket:           bucket,
		Name:             object,
		VersionID:        versionID,
		IsLatest:         fi.IsLatest,
		DeleteMarker:     fi.Deleted,
		Size:             fi.Size,
		ModTime:          fi.ModTime,
		Legacy:           fi.XLV1,
		ContentType:      fi.Metadata["content-type"],
		ContentEncoding:  fi.Metadata["content-encoding"],
		NumVersions:      fi.NumVersions,
		SuccessorModTime: fi.SuccessorModTime,
	}

	// Update expires
	var (
		t time.Time
		e error
	)
	if exp, ok := fi.Metadata["expires"]; ok {
		if t, e = time.Parse(http.TimeFormat, exp); e == nil {
			objInfo.Expires = t.UTC()
		}
	}
	objInfo.backendType = BackendErasure

	// Extract etag from metadata.
	objInfo.ETag = extractETag(fi.Metadata)

	// Add user tags to the object info
	tags := fi.Metadata[xhttp.AmzObjectTagging]
	if len(tags) != 0 {
		objInfo.UserTags = tags
	}

	// Add replication status to the object info
	objInfo.ReplicationStatusInternal = fi.ReplicationState.ReplicationStatusInternal
	objInfo.VersionPurgeStatusInternal = fi.ReplicationState.VersionPurgeStatusInternal
	objInfo.ReplicationStatus = fi.ReplicationState.CompositeReplicationStatus()

	objInfo.VersionPurgeStatus = fi.ReplicationState.CompositeVersionPurgeStatus()
	objInfo.TransitionedObject = TransitionedObject{
		Name:        fi.TransitionedObjName,
		VersionID:   fi.TransitionVersionID,
		Status:      fi.TransitionStatus,
		FreeVersion: fi.TierFreeVersion(),
		Tier:        fi.TransitionTier,
	}

	// etag/md5Sum has already been extracted. We need to
	// remove to avoid it from appearing as part of
	// response headers. e.g, X-Minio-* or X-Amz-*.
	// Tags have also been extracted, we remove that as well.
	objInfo.UserDefined = cleanMetadata(fi.Metadata)

	// All the parts per object.
	objInfo.Parts = fi.Parts

	// Update storage class
	if sc, ok := fi.Metadata[xhttp.AmzStorageClass]; ok {
		objInfo.StorageClass = sc
	} else {
		objInfo.StorageClass = globalMinioDefaultStorageClass
	}

	objInfo.VersionPurgeStatus = fi.VersionPurgeStatus()
	// set restore status for transitioned object
	restoreHdr, ok := fi.Metadata[xhttp.AmzRestore]
	if ok {
		if restoreStatus, err := parseRestoreObjStatus(restoreHdr); err == nil {
			objInfo.RestoreOngoing = restoreStatus.Ongoing()
			objInfo.RestoreExpires, _ = restoreStatus.Expiry()
		}
	}
	// Success.
	return objInfo
}

// TransitionInfoEquals returns true if transition related information are equal, false otherwise.
func (fi FileInfo) TransitionInfoEquals(ofi FileInfo) bool {
	switch {
	case fi.TransitionStatus != ofi.TransitionStatus,
		fi.TransitionTier != ofi.TransitionTier,
		fi.TransitionedObjName != ofi.TransitionedObjName,
		fi.TransitionVersionID != ofi.TransitionVersionID:
		return false
	}
	return true
}

// MetadataEquals returns true if FileInfos Metadata maps are equal, false otherwise.
func (fi FileInfo) MetadataEquals(ofi FileInfo) bool {
	if len(fi.Metadata) != len(ofi.Metadata) {
		return false
	}
	for k, v := range fi.Metadata {
		if ov, ok := ofi.Metadata[k]; !ok || ov != v {
			return false
		}
	}
	return true
}

// ReplicationInfoEquals returns true if server-side replication related fields are equal, false otherwise.
func (fi FileInfo) ReplicationInfoEquals(ofi FileInfo) bool {
	switch {
	case fi.MarkDeleted != ofi.MarkDeleted,
		!fi.ReplicationState.Equal(ofi.ReplicationState):
		return false
	}
	return true
}

// objectPartIndex - returns the index of matching object part number.
func objectPartIndex(parts []ObjectPartInfo, partNumber int) int {
	for i, part := range parts {
		if partNumber == part.Number {
			return i
		}
	}
	return -1
}

// AddObjectPart - add a new object part in order.
func (fi *FileInfo) AddObjectPart(partNumber int, partETag string, partSize int64, actualSize int64) {
	partInfo := ObjectPartInfo{
		Number:     partNumber,
		ETag:       partETag,
		Size:       partSize,
		ActualSize: actualSize,
	}

	// Update part info if it already exists.
	for i, part := range fi.Parts {
		if partNumber == part.Number {
			fi.Parts[i] = partInfo
			return
		}
	}

	// Proceed to include new part info.
	fi.Parts = append(fi.Parts, partInfo)

	// Parts in FileInfo should be in sorted order by part number.
	sort.Sort(byObjectPartNumber(fi.Parts))
}

// ObjectToPartOffset - translate offset of an object to offset of its individual part.
//  判断传入的offset所在的part和和所在part的偏移量
//  例如一个对象有1000byte，用较小的数据说明，看起来比较简单，实际不能将这么小的对象切分成多个part。
//  场景1：上传对象不分块，传入offset为110。此时整个只有一个part，所以partIndex为0，partOffset为110
//  场景2：上传对象分成10块，假设每块大小为100byte，传入offset为110。所以传入的offset实际在在第二个part（partIndex为1），所在part的partOffset为10
func (fi FileInfo) ObjectToPartOffset(ctx context.Context, offset int64) (partIndex int, partOffset int64, err error) {
	if offset == 0 {
		// Special case - if offset is 0, then partIndex and partOffset are always 0.
		return 0, 0, nil
	}
	partOffset = offset
	// Seek until object offset maps to a particular part offset.
	for i, part := range fi.Parts {
		partIndex = i
		// Offset is smaller than size we have reached the proper part offset.
		if partOffset < part.Size {
			return partIndex, partOffset, nil
		}
		// Continue to towards the next part.
		partOffset -= part.Size
	}
	logger.LogIf(ctx, InvalidRange{})
	// Offset beyond the size of the object return InvalidRange.
	return 0, 0, InvalidRange{}
}

// 从传入的part元数据切片中返回任意一个有效的part元数据，该方法只会返回的错误信息类型只有errErasureReadQuorum一种。
func findFileInfoInQuorum(ctx context.Context, metaArr []FileInfo, modTime time.Time, quorum int) (FileInfo, error) {
	// with less quorum return error.
	if quorum < 2 {
		return FileInfo{}, errErasureReadQuorum
	}
	metaHashes := make([]string, len(metaArr))
	h := sha256.New()
	for i, meta := range metaArr {
		if meta.IsValid() && meta.ModTime.Equal(modTime) {
			fmt.Fprintf(h, "%v", meta.XLV1)
			fmt.Fprintf(h, "%v", meta.GetDataDir())
			for _, part := range meta.Parts {
				fmt.Fprintf(h, "part.%d", part.Number)
			}
			fmt.Fprintf(h, "%v", meta.Erasure.Distribution)
			// make sure that length of Data is same
			fmt.Fprintf(h, "%v", len(meta.Data))

			// ILM transition fields
			fmt.Fprint(h, meta.TransitionStatus)
			fmt.Fprint(h, meta.TransitionTier)
			fmt.Fprint(h, meta.TransitionedObjName)
			fmt.Fprint(h, meta.TransitionVersionID)

			// Server-side replication fields
			fmt.Fprintf(h, "%v", meta.MarkDeleted)
			fmt.Fprint(h, meta.Metadata[string(meta.ReplicationState.ReplicaStatus)])
			fmt.Fprint(h, meta.Metadata[meta.ReplicationState.ReplicationTimeStamp.Format(http.TimeFormat)])
			fmt.Fprint(h, meta.Metadata[meta.ReplicationState.ReplicaTimeStamp.Format(http.TimeFormat)])
			fmt.Fprint(h, meta.Metadata[meta.ReplicationState.ReplicationStatusInternal])
			fmt.Fprint(h, meta.Metadata[meta.ReplicationState.VersionPurgeStatusInternal])

			metaHashes[i] = hex.EncodeToString(h.Sum(nil))
			h.Reset()
		}
	}

	metaHashCountMap := make(map[string]int)
	for _, hash := range metaHashes {
		if hash == "" {
			continue
		}
		metaHashCountMap[hash]++
	}

	maxHash := ""
	maxCount := 0
	for hash, count := range metaHashCountMap {
		if count > maxCount {
			maxCount = count
			maxHash = hash
		}
	}

	// 计算后有效的part元数据数量小于仲裁数量的情况，返回错误信息
	if maxCount < quorum {
		return FileInfo{}, errErasureReadQuorum
	}

	// 返回任意一个有效的part的元数据
	for i, hash := range metaHashes {
		if hash == maxHash {
			if metaArr[i].IsValid() {
				return metaArr[i], nil
			}
		}
	}

	return FileInfo{}, errErasureReadQuorum
}

func pickValidDiskTimeWithQuorum(metaArr []FileInfo, quorum int) time.Time {
	diskMTimes := listObjectDiskMtimes(metaArr)

	diskMTime, diskMaxima := commonTimeAndOccurence(diskMTimes, shardDiskTimeDelta)
	if diskMaxima >= quorum {
		return diskMTime
	}

	return timeSentinel
}

// pickValidFileInfo - picks one valid FileInfo content and returns from a
// slice of FileInfo.
func pickValidFileInfo(ctx context.Context, metaArr []FileInfo, modTime time.Time, quorum int) (FileInfo, error) {
	return findFileInfoInQuorum(ctx, metaArr, modTime, quorum)
}

// writeUniqueFileInfo - writes unique `xl.meta` content for each disk concurrently.
func writeUniqueFileInfo(ctx context.Context, disks []StorageAPI, bucket, prefix string, files []FileInfo, quorum int) ([]StorageAPI, error) {
	g := errgroup.WithNErrs(len(disks))

	// Start writing `xl.meta` to all disks in parallel.
	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			// Pick one FileInfo for a disk at index.
			fi := files[index]
			fi.Erasure.Index = index + 1
			if fi.IsValid() {
				return disks[index].WriteMetadata(ctx, bucket, prefix, fi)
			}
			return errCorruptedFormat
		}, index)
	}

	// Wait for all the routines.
	mErrs := g.Wait()

	err := reduceWriteQuorumErrs(ctx, mErrs, objectOpIgnoredErrs, quorum)
	return evalDisks(disks, mErrs), err
}

// Returns per object readQuorum and writeQuorum
// readQuorum is the min required disks to read data.
// writeQuorum is the min required disks to write data.
// 根据传入的part切片和error对象切片，返回对象的读写仲裁数量
// 如果对上传对象的最后一次操作为删除操作，读仲裁为传入part切片长度的一半，写仲裁为读仲裁数量加1
// 如果对上传对象的最后一次操作不是删除操作，根据以下步骤来计算
//  1. 通过元数据中的存储类型来获取冗余盘数量
//  2. 元数据中有记录EC降级的场合，从元数据记录中获取冗余盘数量，覆盖步骤1的结果
//  3. 从元数据记录中获取数据盘的数量，获取结果为0的场合，用传入part切片长度减去冗余盘数量
//  4. 用步骤1或2获取的数据盘数量作为读仲裁
//  5. 如果上述步骤中获取的冗余盘数量和数据盘数量相等，写仲裁为数据盘数量加1，否则写仲裁为数据盘数量
func objectQuorumFromMeta(ctx context.Context, partsMetaData []FileInfo, errs []error, defaultParityCount int) (objectReadQuorum, objectWriteQuorum int, err error) {
	// get the latest updated Metadata and a count of all the latest updated FileInfo(s)
	// 从传入的分片元数据切片中获取modtime最近的分片元数据
	latestFileInfo, err := getLatestFileInfo(ctx, partsMetaData, errs)
	if err != nil {
		return 0, 0, err
	}

	// 对指定对象最后一次操作为删除的场景，返回读写仲裁
	if latestFileInfo.Deleted {
		// For delete markers do not use 'defaultParityCount' as it is not expected to be the case.
		// Use maximum allowed read quorum instead, writeQuorum+1 is returned for compatibility sake
		// but there are no callers that shall be using this.
		readQuorum := len(partsMetaData) / 2
		return readQuorum, readQuorum + 1, nil
	}

	// 从对象的part元数据的MetaData属性中获取key为x-amz-storage-class的值，并作为传入参数，用于获取冗余盘数量
	parityBlocks := globalStorageClass.GetParityForSC(latestFileInfo.Metadata[xhttp.AmzStorageClass])
	// 获取的冗余盘数量小于等于0时，将冗余盘数量设置为传入的默认值
	if parityBlocks <= 0 {
		parityBlocks = defaultParityCount
	}

	// For erasure code upgraded objects choose the parity
	// blocks saved internally, instead of 'defaultParityCount'
	// 当元数据中保存有EC降级的信息时，获取元数据中的记录的冗余盘数量
	if _, ok := latestFileInfo.Metadata[minIOErasureUpgraded]; ok {
		if latestFileInfo.Erasure.ParityBlocks != 0 {
			parityBlocks = latestFileInfo.Erasure.ParityBlocks
		}
	}

	// 获取元数据中的记录的数据盘数量
	dataBlocks := latestFileInfo.Erasure.DataBlocks
	if dataBlocks == 0 {
		dataBlocks = len(partsMetaData) - parityBlocks
	}

	// 计算些仲裁的数量
	writeQuorum := dataBlocks
	if dataBlocks == parityBlocks {
		writeQuorum++
	}

	// Since all the valid erasure code meta updated at the same time are equivalent, pass dataBlocks
	// from latestFileInfo to get the quorum
	return dataBlocks, writeQuorum, nil
}

const (
	tierFVID     = "tier-free-versionID"
	tierFVMarker = "tier-free-marker"
)

// SetTierFreeVersionID sets free-version's versionID. This method is used by
// object layer to pass down a versionID to set for a free-version that may be
// created.
func (fi *FileInfo) SetTierFreeVersionID(versionID string) {
	if fi.Metadata == nil {
		fi.Metadata = make(map[string]string)
	}
	fi.Metadata[ReservedMetadataPrefixLower+tierFVID] = versionID
}

// TierFreeVersionID returns the free-version's version id.
func (fi *FileInfo) TierFreeVersionID() string {
	return fi.Metadata[ReservedMetadataPrefixLower+tierFVID]
}

// SetTierFreeVersion sets fi as a free-version. This method is used by
// lower layers to indicate a free-version.
func (fi *FileInfo) SetTierFreeVersion() {
	if fi.Metadata == nil {
		fi.Metadata = make(map[string]string)
	}
	fi.Metadata[ReservedMetadataPrefixLower+tierFVMarker] = ""
}

// TierFreeVersion returns true if version is a free-version.
func (fi *FileInfo) TierFreeVersion() bool {
	_, ok := fi.Metadata[ReservedMetadataPrefixLower+tierFVMarker]
	return ok
}

// VersionPurgeStatus returns overall version purge status for this object version across targets
func (fi *FileInfo) VersionPurgeStatus() VersionPurgeStatusType {
	return fi.ReplicationState.CompositeVersionPurgeStatus()
}

// DeleteMarkerReplicationStatus returns overall replication status for this delete marker version across targets
func (fi *FileInfo) DeleteMarkerReplicationStatus() replication.StatusType {
	if fi.Deleted {
		return fi.ReplicationState.CompositeReplicationStatus()
	}
	return replication.StatusType("")
}

// GetInternalReplicationState is a wrapper method to fetch internal replication state from the map m
func GetInternalReplicationState(m map[string][]byte) ReplicationState {
	m1 := make(map[string]string, len(m))
	for k, v := range m {
		m1[k] = string(v)
	}
	return getInternalReplicationState(m1)
}

// getInternalReplicationState fetches internal replication state from the map m
func getInternalReplicationState(m map[string]string) ReplicationState {
	d := ReplicationState{}
	for k, v := range m {
		switch {
		case equals(k, ReservedMetadataPrefixLower+ReplicationTimestamp):
			tm, _ := time.Parse(http.TimeFormat, v)
			d.ReplicationTimeStamp = tm
		case equals(k, ReservedMetadataPrefixLower+ReplicaTimestamp):
			tm, _ := time.Parse(http.TimeFormat, v)
			d.ReplicaTimeStamp = tm
		case equals(k, ReservedMetadataPrefixLower+ReplicaStatus):
			d.ReplicaStatus = replication.StatusType(v)
		case equals(k, ReservedMetadataPrefixLower+ReplicationStatus):
			d.ReplicationStatusInternal = v
			d.Targets = replicationStatusesMap(v)
		case equals(k, VersionPurgeStatusKey):
			d.VersionPurgeStatusInternal = v
			d.PurgeTargets = versionPurgeStatusesMap(v)
		case strings.HasPrefix(k, ReservedMetadataPrefixLower+ReplicationReset):
			arn := strings.TrimPrefix(k, fmt.Sprintf("%s-", ReservedMetadataPrefixLower+ReplicationReset))
			if d.ResetStatusesMap == nil {
				d.ResetStatusesMap = make(map[string]string, 1)
			}
			d.ResetStatusesMap[arn] = v
		}
	}
	return d
}
