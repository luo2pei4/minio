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
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"reflect"
	"sync"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/storageclass"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/sync/errgroup"
)

const (
	// Represents Erasure backend.
	formatBackendErasure = "xl"

	// formatErasureV1.Erasure.Version - version '1'.
	formatErasureVersionV1 = "1"

	// formatErasureV2.Erasure.Version - version '2'.
	formatErasureVersionV2 = "2"

	// formatErasureV3.Erasure.Version - version '3'.
	formatErasureVersionV3 = "3"

	// Distribution algorithm used, legacy
	formatErasureVersionV2DistributionAlgoV1 = "CRCMOD"

	// Distributed algorithm used, with N/2 default parity
	formatErasureVersionV3DistributionAlgoV2 = "SIPMOD"

	// Distributed algorithm used, with EC:4 default parity
	formatErasureVersionV3DistributionAlgoV3 = "SIPMOD+PARITY"
)

// Offline disk UUID represents an offline disk.
const offlineDiskUUID = "ffffffff-ffff-ffff-ffff-ffffffffffff"

// Used to detect the version of "xl" format.
type formatErasureVersionDetect struct {
	Erasure struct {
		Version string `json:"version"`
	} `json:"xl"`
}

// Represents the V1 backend disk structure version
// under `.minio.sys` and actual data namespace.
// formatErasureV1 - structure holds format config version '1'.
type formatErasureV1 struct {
	formatMetaV1
	Erasure struct {
		Version string `json:"version"` // Version of 'xl' format.
		Disk    string `json:"disk"`    // Disk field carries assigned disk uuid.
		// JBOD field carries the input disk order generated the first
		// time when fresh disks were supplied.
		JBOD []string `json:"jbod"`
	} `json:"xl"` // Erasure field holds xl format.
}

// Represents the V2 backend disk structure version
// under `.minio.sys` and actual data namespace.
// formatErasureV2 - structure holds format config version '2'.
// The V2 format to support "large bucket" support where a bucket
// can span multiple erasure sets.
type formatErasureV2 struct {
	formatMetaV1
	Erasure struct {
		Version string `json:"version"` // Version of 'xl' format.
		This    string `json:"this"`    // This field carries assigned disk uuid.
		// Sets field carries the input disk order generated the first
		// time when fresh disks were supplied, it is a two dimensional
		// array second dimension represents list of disks used per set.
		Sets [][]string `json:"sets"`
		// Distribution algorithm represents the hashing algorithm
		// to pick the right set index for an object.
		DistributionAlgo string `json:"distributionAlgo"`
	} `json:"xl"`
}

// formatErasureV3 struct is same as formatErasureV2 struct except that formatErasureV3.Erasure.Version is "3" indicating
// the simplified multipart backend which is a flat hierarchy now.
// In .minio.sys/multipart we have:
// sha256(bucket/object)/uploadID/[xl.meta, part.1, part.2 ....]
type formatErasureV3 struct {
	formatMetaV1
	Erasure struct {
		Version string `json:"version"` // Version of 'xl' format.
		This    string `json:"this"`    // This field carries assigned disk uuid.
		// Sets field carries the input disk order generated the first
		// time when fresh disks were supplied, it is a two dimensional
		// array second dimension represents list of disks used per set.
		Sets [][]string `json:"sets"`
		// Distribution algorithm represents the hashing algorithm
		// to pick the right set index for an object.
		DistributionAlgo string `json:"distributionAlgo"`
	} `json:"xl"`
}

func (f *formatErasureV3) Drives() (drives int) {
	for _, set := range f.Erasure.Sets {
		drives += len(set)
	}
	return drives
}

func (f *formatErasureV3) Clone() *formatErasureV3 {
	b, err := json.Marshal(f)
	if err != nil {
		panic(err)
	}
	var dst formatErasureV3
	if err = json.Unmarshal(b, &dst); err != nil {
		panic(err)
	}
	return &dst
}

// Returns formatErasure.Erasure.Version
// 创建并返回formatErasureV3结构体的实例，
// formatErasureV3结构体实例的ID是随机生成的UUID
// 实例中还包含了所有纠删集下所有磁盘的UUID，磁盘的UUID也是随机生成的
func newFormatErasureV3(numSets int, setLen int) *formatErasureV3 {
	format := &formatErasureV3{}
	format.Version = formatMetaVersionV1
	format.Format = formatBackendErasure
	format.ID = mustGetUUID()
	format.Erasure.Version = formatErasureVersionV3
	format.Erasure.DistributionAlgo = formatErasureVersionV3DistributionAlgoV3
	format.Erasure.Sets = make([][]string, numSets)

	// 为每个纠删集中的每一块磁盘生成一个UUID
	for i := 0; i < numSets; i++ {
		format.Erasure.Sets[i] = make([]string, setLen)
		for j := 0; j < setLen; j++ {
			format.Erasure.Sets[i][j] = mustGetUUID()
		}
	}
	return format
}

// Returns format Erasure version after reading `format.json`, returns
// successfully the version only if the backend is Erasure.
func formatGetBackendErasureVersion(b []byte) (string, error) {
	meta := &formatMetaV1{}
	if err := json.Unmarshal(b, meta); err != nil {
		return "", err
	}
	if meta.Version != formatMetaVersionV1 {
		return "", fmt.Errorf(`format.Version expected: %s, got: %s`, formatMetaVersionV1, meta.Version)
	}
	if meta.Format != formatBackendErasure {
		return "", fmt.Errorf(`found backend type %s, expected %s`, meta.Format, formatBackendErasure)
	}
	// Erasure backend found, proceed to detect version.
	format := &formatErasureVersionDetect{}
	if err := json.Unmarshal(b, format); err != nil {
		return "", err
	}
	return format.Erasure.Version, nil
}

// Migrates all previous versions to latest version of `format.json`,
// this code calls migration in sequence, such as V1 is migrated to V2
// first before it V2 migrates to V3.n
func formatErasureMigrate(export string) ([]byte, fs.FileInfo, error) {
	formatPath := pathJoin(export, minioMetaBucket, formatConfigFile)
	formatData, formatFi, err := xioutil.ReadFileWithFileInfo(formatPath)
	if err != nil {
		return nil, nil, err
	}

	version, err := formatGetBackendErasureVersion(formatData)
	if err != nil {
		return nil, nil, fmt.Errorf("Disk %s: %w", export, err)
	}

	migrate := func(formatPath string, formatData []byte) ([]byte, fs.FileInfo, error) {
		if err = ioutil.WriteFile(formatPath, formatData, 0o666); err != nil {
			return nil, nil, err
		}
		formatFi, err := Lstat(formatPath)
		if err != nil {
			return nil, nil, err
		}
		return formatData, formatFi, nil
	}

	switch version {
	case formatErasureVersionV1:
		formatData, err = formatErasureMigrateV1ToV2(formatData, version)
		if err != nil {
			return nil, nil, fmt.Errorf("Disk %s: %w", export, err)
		}
		// Migrate successful v1 => v2, proceed to v2 => v3
		version = formatErasureVersionV2
		fallthrough
	case formatErasureVersionV2:
		formatData, err = formatErasureMigrateV2ToV3(formatData, export, version)
		if err != nil {
			return nil, nil, fmt.Errorf("Disk %s: %w", export, err)
		}
		// Migrate successful v2 => v3, v3 is latest
		// version = formatXLVersionV3
		return migrate(formatPath, formatData)
	case formatErasureVersionV3:
		// v3 is the latest version, return.
		return formatData, formatFi, nil
	}
	return nil, nil, fmt.Errorf(`Disk %s: unknown format version %s`, export, version)
}

// Migrates version V1 of format.json to version V2 of format.json,
// migration fails upon any error.
func formatErasureMigrateV1ToV2(data []byte, version string) ([]byte, error) {
	if version != formatErasureVersionV1 {
		return nil, fmt.Errorf(`format version expected %s, found %s`, formatErasureVersionV1, version)
	}

	formatV1 := &formatErasureV1{}
	if err := json.Unmarshal(data, formatV1); err != nil {
		return nil, err
	}

	formatV2 := &formatErasureV2{}
	formatV2.Version = formatMetaVersionV1
	formatV2.Format = formatBackendErasure
	formatV2.Erasure.Version = formatErasureVersionV2
	formatV2.Erasure.DistributionAlgo = formatErasureVersionV2DistributionAlgoV1
	formatV2.Erasure.This = formatV1.Erasure.Disk
	formatV2.Erasure.Sets = make([][]string, 1)
	formatV2.Erasure.Sets[0] = make([]string, len(formatV1.Erasure.JBOD))
	copy(formatV2.Erasure.Sets[0], formatV1.Erasure.JBOD)

	return json.Marshal(formatV2)
}

// Migrates V2 for format.json to V3 (Flat hierarchy for multipart)
func formatErasureMigrateV2ToV3(data []byte, export, version string) ([]byte, error) {
	if version != formatErasureVersionV2 {
		return nil, fmt.Errorf(`format version expected %s, found %s`, formatErasureVersionV2, version)
	}

	formatV2 := &formatErasureV2{}
	if err := json.Unmarshal(data, formatV2); err != nil {
		return nil, err
	}

	tmpOld := pathJoin(export, minioMetaTmpDeletedBucket, mustGetUUID())
	if err := renameAll(pathJoin(export, minioMetaMultipartBucket),
		tmpOld); err != nil && err != errFileNotFound {
		logger.LogIf(GlobalContext, fmt.Errorf("unable to rename (%s -> %s) %w, drive may be faulty please investigate",
			pathJoin(export, minioMetaMultipartBucket),
			tmpOld,
			osErrToFileErr(err)))
	}

	// format-V2 struct is exactly same as format-V1 except that version is "3"
	// which indicates the simplified multipart backend.
	formatV3 := formatErasureV3{}
	formatV3.Version = formatV2.Version
	formatV3.Format = formatV2.Format
	formatV3.Erasure = formatV2.Erasure
	formatV3.Erasure.Version = formatErasureVersionV3

	return json.Marshal(formatV3)
}

// countErrs - count a specific error.
func countErrs(errs []error, err error) int {
	i := 0
	for _, err1 := range errs {
		if err1 == err {
			i++
		}
	}
	return i
}

// Does all errors indicate we need to initialize all disks?.
func shouldInitErasureDisks(errs []error) bool {
	return countErrs(errs, errUnformattedDisk) == len(errs)
}

// Check if unformatted disks are equal to write quorum.
func quorumUnformattedDisks(errs []error) bool {
	return countErrs(errs, errUnformattedDisk) >= (len(errs)/2)+1
}

// loadFormatErasureAll - load all format config from all input disks in parallel.
func loadFormatErasureAll(storageDisks []StorageAPI, heal bool) ([]*formatErasureV3, []error) {
	// Initialize list of errors.
	g := errgroup.WithNErrs(len(storageDisks))

	// Initialize format configs.
	formats := make([]*formatErasureV3, len(storageDisks))

	// Load format from each disk in parallel
	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if storageDisks[index] == nil {
				return errDiskNotFound
			}
			format, err := loadFormatErasure(storageDisks[index])
			if err != nil {
				return err
			}
			formats[index] = format
			if !heal {
				// If no healing required, make the disks valid and
				// online.
				storageDisks[index].SetDiskID(format.Erasure.This)
			}
			return nil
		}, index)
	}

	// Return all formats and errors if any.
	return formats, g.Wait()
}

func saveFormatErasure(disk StorageAPI, format *formatErasureV3, heal bool) error {
	if disk == nil || format == nil {
		return errDiskNotFound
	}

	diskID := format.Erasure.This

	if err := makeFormatErasureMetaVolumes(disk); err != nil {
		return err
	}

	// Marshal and write to disk.
	formatBytes, err := json.Marshal(format)
	if err != nil {
		return err
	}

	tmpFormat := mustGetUUID()

	// Purge any existing temporary file, okay to ignore errors here.
	defer disk.Delete(context.TODO(), minioMetaBucket, tmpFormat, false)

	// write to unique file.
	if err = disk.WriteAll(context.TODO(), minioMetaBucket, tmpFormat, formatBytes); err != nil {
		return err
	}

	// Rename file `uuid.json` --> `format.json`.
	if err = disk.RenameFile(context.TODO(), minioMetaBucket, tmpFormat, minioMetaBucket, formatConfigFile); err != nil {
		return err
	}

	disk.SetDiskID(diskID)
	// 需要heal时，调用save方法在磁盘上创建.healing.bin文件
	if heal {
		ctx := context.Background()
		ht := newHealingTracker(disk)
		return ht.save(ctx)
	}
	return nil
}

// loadFormatErasure - loads format.json from disk.
// 读取指定磁盘下的format.json文件，解码为formatErasureV3结构体的实例并返回
func loadFormatErasure(disk StorageAPI) (format *formatErasureV3, err error) {
	buf, err := disk.ReadAll(context.TODO(), minioMetaBucket, formatConfigFile)
	if err != nil {
		// 'file not found' and 'volume not found' as
		// same. 'volume not found' usually means its a fresh disk.
		if err == errFileNotFound || err == errVolumeNotFound {
			return nil, errUnformattedDisk
		}
		return nil, err
	}

	// Try to decode format json into formatConfigV1 struct.
	format = &formatErasureV3{}
	if err = json.Unmarshal(buf, format); err != nil {
		return nil, err
	}

	// Success.
	return format, nil
}

// Valid formatErasure basic versions.
func checkFormatErasureValue(formatErasure *formatErasureV3, disk StorageAPI) error {
	// Validate format version and format type.
	if formatErasure.Version != formatMetaVersionV1 {
		return fmt.Errorf("Unsupported version of backend format [%s] found on %s", formatErasure.Version, disk)
	}
	if formatErasure.Format != formatBackendErasure {
		return fmt.Errorf("Unsupported backend format [%s] found on %s", formatErasure.Format, disk)
	}
	if formatErasure.Erasure.Version != formatErasureVersionV3 {
		return fmt.Errorf("Unsupported Erasure backend format found [%s] on %s", formatErasure.Erasure.Version, disk)
	}
	return nil
}

// Check all format values.
func checkFormatErasureValues(formats []*formatErasureV3, disks []StorageAPI, setDriveCount int) error {
	for i, formatErasure := range formats {
		if formatErasure == nil {
			continue
		}
		if err := checkFormatErasureValue(formatErasure, disks[i]); err != nil {
			return err
		}
		if len(formats) != len(formatErasure.Erasure.Sets)*len(formatErasure.Erasure.Sets[0]) {
			return fmt.Errorf("%s disk is already being used in another erasure deployment. (Number of disks specified: %d but the number of disks found in the %s disk's format.json: %d)",
				disks[i], len(formats), humanize.Ordinal(i+1), len(formatErasure.Erasure.Sets)*len(formatErasure.Erasure.Sets[0]))
		}
		// Only if custom erasure drive count is set, verify if the
		// set_drive_count was manually set - we need to honor what is
		// present on the drives.
		if globalCustomErasureDriveCount && len(formatErasure.Erasure.Sets[0]) != setDriveCount {
			return fmt.Errorf("%s disk is already formatted with %d drives per erasure set. This cannot be changed to %d, please revert your MINIO_ERASURE_SET_DRIVE_COUNT setting", disks[i], len(formatErasure.Erasure.Sets[0]), setDriveCount)
		}
	}
	return nil
}

// Get Deployment ID for the Erasure sets from format.json.
// This need not be in quorum. Even if one of the format.json
// file has this value, we assume it is valid.
// If more than one format.json's have different id, it is considered a corrupt
// backend format.
func formatErasureGetDeploymentID(refFormat *formatErasureV3, formats []*formatErasureV3) (string, error) {
	var deploymentID string
	for _, format := range formats {
		if format == nil || format.ID == "" {
			continue
		}
		if reflect.DeepEqual(format.Erasure.Sets, refFormat.Erasure.Sets) {
			// Found an ID in one of the format.json file
			// Set deploymentID for the first time.
			if deploymentID == "" {
				deploymentID = format.ID
			} else if deploymentID != format.ID {
				// DeploymentID found earlier doesn't match with the
				// current format.json's ID.
				return "", fmt.Errorf("Deployment IDs do not match expected %s, got %s: %w",
					deploymentID, format.ID, errCorruptedFormat)
			}
		}
	}
	return deploymentID, nil
}

// formatErasureFixDeploymentID - Add deployment id if it is not present.
func formatErasureFixDeploymentID(endpoints Endpoints, storageDisks []StorageAPI, refFormat *formatErasureV3) (err error) {
	// Attempt to load all `format.json` from all disks.
	formats, _ := loadFormatErasureAll(storageDisks, false)
	for index := range formats {
		// If the Erasure sets do not match, set those formats to nil,
		// We do not have to update the ID on those format.json file.
		if formats[index] != nil && !reflect.DeepEqual(formats[index].Erasure.Sets, refFormat.Erasure.Sets) {
			formats[index] = nil
		}
	}

	refFormat.ID, err = formatErasureGetDeploymentID(refFormat, formats)
	if err != nil {
		return err
	}

	// If ID is set, then some other node got the lock
	// before this node could and generated an ID
	// for the deployment. No need to generate one.
	if refFormat.ID != "" {
		return nil
	}

	// ID is generated for the first time,
	// We set the ID in all the formats and update.
	refFormat.ID = mustGetUUID()
	for _, format := range formats {
		if format != nil {
			format.ID = refFormat.ID
		}
	}
	// Deployment ID needs to be set on all the disks.
	// Save `format.json` across all disks.
	return saveFormatErasureAll(GlobalContext, storageDisks, formats)
}

// Update only the valid local disks which have not been updated before.
func formatErasureFixLocalDeploymentID(endpoints Endpoints, storageDisks []StorageAPI, refFormat *formatErasureV3) error {
	// If this server was down when the deploymentID was updated
	// then we make sure that we update the local disks with the deploymentID.

	// Initialize errs to collect errors inside go-routine.
	g := errgroup.WithNErrs(len(storageDisks))

	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if endpoints[index].IsLocal && storageDisks[index] != nil && storageDisks[index].IsOnline() {
				format, err := loadFormatErasure(storageDisks[index])
				if err != nil {
					// Disk can be offline etc.
					// ignore the errors seen here.
					return nil
				}
				if format.ID != "" {
					return nil
				}
				if !reflect.DeepEqual(format.Erasure.Sets, refFormat.Erasure.Sets) {
					return nil
				}
				format.ID = refFormat.ID
				// Heal the drive if we fixed its deployment ID.
				if err := saveFormatErasure(storageDisks[index], format, true); err != nil {
					logger.LogIf(GlobalContext, err)
					return fmt.Errorf("Unable to save format.json, %w", err)
				}
			}
			return nil
		}, index)
	}
	for _, err := range g.Wait() {
		if err != nil {
			return err
		}
	}
	return nil
}

// Get backend Erasure format in quorum `format.json`.
func getFormatErasureInQuorum(formats []*formatErasureV3) (*formatErasureV3, error) {
	formatCountMap := make(map[int]int, len(formats))
	for _, format := range formats {
		if format == nil {
			continue
		}
		formatCountMap[format.Drives()]++
	}

	maxDrives := 0
	maxCount := 0
	for drives, count := range formatCountMap {
		if count > maxCount {
			maxCount = count
			maxDrives = drives
		}
	}

	if maxDrives == 0 {
		return nil, errErasureReadQuorum
	}

	if maxCount < len(formats)/2 {
		return nil, errErasureReadQuorum
	}

	for i, format := range formats {
		if format == nil {
			continue
		}
		if format.Drives() == maxDrives {
			format := formats[i].Clone()
			format.Erasure.This = ""
			return format, nil
		}
	}

	return nil, errErasureReadQuorum
}

func formatErasureV3Check(reference *formatErasureV3, format *formatErasureV3) error {
	tmpFormat := format.Clone()
	this := tmpFormat.Erasure.This
	tmpFormat.Erasure.This = ""
	if len(reference.Erasure.Sets) != len(format.Erasure.Sets) {
		return fmt.Errorf("Expected number of sets %d, got %d", len(reference.Erasure.Sets), len(format.Erasure.Sets))
	}

	// Make sure that the sets match.
	for i := range reference.Erasure.Sets {
		if len(reference.Erasure.Sets[i]) != len(format.Erasure.Sets[i]) {
			return fmt.Errorf("Each set should be of same size, expected %d got %d",
				len(reference.Erasure.Sets[i]), len(format.Erasure.Sets[i]))
		}
		for j := range reference.Erasure.Sets[i] {
			if reference.Erasure.Sets[i][j] != format.Erasure.Sets[i][j] {
				return fmt.Errorf("UUID on positions %d:%d do not match with, expected %s got %s: (%w)",
					i, j, reference.Erasure.Sets[i][j], format.Erasure.Sets[i][j], errInconsistentDisk)
			}
		}
	}

	// Make sure that the diskID is found in the set.
	for i := 0; i < len(tmpFormat.Erasure.Sets); i++ {
		for j := 0; j < len(tmpFormat.Erasure.Sets[i]); j++ {
			if this == tmpFormat.Erasure.Sets[i][j] {
				return nil
			}
		}
	}
	return fmt.Errorf("Disk ID %s not found in any disk sets %s", this, format.Erasure.Sets)
}

// saveFormatErasureAll - populates `format.json` on disks in its order.
func saveFormatErasureAll(ctx context.Context, storageDisks []StorageAPI, formats []*formatErasureV3) error {
	g := errgroup.WithNErrs(len(storageDisks))

	// Write `format.json` to all disks.
	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if formats[index] == nil {
				return errDiskNotFound
			}
			return saveFormatErasure(storageDisks[index], formats[index], false)
		}, index)
	}

	writeQuorum := getWriteQuorum(len(storageDisks))
	// Wait for the routines to finish.
	return reduceWriteQuorumErrs(ctx, g.Wait(), nil, writeQuorum)
}

// relinquishes the underlying connection for all storage disks.
func closeStorageDisks(storageDisks []StorageAPI) {
	var wg sync.WaitGroup
	for _, disk := range storageDisks {
		if disk == nil {
			continue
		}
		wg.Add(1)
		go func(disk StorageAPI) {
			defer wg.Done()
			disk.Close()
		}(disk)
	}
	wg.Wait()
}

func initStorageDisksWithErrorsWithoutHealthCheck(endpoints Endpoints) ([]StorageAPI, []error) {
	// Bootstrap disks.
	storageDisks := make([]StorageAPI, len(endpoints))
	g := errgroup.WithNErrs(len(endpoints))
	for index := range endpoints {
		index := index
		g.Go(func() (err error) {
			storageDisks[index], err = newStorageAPIWithoutHealthCheck(endpoints[index])
			return err
		}, index)
	}
	return storageDisks, g.Wait()
}

// Initialize storage disks for each endpoint.
// Errors are returned for each endpoint with matching index.
func initStorageDisksWithErrors(endpoints Endpoints) ([]StorageAPI, []error) {
	// Bootstrap disks.
	// 根据单个pool中所有的磁盘的数量创建一个StorageAPI的切片
	storageDisks := make([]StorageAPI, len(endpoints))
	g := errgroup.WithNErrs(len(endpoints))
	for index := range endpoints {
		index := index
		g.Go(func() (err error) {
			storageDisks[index], err = newStorageAPI(endpoints[index])
			return err
		}, index)
	}
	return storageDisks, g.Wait()
}

// formatErasureV3ThisEmpty - find out if '.This' field is empty
// in any of the input `formats`, if yes return true.
func formatErasureV3ThisEmpty(formats []*formatErasureV3) bool {
	for _, format := range formats {
		if format == nil {
			continue
		}
		// NOTE: This code is specifically needed when migrating version
		// V1 to V2 to V3, in a scenario such as this we only need to handle
		// single sets since we never used to support multiple sets in releases
		// with V1 format version.
		if len(format.Erasure.Sets) > 1 {
			continue
		}
		if format.Erasure.This == "" {
			return true
		}
	}
	return false
}

// fixFormatErasureV3 - fix format Erasure configuration on all disks.
func fixFormatErasureV3(storageDisks []StorageAPI, endpoints Endpoints, formats []*formatErasureV3) error {
	g := errgroup.WithNErrs(len(formats))
	for i := range formats {
		i := i
		g.Go(func() error {
			if formats[i] == nil || !endpoints[i].IsLocal {
				return nil
			}
			// NOTE: This code is specifically needed when migrating version
			// V1 to V2 to V3, in a scenario such as this we only need to handle
			// single sets since we never used to support multiple sets in releases
			// with V1 format version.
			if len(formats[i].Erasure.Sets) > 1 {
				return nil
			}
			if formats[i].Erasure.This == "" {
				formats[i].Erasure.This = formats[i].Erasure.Sets[0][i]
				// Heal the drive if drive has .This empty.
				if err := saveFormatErasure(storageDisks[i], formats[i], true); err != nil {
					return err
				}
			}
			return nil
		}, i)
	}
	for _, err := range g.Wait() {
		if err != nil {
			return err
		}
	}
	return nil
}

// initFormatErasure - save Erasure format configuration on all disks.
func initFormatErasure(ctx context.Context, storageDisks []StorageAPI, setCount, setDriveCount int, deploymentID, distributionAlgo string, sErrs []error) (*formatErasureV3, error) {
	// // 创建并返回formatErasureV3结构体的实例
	format := newFormatErasureV3(setCount, setDriveCount)
	formats := make([]*formatErasureV3, len(storageDisks))
	wantAtMost := ecDrivesNoConfig(setDriveCount)

	// i为纠删集的遍历编号
	for i := 0; i < setCount; i++ {
		hostCount := make(map[string]int, setDriveCount)
		// j每个纠删集中磁盘的遍历编号
		for j := 0; j < setDriveCount; j++ {
			disk := storageDisks[i*setDriveCount+j]
			newFormat := format.Clone()
			newFormat.Erasure.This = format.Erasure.Sets[i][j]
			if distributionAlgo != "" {
				newFormat.Erasure.DistributionAlgo = distributionAlgo
			}
			if deploymentID != "" {
				newFormat.ID = deploymentID
			}
			hostCount[disk.Hostname()]++
			formats[i*setDriveCount+j] = newFormat
		}
		if len(hostCount) > 0 {
			var once sync.Once
			for host, count := range hostCount {
				if count > wantAtMost {
					if host == "" {
						host = "local"
					}
					once.Do(func() {
						if len(hostCount) == 1 {
							return
						}
						logger.Info(" * Set %v:", i+1)
						for j := 0; j < setDriveCount; j++ {
							disk := storageDisks[i*setDriveCount+j]
							logger.Info("   - Drive: %s", disk.String())
						}
					})
					logger.Info(color.Yellow("WARNING:")+" Host %v has more than %v drives of set. "+
						"A host failure will result in data becoming unavailable.", host, wantAtMost)
				}
			}
		}
	}

	// Mark all root disks down
	markRootDisksAsDown(storageDisks, sErrs)

	// Save formats `format.json` across all disks.
	// 向所有磁盘中写入format.json文件
	if err := saveFormatErasureAll(ctx, storageDisks, formats); err != nil {
		return nil, err
	}

	// 正常情况下，返回的formatErasureV3结构体实例与函数开始调用newFormatErasureV3函数生成的实例一致
	// format.Erasure.This为空字符串
	return getFormatErasureInQuorum(formats)
}

func getDefaultParityBlocks(drive int) int {
	switch drive {
	case 3, 2:
		return 1
	case 4, 5:
		return 2
	case 6, 7:
		return 3
	default:
		return 4
	}
}

// ecDrivesNoConfig returns the erasure coded drives in a set if no config has been set.
// It will attempt to read it from env variable and fall back to drives/2.
func ecDrivesNoConfig(setDriveCount int) int {
	// 返回storage class的配置信息
	sc, _ := storageclass.LookupConfig(config.KVS{}, setDriveCount)
	// 从storage class的配置信息中获取standard storage class的校验盘的数量
	ecDrives := sc.GetParityForSC(storageclass.STANDARD)
	// 校验盘数量等于0的情况，一般是没有在环境变量中设置EC配额的默认情况
	if ecDrives <= 0 {
		ecDrives = getDefaultParityBlocks(setDriveCount)
	}
	return ecDrives
}

// Make Erasure backend meta volumes.
func makeFormatErasureMetaVolumes(disk StorageAPI) error {
	if disk == nil {
		return errDiskNotFound
	}
	volumes := []string{
		minioMetaTmpDeletedBucket, // creates .minio.sys/tmp as well as .minio.sys/tmp/.trash
		minioMetaMultipartBucket,  // creates .minio.sys/multipart
		dataUsageBucket,           // creates .minio.sys/buckets
		minioConfigBucket,         // creates .minio.sys/config
	}
	// Attempt to create MinIO internal buckets.
	return disk.MakeVolBulk(context.TODO(), volumes...)
}

// Initialize a new set of set formats which will be written to all disks.
func newHealFormatSets(refFormat *formatErasureV3, setCount, setDriveCount int, formats []*formatErasureV3, errs []error) [][]*formatErasureV3 {
	newFormats := make([][]*formatErasureV3, setCount)
	for i := range refFormat.Erasure.Sets {
		newFormats[i] = make([]*formatErasureV3, setDriveCount)
	}
	for i := range refFormat.Erasure.Sets {
		for j := range refFormat.Erasure.Sets[i] {
			if errors.Is(errs[i*setDriveCount+j], errUnformattedDisk) {
				newFormats[i][j] = &formatErasureV3{}
				newFormats[i][j].ID = refFormat.ID
				newFormats[i][j].Format = refFormat.Format
				newFormats[i][j].Version = refFormat.Version
				newFormats[i][j].Erasure.This = refFormat.Erasure.Sets[i][j]
				newFormats[i][j].Erasure.Sets = refFormat.Erasure.Sets
				newFormats[i][j].Erasure.Version = refFormat.Erasure.Version
				newFormats[i][j].Erasure.DistributionAlgo = refFormat.Erasure.DistributionAlgo
			}
		}
	}
	return newFormats
}
