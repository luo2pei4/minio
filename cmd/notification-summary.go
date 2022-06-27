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
	"github.com/minio/madmin-go"
)

// GetTotalCapacity gets the total capacity in the cluster.
// 获取集群中所有数据盘的总容量
func GetTotalCapacity(diskInfo []madmin.Disk) (capacity uint64) {
	for _, disk := range diskInfo {
		// 过滤掉磁盘异常的情况，包括拔盘。
		if disk.State != "ok" {
			continue
		}
		capacity += disk.TotalSpace
	}
	return
}

// GetTotalUsableCapacity gets the total usable capacity in the cluster.
// This value is not an accurate representation of total usable in a multi-tenant deployment.
// 获取集群总的可用容量
func GetTotalUsableCapacity(diskInfo []madmin.Disk, s StorageInfo) (capacity float64) {
	raw := GetTotalCapacity(diskInfo)
	var approxDataBlocks float64
	var actualDisks float64
	for _, scData := range s.Backend.StandardSCData {
		approxDataBlocks += float64(scData)
		actualDisks += float64(scData + s.Backend.StandardSCParity)
	}
	ratio := approxDataBlocks / actualDisks
	return float64(raw) * ratio
}

// GetTotalCapacityFree gets the total capacity free in the cluster.
func GetTotalCapacityFree(diskInfo []madmin.Disk) (capacity uint64) {
	for _, d := range diskInfo {
		if d.State != "ok" {
			continue
		}
		capacity += d.AvailableSpace
	}
	return
}

// GetTotalUsableCapacityFree gets the total usable capacity free in the cluster.
// This value is not an accurate representation of total free in a multi-tenant deployment.
func GetTotalUsableCapacityFree(diskInfo []madmin.Disk, s StorageInfo) (capacity float64) {
	raw := GetTotalCapacityFree(diskInfo)
	var approxDataBlocks float64
	var actualDisks float64
	for _, scData := range s.Backend.StandardSCData {
		approxDataBlocks += float64(scData)
		actualDisks += float64(scData + s.Backend.StandardSCParity)
	}
	ratio := approxDataBlocks / actualDisks
	return float64(raw) * ratio
}
