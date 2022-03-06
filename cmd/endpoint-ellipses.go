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
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/ellipses"
	"github.com/minio/pkg/env"
)

// This file implements and supports ellipses pattern for
// `minio server` command line arguments.

// Endpoint set represents parsed ellipses values, also provides
// methods to get the sets of endpoints.
type endpointSet struct {
	argPatterns []ellipses.ArgPattern
	endpoints   []string   // Endpoints saved from previous GetEndpoints().
	setIndexes  [][]uint64 // All the sets.
}

// Supported set sizes this is used to find the optimal
// single set size.
var setSizes = []uint64{4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

// getDivisibleSize - returns a greatest common divisor of
// all the ellipses sizes.
// 获取server pool盘数的最大公约数。如果只有一个server pool的话，最大公约数就是这个server pool的总盘数。
func getDivisibleSize(totalSizes []uint64) (result uint64) {
	gcd := func(x, y uint64) uint64 {
		for y != 0 {
			x, y = y, x%y
		}
		return x
	}
	result = totalSizes[0]
	for i := 1; i < len(totalSizes); i++ {
		result = gcd(result, totalSizes[i])
	}
	return result
}

// isValidSetSize - checks whether given count is a valid set size for erasure coding.
var isValidSetSize = func(count uint64) bool {
	return (count >= setSizes[0] && count <= setSizes[len(setSizes)-1])
}

// 通过最大公约数和可用的纠删集磁盘数切片，选出合适的纠删集磁盘数。
// 核心思路就尽量减少纠删集，纠删集的磁盘数尽量大。
func commonSetDriveCount(divisibleSize uint64, setCounts []uint64) (setSize uint64) {
	// prefers setCounts to be sorted for optimal behavior.
	if divisibleSize < setCounts[len(setCounts)-1] {
		return divisibleSize
	}

	// Figure out largest value of total_drives_in_erasure_set which results
	// in least number of total_drives/total_drives_erasure_set ratio.
	prevD := divisibleSize / setCounts[0]
	for _, cnt := range setCounts {
		if divisibleSize%cnt == 0 {
			d := divisibleSize / cnt
			if d <= prevD {
				prevD = d
				setSize = cnt
			}
		}
	}
	return setSize
}

// possibleSetCountsWithSymmetry returns symmetrical setCounts based on the
// input argument patterns, the symmetry calculation is to ensure that
// we also use uniform number of drives common across all ellipses patterns.
func possibleSetCountsWithSymmetry(setCounts []uint64, argPatterns []ellipses.ArgPattern) []uint64 {
	newSetCounts := make(map[uint64]struct{})

	// 遍历可用的set数，
	for _, ss := range setCounts {
		var symmetry bool
		// 遍历模式切片，判断可用set数与pattern中的seq的长度取余是否为0
		for _, argPattern := range argPatterns {
			for _, p := range argPattern {
				if uint64(len(p.Seq)) > ss {
					symmetry = uint64(len(p.Seq))%ss == 0
				} else {
					symmetry = ss%uint64(len(p.Seq)) == 0
				}
			}
		}
		// With no arg patterns, it is expected that user knows
		// the right symmetry, so either ellipses patterns are
		// provided (recommended) or no ellipses patterns.
		// 可用set数与pattern中的seq的长度取余为0的话，说明该可用的set数是对称的
		if _, ok := newSetCounts[ss]; !ok && (symmetry || argPatterns == nil) {
			newSetCounts[ss] = struct{}{}
		}
	}

	setCounts = []uint64{}
	for setCount := range newSetCounts {
		setCounts = append(setCounts, setCount)
	}

	// Not necessarily needed but it ensures to the readers
	// eyes that we prefer a sorted setCount slice for the
	// subsequent function to figure out the right common
	// divisor, it avoids loops.
	// 升序排列
	sort.Slice(setCounts, func(i, j int) bool {
		return setCounts[i] < setCounts[j]
	})

	return setCounts
}

// getSetIndexes returns list of indexes which provides the set size
// on each index, this function also determines the final set size
// The final set size has the affinity towards choosing smaller
// indexes (total sets)
func getSetIndexes(args []string, totalSizes []uint64, customSetDriveCount uint64, argPatterns []ellipses.ArgPattern) (setIndexes [][]uint64, err error) {
	if len(totalSizes) == 0 || len(args) == 0 {
		return nil, errInvalidArgument
	}

	setIndexes = make([][]uint64, len(totalSizes))
	for _, totalSize := range totalSizes {
		// Check if totalSize has minimum range upto setSize
		// 判断总盘数是否小于4块盘，或者总盘数是否小于用户指定的纠删集盘数
		if totalSize < setSizes[0] || totalSize < customSetDriveCount {
			msg := fmt.Sprintf("Incorrect number of endpoints provided %s", args)
			return nil, config.ErrInvalidNumberOfErasureEndpoints(nil).Msg(msg)
		}
	}

	// 获取所有server pool盘数的最大公约数
	commonSize := getDivisibleSize(totalSizes)
	// 用最大公约数算出可能的纠删集磁盘数，并返回切片
	// 例如，最大公约数是32的场合，可用的set数是4、8、16
	possibleSetCounts := func(setSize uint64) (ss []uint64) {
		for _, s := range setSizes {
			if setSize%s == 0 {
				ss = append(ss, s)
			}
		}
		return ss
	}

	setCounts := possibleSetCounts(commonSize)
	if len(setCounts) == 0 {
		msg := fmt.Sprintf("Incorrect number of endpoints provided %s, number of disks %d is not divisible by any supported erasure set sizes %d", args, commonSize, setSizes)
		return nil, config.ErrInvalidNumberOfErasureEndpoints(nil).Msg(msg)
	}

	var setSize uint64
	// Custom set drive count allows to override automatic distribution.
	// only meant if you want to further optimize drive distribution.
	if customSetDriveCount > 0 {
		msg := fmt.Sprintf("Invalid set drive count. Acceptable values for %d number drives are %d", commonSize, setCounts)
		var found bool
		for _, ss := range setCounts {
			if ss == customSetDriveCount {
				found = true
			}
		}
		if !found {
			return nil, config.ErrInvalidErasureSetSize(nil).Msg(msg)
		}

		// No automatic symmetry calculation expected, user is on their own
		setSize = customSetDriveCount
		globalCustomErasureDriveCount = true
	} else {
		// Returns possible set counts with symmetry.
		setCounts = possibleSetCountsWithSymmetry(setCounts, argPatterns)

		if len(setCounts) == 0 {
			msg := fmt.Sprintf("No symmetric distribution detected with input endpoints provided %s, disks %d cannot be spread symmetrically by any supported erasure set sizes %d", args, commonSize, setSizes)
			return nil, config.ErrInvalidNumberOfErasureEndpoints(nil).Msg(msg)
		}

		// Final set size with all the symmetry accounted for.
		setSize = commonSetDriveCount(commonSize, setCounts)
	}

	// Check whether setSize is with the supported range.
	if !isValidSetSize(setSize) {
		msg := fmt.Sprintf("Incorrect number of endpoints provided %s, number of disks %d is not divisible by any supported erasure set sizes %d", args, commonSize, setSizes)
		return nil, config.ErrInvalidNumberOfErasureEndpoints(nil).Msg(msg)
	}

	for i := range totalSizes {
		for j := uint64(0); j < totalSizes[i]/setSize; j++ {
			setIndexes[i] = append(setIndexes[i], setSize)
		}
	}

	return setIndexes, nil
}

// Returns all the expanded endpoints, each argument is expanded separately.
func (s endpointSet) getEndpoints() (endpoints []string) {
	if len(s.endpoints) != 0 {
		return s.endpoints
	}
	for _, argPattern := range s.argPatterns {
		for _, lbls := range argPattern.Expand() {
			endpoints = append(endpoints, strings.Join(lbls, ""))
		}
	}
	s.endpoints = endpoints
	return endpoints
}

// Get returns the sets representation of the endpoints
// this function also intelligently decides on what will
// be the right set size etc.
func (s endpointSet) Get() (sets [][]string) {
	k := uint64(0)
	endpoints := s.getEndpoints()
	for i := range s.setIndexes {
		for j := range s.setIndexes[i] {
			sets = append(sets, endpoints[k:s.setIndexes[i][j]+k])
			k = s.setIndexes[i][j] + k
		}
	}

	return sets
}

// Return the total size for each argument patterns.
func getTotalSizes(argPatterns []ellipses.ArgPattern) []uint64 {
	var totalSizes []uint64
	for _, argPattern := range argPatterns {
		var totalSize uint64 = 1
		for _, p := range argPattern {
			totalSize *= uint64(len(p.Seq))
		}
		totalSizes = append(totalSizes, totalSize)
	}
	return totalSizes
}

// Parses all arguments and returns an endpointSet which is a collection
// of endpoints following the ellipses pattern, this is what is used
// by the object layer for initializing itself.
func parseEndpointSet(customSetDriveCount uint64, args ...string) (ep endpointSet, err error) {
	// 创建基于传入参数的切片，目前来看长度和容量只有1
	// 理论上传入了几个server pool的配置，就创建长度为几的切片。 TODO 待验证
	argPatterns := make([]ellipses.ArgPattern, len(args))
	for i, arg := range args {

		// 查找带省略号的模式(pattern),因为是SDK中函数，所以将大致流程以注释的方式写到这里.
		// 创建一个Pattern的切片patterns
		// 通过Regexp的FindStringSubmatch方法，查找正则表达式为{[0-9a-z]*\.\.\.[0-9a-z]*}的字符串切片parts
		// 例，传入字符串: http://192.168.1.{1...4}/data/minio{1...8}, 获取字符串切片如下
		//   parts[0]: "http://192.168.1.{1...4}/data/minio{1...8}"
		//   parts[1]: "http://192.168.1.{1...4}/data/minio"
		//   parts[2]: "{1...8}"
		//   parts[3]: ""        是的，还有一个空串。
		// 删除切片parts的第一个元素，切片内容变成一下内容
		//   parts[0]: "http://192.168.1.{1...4}/data/minio"
		//   parts[1]: "{1...8}"
		//   parts[2]: ""
		// 判断parts[0]是否匹配正则表达式{[0-9a-z]*\.\.\.[0-9a-z]*}
		// 匹配的情况下，解析parts[1]的范围，返回一个string类型的切片, 如下
		//   seq{"1","2","3","4","5","6","7","8"}
		// 向patterns切片中放入一个Pattern对象，对象内容如下
		//   Pattern{
		//       Prefix: "",
		//       Suffix: parts[2],   实际是空字符串
		//       Seq: seq,
		//   }
		// 再次通过Regexp的FindStringSubmatch方法，查找parts[0]的符合正则表达式为{[0-9a-z]*\.\.\.[0-9a-z]*}的字符串切片parts
		//   parts[0]: "http://192.168.1.{1...4}/data/minio"
		//   parts[1]: "http://192.168.1."
		//   parts[2]: "{1...4}"
		//   parts[3]: "/data/minio"
		// 再次删除切片parts的第一个元素，切片内容变成一下内容
		//   parts[0]: "http://192.168.1."
		//   parts[1]: "{1...4}"
		//   parts[2]: "/data/minio"
		// 判断parts的长度是否大于0
		// 大于0的情况下，解析parts[1]的范围，返回一个string类型的切片, 如下
		//   seq{"1","2","3","4"}
		// 向patterns切片中放入第二个Pattern对象，对象内容如下
		//   Pattern{
		//       Prefix: parts[0],    实际内容: "http://192.168.1."
		//       Suffix: parts[2],    实际内容: "/data/minio"
		//       Seq: seq,
		//   }
		// 对patterns进行遍历，如果没有{和}字符，则返回patterns，具体内容如下
		//   [
		//       {
		//           Prefix: "",
		//           Suffix: "",
		//           Seq: {"1","2","3","4","5","6","7","8"}
		//       },
		//       {
		//           Prefix: "http://192.168.1.",
		//           Suffix: "/data/minio",
		//           Seq: {"1","2","3","4"}
		//       }
		//   ]
		patterns, perr := ellipses.FindEllipsesPatterns(arg)
		if perr != nil {
			return endpointSet{}, config.ErrInvalidErasureEndpoints(nil).Msg(perr.Error())
		}
		argPatterns[i] = patterns
	}

	// 传入参数内容具体如下
	// args: ["http://192.168.1.{1...4}/data/minio{1...8}"]
	// getTotalSizes(argPatterns): [32]
	// customSetDriveCount: 0
	// argPatterns:
	//   [
	//       [
	//           {
	//               Prefix: "",
	//               Suffix: "",
	//               Seq: {"1","2","3","4","5","6","7","8"}
	//           },
	//           {
	//               Prefix: "http://192.168.1.",
	//               Suffix: "/data/minio",
	//               Seq: {"1","2","3","4"}
	//           }
	//       ]
	//   ]
	ep.setIndexes, err = getSetIndexes(args, getTotalSizes(argPatterns), customSetDriveCount, argPatterns)
	if err != nil {
		return endpointSet{}, config.ErrInvalidErasureEndpoints(nil).Msg(err.Error())
	}

	ep.argPatterns = argPatterns

	return ep, nil
}

// GetAllSets - parses all ellipses input arguments, expands them into
// corresponding list of endpoints chunked evenly in accordance with a
// specific set size.
// For example: {1...64} is divided into 4 sets each of size 16.
// This applies to even distributed setup syntax as well.
func GetAllSets(args ...string) ([][]string, error) {
	var customSetDriveCount uint64
	// 从环境变量MINIO_ERASURE_SET_DRIVE_COUNT中获取每个纠删集有多少块盘
	if v := env.Get(EnvErasureSetDriveCount, ""); v != "" {
		driveCount, err := strconv.Atoi(v)
		if err != nil {
			return nil, config.ErrInvalidErasureSetSize(err)
		}
		customSetDriveCount = uint64(driveCount)
	}

	var setArgs [][]string
	// 传入参数中带有省略号的场合
	if !ellipses.HasEllipses(args...) {
		var setIndexes [][]uint64
		// Check if we have more one args.
		if len(args) > 1 {
			var err error
			setIndexes, err = getSetIndexes(args, []uint64{uint64(len(args))}, customSetDriveCount, nil)
			if err != nil {
				return nil, err
			}
		} else {
			// We are in FS setup, proceed forward.
			setIndexes = [][]uint64{{uint64(len(args))}}
		}
		s := endpointSet{
			endpoints:  args,
			setIndexes: setIndexes,
		}
		setArgs = s.Get()

		// 传入参数中不带省略号的场合
	} else {
		// 解析传入参数
		s, err := parseEndpointSet(customSetDriveCount, args...)
		if err != nil {
			return nil, err
		}
		setArgs = s.Get()
	}

	uniqueArgs := set.NewStringSet()
	for _, sargs := range setArgs {
		for _, arg := range sargs {
			if uniqueArgs.Contains(arg) {
				return nil, config.ErrInvalidErasureEndpoints(nil).Msg(fmt.Sprintf("Input args (%s) has duplicate ellipses", args))
			}
			uniqueArgs.Add(arg)
		}
	}

	return setArgs, nil
}

// Override set drive count for manual distribution.
const (
	EnvErasureSetDriveCount = "MINIO_ERASURE_SET_DRIVE_COUNT"
)

var globalCustomErasureDriveCount = false

// CreateServerEndpoints - validates and creates new endpoints from input args, supports
// both ellipses and without ellipses transparently.
func createServerEndpoints(serverAddr string, args ...string) (
	endpointServerPools EndpointServerPools, setupType SetupType, err error,
) {
	if len(args) == 0 {
		return nil, -1, errInvalidArgument
	}

	// 参数中没有省略号的情况
	if !ellipses.HasEllipses(args...) {
		setArgs, err := GetAllSets(args...)
		if err != nil {
			return nil, -1, err
		}
		endpointList, newSetupType, err := CreateEndpoints(serverAddr, false, setArgs...)
		if err != nil {
			return nil, -1, err
		}
		endpointServerPools = append(endpointServerPools, PoolEndpoints{
			Legacy:       true,
			SetCount:     len(setArgs),
			DrivesPerSet: len(setArgs[0]),
			Endpoints:    endpointList,
			CmdLine:      strings.Join(args, " "),
		})
		setupType = newSetupType
		return endpointServerPools, setupType, nil
	}

	var foundPrevLocal bool

	// 参数中有省略号的情况
	// args是server启动时候传入的参数，例:
	//     http://192.168.50.{1...4}/data/minio{1...4}
	for _, arg := range args {

		// 将省略号解析出来, 生成一个二维slice
		// setArgs[]表示有多少个纠删集，setArgs[][]表示纠删集中磁盘的URL，实例内容如下
		/*
			[
			    [
			        http://192.168.1.1/data/minio1
			        http://192.168.1.2/data/minio1
			        http://192.168.1.3/data/minio1
			        http://192.168.1.4/data/minio1
			        http://192.168.1.1/data/minio2
			        http://192.168.1.2/data/minio2
			        http://192.168.1.3/data/minio2
			        http://192.168.1.4/data/minio2
			        http://192.168.1.1/data/minio3
			        http://192.168.1.2/data/minio3
			        http://192.168.1.3/data/minio3
			        http://192.168.1.4/data/minio3
			        http://192.168.1.1/data/minio4
			        http://192.168.1.2/data/minio4
			        http://192.168.1.3/data/minio4
			        http://192.168.1.4/data/minio4
			    ]
			    [
			        http://192.168.1.1/data/minio5
			        http://192.168.1.2/data/minio5
			        http://192.168.1.3/data/minio5
			        http://192.168.1.4/data/minio5
			        http://192.168.1.1/data/minio6
			        http://192.168.1.2/data/minio6
			        http://192.168.1.3/data/minio6
			        http://192.168.1.4/data/minio6
			        http://192.168.1.1/data/minio7
			        http://192.168.1.2/data/minio7
			        http://192.168.1.3/data/minio7
			        http://192.168.1.4/data/minio7
			        http://192.168.1.1/data/minio8
			        http://192.168.1.2/data/minio8
			        http://192.168.1.3/data/minio8
			        http://192.168.1.4/data/minio8
			    ]
			]
		*/
		// 纠删组数量和每个纠删组磁盘数的计算，简单如下
		// 1. 解析传入参数中的pattern
		// 2. 先获取每个server pool的磁盘总数
		// 3. 计算server pool磁盘数的最大公约数
		// 4. 通过最大公约数，从给定的纠删组磁盘数的数据集中找出可用的数据项
		// 5. 遍历步骤3返回的切片，并且和步骤1中pattern的seq长度做计算，找出对称的数据(对称性计算参看函数possibleSetCountsWithSymmetry)
		// 6. 从步骤5中选出磁盘数最大的选项
		setArgs, err := GetAllSets(arg)
		if err != nil {
			return nil, -1, err
		}

		// 根据解析出来的endpoint名称，创建endpoint的实例和安装类型。
		endpointList, gotSetupType, err := CreateEndpoints(serverAddr, foundPrevLocal, setArgs...)
		if err != nil {
			return nil, -1, err
		}
		if err = endpointServerPools.Add(PoolEndpoints{
			SetCount:     len(setArgs),
			DrivesPerSet: len(setArgs[0]),
			Endpoints:    endpointList,
			CmdLine:      arg,
		}); err != nil {
			return nil, -1, err
		}
		foundPrevLocal = endpointList.atleastOneEndpointLocal()
		if setupType == UnknownSetupType {
			setupType = gotSetupType
		}
		if setupType == ErasureSetupType && gotSetupType == DistErasureSetupType {
			setupType = DistErasureSetupType
		}
	}

	return endpointServerPools, setupType, nil
}
