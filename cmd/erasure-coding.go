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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/reedsolomon"
	"github.com/minio/minio/internal/logger"
)

// Erasure - erasure encoding details.
type Erasure struct {
	encoder                  func() reedsolomon.Encoder
	dataBlocks, parityBlocks int
	blockSize                int64
}

// NewErasure creates a new ErasureStorage.
// 创建Erasure结构体实例，设置数据盘数量，冗余盘数量，数据块大小和编码算法
func NewErasure(ctx context.Context, dataBlocks, parityBlocks int, blockSize int64) (e Erasure, err error) {
	// Check the parameters for sanity now.
	if dataBlocks <= 0 || parityBlocks <= 0 {
		return e, reedsolomon.ErrInvShardNum
	}

	if dataBlocks+parityBlocks > 256 {
		return e, reedsolomon.ErrMaxShardNum
	}

	e = Erasure{
		dataBlocks:   dataBlocks,
		parityBlocks: parityBlocks,
		blockSize:    blockSize,
	}

	// Encoder when needed.
	var enc reedsolomon.Encoder
	var once sync.Once
	e.encoder = func() reedsolomon.Encoder {
		once.Do(func() {
			// 传入数据盘数量和校验盘数量，获取reedSolomon实例，
			// reedSolomon实例中的DataShards就是数据盘数量,ParityShards就是校验盘数量
			e, err := reedsolomon.New(dataBlocks, parityBlocks, reedsolomon.WithAutoGoroutines(int(e.ShardSize())))
			if err != nil {
				// Error conditions should be checked above.
				panic(err)
			}
			enc = e
		})
		return enc
	}
	return
}

// EncodeData encodes the given data and returns the erasure-coded data.
// It returns an error if the erasure coding failed.
// 对对象进行数据分片和纠删编码。
// !!请重点看方法体内的注释!!
func (e *Erasure) EncodeData(ctx context.Context, data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return make([][]byte, e.dataBlocks+e.parityBlocks), nil
	}
	// reedSolomon结构体的Splite方法做了下面几件事：
	//  1. 根据结构体中保存的数据盘数量计算每个分片的大小
	//  2. 用步骤1获取的分片大小乘以（数据盘+校验盘）数量，获得对象数据+校验数据的总容量大小，并创建对应大小的切片
	//  3. 将数据对象的切片拷贝到步骤2创建的切片中，后面不足的全部用0补足。
	//  4. 按步骤1获取的切片大小切割对象字节切片，并依次放入将要返回的二维切片中并返回
	// 步骤4中采用了两次for循环，在第一次for循环中将对象数据切割后保存，第二次for循环实际是对步骤3中补0的那部分切片进行分割并保存
	// 两次for循环后，在返回的二维切片中就顺次存放了与数据盘数量和校验盘数量一致的数据切片
	encoded, err := e.encoder().Split(data)
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}
	// Encode方法实际做了下面几件事完成编码：
	//  1. 从传入的encoded中取出数据分片部分和校验分片部分
	//  2. 对数据分片部分的数据进行编码计算并将计算结果回写到校验分片部分
	if err = e.encoder().Encode(encoded); err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}
	return encoded, nil
}

// DecodeDataBlocks decodes the given erasure-coded data.
// It only decodes the data blocks but does not verify them.
// It returns an error if the decoding failed.
func (e *Erasure) DecodeDataBlocks(data [][]byte) error {
	isZero := 0
	for _, b := range data {
		if len(b) == 0 {
			isZero++
			break
		}
	}
	if isZero == 0 || isZero == len(data) {
		// If all are zero, payload is 0 bytes.
		return nil
	}
	// 实际调用reedSolomon结构体的reconstruct方法，该方法中对传入数据做了以下判断：
	// 1. 检查所以长度不为0的二级分片，判断这些分片的长度是否一致
	// 2. 按纠删set长度对传入的data进行遍历。该逻辑要求传入的data是按data数据在前，parity数据在的顺序排列
	//   2.1. 当data[idx]不为0，numberPresent加1。numberPresent表示分片长度大于0的数量
	//   2.2. 当data[idx]不为0，并且idx小于数据盘数量的时候，dataPresent加1。dataPresent表示有数据的数据盘数量
	// 3. numberPresent等于纠删set长度（表示分片数据无丢失）或则dataPresent等于数据盘长度（表示数据盘都是完整的），这种情况无需做恢复处理，直接返回
	// 4. 如果分片长度大于0的数量小于数据盘数量，返回分片太少的错误（已无法做恢复处理）。
	return e.encoder().ReconstructData(data)
}

// DecodeDataAndParityBlocks decodes the given erasure-coded data and verifies it.
// It returns an error if the decoding failed.
func (e *Erasure) DecodeDataAndParityBlocks(ctx context.Context, data [][]byte) error {
	if err := e.encoder().Reconstruct(data); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	return nil
}

// ShardSize - returns actual shared size from erasure blockSize.
// 计算数据处理块（默认1MB）的数据分片大小
// 对象的读写都是按1MB为单位进行处理，而这1MB的数据又要均匀分布在每个硬盘上
func (e *Erasure) ShardSize() int64 {
	return ceilFrac(e.blockSize, int64(e.dataBlocks))
}

// ShardFileSize - returns final erasure size from original size.
//  根据传入的part大小计算该part平均分割到单块数据盘上的大小，重点是平均分割到单块数据盘上。
//  这里的part大小是指对象切分为多个part后单个part的大小。
//  例：
//    单Set12块盘，EC:2，上传对象为100MB，对象被分为5个part，单个part为20MB。有以下结论
//    1. 数据盘数量为10块，传入的totalLength为20MB。
//    2. 按纠删数据处理块大小为1MB计算，传入的20MB数据将被分成20次处理
//    3. 传入的20MB数据将会被再次分为10分，保存在10个数据盘中，文件的名称为part.X
//    4. 该方法返回的实际是20MB数据切分为10份的大小。另外每个磁盘上的part.X文件除了数据外，还包括了校验数据，所以文件实际大小比数据切分后的大小要大。
func (e *Erasure) ShardFileSize(totalLength int64) int64 {
	if totalLength == 0 {
		return 0
	}
	if totalLength == -1 {
		return -1
	}
	// 获取纠删处理块的数量
	numShards := totalLength / e.blockSize
	// 获取最后一个数据块的实际大小
	lastBlockSize := totalLength % e.blockSize
	// 获取最后一个数据块在一块数据盘上占用大小，做了向上取整处理。
	lastShardSize := ceilFrac(lastBlockSize, int64(e.dataBlocks))
	// 返回一块数据盘上的占用大小
	// e.ShardSize()计算了一个数据块（默认1MB）在一块数据盘上占用的大小，做了向上取整
	// 返回一个对象切分到一个数据盘上实际占用的空间大小
	return numShards*e.ShardSize() + lastShardSize
}

// ShardFileOffset - returns the effective offset where erasure reading begins.
// 返回单个分片文件的有效结束位置（分片文件不含校验数据）
//  1. startOffset，起始偏移位置
//  2. length，在对象单个part中的读取长度
//  3. totalLength，单个part的大小。这个par的大小不是磁盘上part.X文件的大小，而是对象分成多个part场景下单个part的大小
//  4. totalLength = startOffset + length
func (e *Erasure) ShardFileOffset(startOffset, length, totalLength int64) int64 {
	shardSize := e.ShardSize()
	shardFileSize := e.ShardFileSize(totalLength)
	// 起始位置加读取长度得到一个结束位置，表示从0位到这个结束为的长度
	// 除以纠删数据处理块长度，表示到结束位位置，会被处理多少次。
	endShard := (startOffset + length) / e.blockSize
	// 处理次数乘以shardSize再加上shardSize的结果和shardFileSize做比较
	tillOffset := endShard*shardSize + shardSize
	if tillOffset > shardFileSize {
		tillOffset = shardFileSize
	}
	return tillOffset
}

// erasureSelfTest performs a self-test to ensure that erasure
// algorithms compute expected erasure codes. If any algorithm
// produces an incorrect value it fails with a hard error.
//
// erasureSelfTest tries to catch any issue in the erasure implementation
// early instead of silently corrupting data.
func erasureSelfTest() {
	// Approx runtime ~1ms
	var testConfigs [][2]uint8
	for total := uint8(4); total < 16; total++ {
		for data := total / 2; data < total; data++ {
			parity := total - data
			testConfigs = append(testConfigs, [2]uint8{data, parity})
		}
	}
	got := make(map[[2]uint8]map[ErasureAlgo]uint64, len(testConfigs))
	// Copied from output of fmt.Printf("%#v", got) at the end.
	want := map[[2]uint8]map[ErasureAlgo]uint64{{0x2, 0x2}: {0x1: 0x23fb21be2496f5d3}, {0x2, 0x3}: {0x1: 0xa5cd5600ba0d8e7c}, {0x3, 0x1}: {0x1: 0x60ab052148b010b4}, {0x3, 0x2}: {0x1: 0xe64927daef76435a}, {0x3, 0x3}: {0x1: 0x672f6f242b227b21}, {0x3, 0x4}: {0x1: 0x571e41ba23a6dc6}, {0x4, 0x1}: {0x1: 0x524eaa814d5d86e2}, {0x4, 0x2}: {0x1: 0x62b9552945504fef}, {0x4, 0x3}: {0x1: 0xcbf9065ee053e518}, {0x4, 0x4}: {0x1: 0x9a07581dcd03da8}, {0x4, 0x5}: {0x1: 0xbf2d27b55370113f}, {0x5, 0x1}: {0x1: 0xf71031a01d70daf}, {0x5, 0x2}: {0x1: 0x8e5845859939d0f4}, {0x5, 0x3}: {0x1: 0x7ad9161acbb4c325}, {0x5, 0x4}: {0x1: 0xc446b88830b4f800}, {0x5, 0x5}: {0x1: 0xabf1573cc6f76165}, {0x5, 0x6}: {0x1: 0x7b5598a85045bfb8}, {0x6, 0x1}: {0x1: 0xe2fc1e677cc7d872}, {0x6, 0x2}: {0x1: 0x7ed133de5ca6a58e}, {0x6, 0x3}: {0x1: 0x39ef92d0a74cc3c0}, {0x6, 0x4}: {0x1: 0xcfc90052bc25d20}, {0x6, 0x5}: {0x1: 0x71c96f6baeef9c58}, {0x6, 0x6}: {0x1: 0x4b79056484883e4c}, {0x6, 0x7}: {0x1: 0xb1a0e2427ac2dc1a}, {0x7, 0x1}: {0x1: 0x937ba2b7af467a22}, {0x7, 0x2}: {0x1: 0x5fd13a734d27d37a}, {0x7, 0x3}: {0x1: 0x3be2722d9b66912f}, {0x7, 0x4}: {0x1: 0x14c628e59011be3d}, {0x7, 0x5}: {0x1: 0xcc3b39ad4c083b9f}, {0x7, 0x6}: {0x1: 0x45af361b7de7a4ff}, {0x7, 0x7}: {0x1: 0x456cc320cec8a6e6}, {0x7, 0x8}: {0x1: 0x1867a9f4db315b5c}, {0x8, 0x1}: {0x1: 0xbc5756b9a9ade030}, {0x8, 0x2}: {0x1: 0xdfd7d9d0b3e36503}, {0x8, 0x3}: {0x1: 0x72bb72c2cdbcf99d}, {0x8, 0x4}: {0x1: 0x3ba5e9b41bf07f0}, {0x8, 0x5}: {0x1: 0xd7dabc15800f9d41}, {0x8, 0x6}: {0x1: 0xb482a6169fd270f}, {0x8, 0x7}: {0x1: 0x50748e0099d657e8}, {0x9, 0x1}: {0x1: 0xc77ae0144fcaeb6e}, {0x9, 0x2}: {0x1: 0x8a86c7dbebf27b68}, {0x9, 0x3}: {0x1: 0xa64e3be6d6fe7e92}, {0x9, 0x4}: {0x1: 0x239b71c41745d207}, {0x9, 0x5}: {0x1: 0x2d0803094c5a86ce}, {0x9, 0x6}: {0x1: 0xa3c2539b3af84874}, {0xa, 0x1}: {0x1: 0x7d30d91b89fcec21}, {0xa, 0x2}: {0x1: 0xfa5af9aa9f1857a3}, {0xa, 0x3}: {0x1: 0x84bc4bda8af81f90}, {0xa, 0x4}: {0x1: 0x6c1cba8631de994a}, {0xa, 0x5}: {0x1: 0x4383e58a086cc1ac}, {0xb, 0x1}: {0x1: 0x4ed2929a2df690b}, {0xb, 0x2}: {0x1: 0xecd6f1b1399775c0}, {0xb, 0x3}: {0x1: 0xc78cfbfc0dc64d01}, {0xb, 0x4}: {0x1: 0xb2643390973702d6}, {0xc, 0x1}: {0x1: 0x3b2a88686122d082}, {0xc, 0x2}: {0x1: 0xfd2f30a48a8e2e9}, {0xc, 0x3}: {0x1: 0xd5ce58368ae90b13}, {0xd, 0x1}: {0x1: 0x9c88e2a9d1b8fff8}, {0xd, 0x2}: {0x1: 0xcb8460aa4cf6613}, {0xe, 0x1}: {0x1: 0x78a28bbaec57996e}}
	var testData [256]byte
	for i := range testData {
		testData[i] = byte(i)
	}
	ok := true
	for algo := invalidErasureAlgo + 1; algo < lastErasureAlgo; algo++ {
		for _, conf := range testConfigs {
			failOnErr := func(err error) {
				if err != nil {
					logger.Fatal(errSelfTestFailure, "%v: error on self-test [d:%d,p:%d]: %v. Unsafe to start server.\n", algo, conf[0], conf[1], err)
				}
			}
			e, err := NewErasure(context.Background(), int(conf[0]), int(conf[1]), blockSizeV2)
			failOnErr(err)
			encoded, err := e.EncodeData(GlobalContext, testData[:])
			failOnErr(err)
			hash := xxhash.New()
			for i, data := range encoded {
				// Write index to keep track of sizes of each.
				_, err = hash.Write([]byte{byte(i)})
				failOnErr(err)
				_, err = hash.Write(data)
				failOnErr(err)
				got[conf] = map[ErasureAlgo]uint64{algo: hash.Sum64()}
			}

			if a, b := want[conf], got[conf]; !reflect.DeepEqual(a, b) {
				fmt.Fprintf(os.Stderr, "%v: error on self-test [d:%d,p:%d]: want %#v, got %#v\n", algo, conf[0], conf[1], a, b)
				ok = false
				continue
			}
			// Delete first shard and reconstruct...
			first := encoded[0]
			encoded[0] = nil
			failOnErr(e.DecodeDataBlocks(encoded))
			if a, b := first, encoded[0]; !bytes.Equal(a, b) {
				fmt.Fprintf(os.Stderr, "%v: error on self-test [d:%d,p:%d]: want %#v, got %#v\n", algo, conf[0], conf[1], hex.EncodeToString(a), hex.EncodeToString(b))
				ok = false
				continue
			}

		}
	}
	if !ok {
		logger.Fatal(errSelfTestFailure, "Erasure Coding self test failed")
	}
}
