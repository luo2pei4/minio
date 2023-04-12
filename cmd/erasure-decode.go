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
	"io"
	"sync"
	"sync/atomic"

	"github.com/minio/minio/internal/logger"
)

// Reads in parallel from readers.
type parallelReader struct {
	readers       []io.ReaderAt
	orgReaders    []io.ReaderAt
	dataBlocks    int   // 数据盘数量
	offset        int64 // 在对象的part数据块中的数据读取开始位置
	shardSize     int64 // 单位处理数据块（默认1MB），被平均分割到单块数据盘上的大小（向上取整）
	shardFileSize int64 // 对象的part数据块被平均分割到单块数据盘上的大小
	buf           [][]byte
	readerToBuf   []int
}

// newParallelReader returns parallelReader.
func newParallelReader(readers []io.ReaderAt, e Erasure, offset, totalLength int64) *parallelReader {
	r2b := make([]int, len(readers))
	for i := range r2b {
		r2b[i] = i
	}
	return &parallelReader{
		readers:       readers,
		orgReaders:    readers,
		dataBlocks:    e.dataBlocks,
		offset:        (offset / e.blockSize) * e.ShardSize(),
		shardSize:     e.ShardSize(),
		shardFileSize: e.ShardFileSize(totalLength),
		buf:           make([][]byte, len(readers)),
		readerToBuf:   r2b,
	}
}

// preferReaders can mark readers as preferred.
// These will be chosen before others.
//  对reader的切片进行遍历，将本地磁盘的reader提到切片的最靠前的位置。
//  在调整reader位置的同时，用readerToBuf来记录reader在set中原来规定的位置。
//  记录reader在set中原来规定的位置，是因每块磁盘在set中的位置是固定的，写入时按固定顺序来写入各个磁盘（具体是数据的分块读分片写逻辑）。
//  读取数据的时候也要按磁盘的顺序来读取并拼接，才能返回争取的数据。
//  如果按调整后的reader的顺序来读取再拼接数据，会造成数据错误
func (p *parallelReader) preferReaders(prefer []bool) {
	if len(prefer) != len(p.orgReaders) {
		return
	}
	// Copy so we don't change our input.
	tmp := make([]io.ReaderAt, len(p.orgReaders))
	copy(tmp, p.orgReaders)
	p.readers = tmp
	// next is the next non-preferred index.
	next := 0
	for i, ok := range prefer {
		if !ok || p.readers[i] == nil {
			continue
		}
		if i == next {
			next++
			continue
		}
		// Move reader with index i to index next.
		// Do this by swapping next and i
		p.readers[next], p.readers[i] = p.readers[i], p.readers[next]
		p.readerToBuf[next] = i
		p.readerToBuf[i] = next
		next++
	}
}

// Returns if buf can be erasure decoded.
// 判断buf的长度是否大于或等于数据盘数量
// 1. 大于或等于数据盘数量的场合，表示可以进行解码处理
// 2. 小于数据盘数量的场合，表示还不能进行解码处理
func (p *parallelReader) canDecode(buf [][]byte) bool {
	bufCount := 0
	for _, b := range buf {
		if len(b) > 0 {
			bufCount++
		}
	}
	return bufCount >= p.dataBlocks
}

// Read reads from readers in parallel. Returns p.dataBlocks number of bufs.
func (p *parallelReader) Read(dst [][]byte) ([][]byte, error) {
	newBuf := dst
	if len(dst) != len(p.readers) {
		newBuf = make([][]byte, len(p.readers))
	} else {
		for i := range newBuf {
			newBuf[i] = newBuf[i][:0]
		}
	}
	var newBufLK sync.RWMutex

	// 偏移量加单个处理分片长度（一般为256KB）大于分片文件大小（对象在单个磁盘中保存的文件大小，part.X文件）
	// 说明已经读取到最后一个分片处理单元，且最后这个处理单元的长度不为默认值
	if p.offset+p.shardSize > p.shardFileSize {
		// 重新计算最后一个处理单元的分片处理长度
		p.shardSize = p.shardFileSize - p.offset
	}
	if p.shardSize == 0 {
		return newBuf, nil
	}

	// 创建一个类型为bool，缓冲为len(p.readers)的通道，正常情况下缓冲大小为所有在线盘数量
	readTriggerCh := make(chan bool, len(p.readers))
	defer close(readTriggerCh) // close the channel upon return

	// 向通道中放入数据盘大小个数据
	// 因为ShardSize是按dataBlocks（数据盘）数量进行切分的，所以只需要读取到dataBlocks个ShardSize的数据，就可以获得1MB的处理块大小
	for i := 0; i < p.dataBlocks; i++ {
		// Setup read triggers for p.dataBlocks number of reads so that it reads in parallel.
		readTriggerCh <- true
	}

	bitrotHeal := int32(0)       // Atomic bool flag.
	missingPartsHeal := int32(0) // Atomic bool flag.
	readerIndex := 0
	var wg sync.WaitGroup
	// if readTrigger is true, it implies next disk.ReadAt() should be tried
	// if readTrigger is false, it implies previous disk.ReadAt() was successful and there is no need
	// to try reading the next disk.
	// 按preferReaders方法排序后的顺序来遍历reader
	// 先开启和数据盘数量一致的协程并发读取，如果其中某些reader为nil，则再读取下一块盘。通道名用trigger的意义就是在此。
	for readTrigger := range readTriggerCh {
		// 判断newBuf是否满足解码条件
		newBufLK.RLock()
		canDecode := p.canDecode(newBuf)
		newBufLK.RUnlock()
		if canDecode {
			break
		}
		// readerIndex不超过readers的长度
		if readerIndex == len(p.readers) {
			break
		}
		if !readTrigger {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rr := p.readers[i]
			if rr == nil {
				// Since reader is nil, trigger another read.
				readTriggerCh <- true
				return
			}
			// 获取这个reader在set中所规定的位置
			bufIdx := p.readerToBuf[i]
			if p.buf[bufIdx] == nil {
				// Reading first time on this disk, hence the buffer needs to be allocated.
				// Subsequent reads will re-use this buffer.
				p.buf[bufIdx] = make([]byte, p.shardSize)
			}
			// For the last shard, the shardsize might be less than previous shard sizes.
			// Hence the following statement ensures that the buffer size is reset to the right size.
			p.buf[bufIdx] = p.buf[bufIdx][:p.shardSize]
			// 读取数据到p.buf[bufIdx]，p.offset只是用于和reader中currOffset进行比较
			n, err := rr.ReadAt(p.buf[bufIdx], p.offset)
			if err != nil {
				if errors.Is(err, errFileNotFound) {
					atomic.StoreInt32(&missingPartsHeal, 1)
				} else if errors.Is(err, errFileCorrupt) {
					atomic.StoreInt32(&bitrotHeal, 1)
				}

				// This will be communicated upstream.
				p.orgReaders[bufIdx] = nil
				// 此处对reader设置为nil，在外层(er erasureObjects) getObjectWithFileInfo方法中，将根据这个reader把磁盘的状态设置为离线
				// 具体是在外层遍历part文件中，当读完一个part文件后，会对readers进行遍历并设置为对应的磁盘为offline
				// 此处有坑，当每个磁盘上都删除一个part文件，这样理论上会将每个磁盘设置为离线。
				// 但实际是当设置离线磁盘的数量超过校验盘数量时，读取处理将会强制停止，使客户端下载文件失败。
				p.readers[i] = nil

				// Since ReadAt returned error, trigger another read.
				readTriggerCh <- true
				return
			}
			newBufLK.Lock()
			newBuf[bufIdx] = p.buf[bufIdx][:n]
			newBufLK.Unlock()
			// Since ReadAt returned success, there is no need to trigger another read.
			readTriggerCh <- false
		}(readerIndex)
		readerIndex++
	}
	wg.Wait()
	if p.canDecode(newBuf) {
		p.offset += p.shardSize
		if atomic.LoadInt32(&missingPartsHeal) == 1 {
			return newBuf, errFileNotFound
		} else if atomic.LoadInt32(&bitrotHeal) == 1 {
			return newBuf, errFileCorrupt
		}
		return newBuf, nil
	}

	// If we cannot decode, just return read quorum error.
	return nil, errErasureReadQuorum
}

// Decode reads from readers, reconstructs data if needed and writes the data to the writer.
// A set of preferred drives can be supplied. In that case they will be used and the data reconstructed.
//  对所有磁盘上的同名part文件进行读取和编码
//  offset: 在对象的单个分块中的数据读取开始位置
//  length: 在对象的单个分块中的数据读取长度
//  totalLength: 在对象的单个分块长度。单个分块长度是只对象被分为多个数据块时，其中一个数据块的长度。未分块的场合就是对象总大小
//  另外再补充一个概念，对象分多少part和set中有多少块数据盘没有直接关系。
//  单个分块(part)会根据数据盘数量进行平均分割，被分割的每一块数据写入一个对应的数据盘，并且具有相同的文件名：part.N
func (e Erasure) Decode(ctx context.Context, writer io.Writer, readers []io.ReaderAt, offset, length, totalLength int64, prefer []bool) (written int64, derr error) {
	if offset < 0 || length < 0 {
		logger.LogIf(ctx, errInvalidArgument)
		return -1, errInvalidArgument
	}
	if offset+length > totalLength {
		logger.LogIf(ctx, errInvalidArgument)
		return -1, errInvalidArgument
	}

	if length == 0 {
		return 0, nil
	}

	reader := newParallelReader(readers, e, offset, totalLength)
	// preferReaders是一个优化处理，尽量用到当前节点的磁盘来读取数据。
	if len(prefer) == len(readers) {
		reader.preferReaders(prefer)
	}

	// blockSize默认为1MB
	// 按1MB为读取单位，计算第一个读取单位索引和最后一个读取单位索引
	startBlock := offset / e.blockSize
	endBlock := (offset + length) / e.blockSize

	var bytesWritten int64
	// bufs为二维切片，bufs[]存放的是从各个磁盘上读取的同名part文件的数据
	var bufs [][]byte
	// 按1MB为读取单位对part.N文件进行读取，读取完成后进行编码并写入到writer中
	for block := startBlock; block <= endBlock; block++ {
		// blockOffset: 当前数据处理块（默认1MB）的开始偏移量
		// blockLength: 当前数据处理块的读取长度
		var blockOffset, blockLength int64
		switch {
		// 开始块等于结束块的场合，blockOffset等于传入的offset，blockLength等于传入的length
		case startBlock == endBlock:
			blockOffset = offset % e.blockSize
			blockLength = length
			// 第一个读取块的场合，blockOffset等于传入的offset，blockLength等于读取块长度减去传入的offset
		case block == startBlock:
			blockOffset = offset % e.blockSize
			blockLength = e.blockSize - blockOffset
			// 最后一个读取块的场合，blockOffset等于0，blockLength等于(offset + length) % e.blockSize
		case block == endBlock:
			blockOffset = 0
			blockLength = (offset + length) % e.blockSize
			// 中间的场合，blockOffset为0，blockLength为读取块的长度
		default:
			blockOffset = 0
			blockLength = e.blockSize
		}
		if blockLength == 0 {
			break
		}

		var err error
		// 将指定的part文件从各个硬盘读取到bufs中
		bufs, err = reader.Read(bufs)

		// 当无法对读取数据进行解码时，上面的Read方法返回的bufs为nil，同时返回读仲裁不足的错误
		// 当bufs长度大于0时，说明数据是可以解码的，只是读取的时候可能发生了文件无法读取或文件错误的异常
		if len(bufs) > 0 {
			// Set only if there are be enough data for reconstruction.
			// and only for expected errors, also set once.
			if errors.Is(err, errFileNotFound) || errors.Is(err, errFileCorrupt) {
				if derr == nil {
					derr = err
				}
			}
		} else if err != nil {
			// For all errors that cannot be reconstructed fail the read operation.
			return -1, err
		}

		// 对传入的part切片进行编码
		if err = e.DecodeDataBlocks(bufs); err != nil {
			logger.LogIf(ctx, err)
			return -1, err
		}

		// 虽然在写入数据的时候数据分片有补零的情况，但是在读取的时候，blockOffset, blockLength是按对象真实大小计算出来的值，
		// 在writeDataBlocks函数中，根据blockOffset, blockLength两个参数从传入的bufs中读取数据时，实际不会读取补的零
		n, err := writeDataBlocks(ctx, writer, bufs, e.dataBlocks, blockOffset, blockLength)
		if err != nil {
			return -1, err
		}

		bytesWritten += n
	}

	if bytesWritten != length {
		logger.LogIf(ctx, errLessData)
		return bytesWritten, errLessData
	}

	return bytesWritten, derr
}

// Heal reads from readers, reconstruct shards and writes the data to the writers.
func (e Erasure) Heal(ctx context.Context, writers []io.Writer, readers []io.ReaderAt, totalLength int64) (derr error) {
	if len(writers) != e.parityBlocks+e.dataBlocks {
		return errInvalidArgument
	}

	reader := newParallelReader(readers, e, 0, totalLength)

	startBlock := int64(0)
	endBlock := totalLength / e.blockSize
	if totalLength%e.blockSize != 0 {
		endBlock++
	}

	var bufs [][]byte
	for block := startBlock; block < endBlock; block++ {
		var err error
		bufs, err = reader.Read(bufs)
		if len(bufs) > 0 {
			if errors.Is(err, errFileNotFound) || errors.Is(err, errFileCorrupt) {
				if derr == nil {
					derr = err
				}
			}
		} else if err != nil {
			return err
		}

		if err = e.DecodeDataAndParityBlocks(ctx, bufs); err != nil {
			logger.LogIf(ctx, err)
			return err
		}

		w := parallelWriter{
			writers:     writers,
			writeQuorum: 1,
			errs:        make([]error, len(writers)),
		}

		if err = w.Write(ctx, bufs); err != nil {
			logger.LogIf(ctx, err)
			return err
		}
	}

	return derr
}
