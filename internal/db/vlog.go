package db

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db/marshal"
	"github.com/Mulily0513/C2KV/internal/db/partition"
	"github.com/Mulily0513/C2KV/internal/db/wal"
	"github.com/Mulily0513/C2KV/internal/log"
	"os"
	"path"
	"strings"
	"sync"
)

const PartitionFormat = "PARTITION_%d"
const Partition = "PARTITION"

type ValueLog struct {
	vlogCfg config.ValueLogConfig

	memFlushC chan *memTable

	vlogStateSeg *wal.VlogStateSegment

	partitions []*partition.Partition

	partitionHasher func([]byte) uint64
}

func openValueLog(vlogCfg config.ValueLogConfig, tableC chan *memTable, vlogStateSegment *wal.VlogStateSegment) (vlog *ValueLog) {
	dirs, err := os.ReadDir(vlogCfg.ValueLogDir)
	if err != nil {
		log.Panicf("open wal dir failed %v", err)
	}

	partitions := make([]*partition.Partition, 0)
	vlog = &ValueLog{
		memFlushC:       tableC,
		vlogStateSeg:    vlogStateSegment,
		vlogCfg:         vlogCfg,
		partitionHasher: newPartitionHasher(vlogCfg),
	}

	if len(dirs) == 0 {
		for i := 1; i <= vlog.vlogCfg.PartitionNums; i++ {
			partitionDir := path.Join(vlogCfg.ValueLogDir, fmt.Sprintf(PartitionFormat, i))
			if err = os.Mkdir(partitionDir, 0755); err != nil {
				log.Panicf("create partition dir failed %v", err)
			}
			p := partition.OpenPartition(partitionDir)
			partitions = append(partitions, p)
		}
	}

	if len(dirs) > 0 {
		for _, dir := range dirs {
			if dir.IsDir() && strings.Contains(dir.Name(), Partition) {
				p := partition.OpenPartition(path.Join(vlogCfg.ValueLogDir, dir.Name()))
				partitions = append(partitions, p)
			}
		}
	}

	vlog.partitions = partitions
	return
}

func (v *ValueLog) listenAndFlush() {
	errC := make(chan error, 1)
	for mem := range v.memFlushC {
		if mem == nil {
			continue
		}
		kvs := mem.All()
		if len(kvs) == 0 {
			continue
		}
		partitionRecords := make([][]*marshal.KV, v.vlogCfg.PartitionNums)

		lastKV := kvs[len(kvs)-1]
		lastRecords := marshal.DecodeData(lastKV.Value)

		for _, record := range kvs {
			p := v.getKeyPartition(record.Key)
			kv := new(marshal.KV)
			kv.Key = record.Key
			kv.KeySize = uint32(len(record.Key))
			kv.Data = marshal.DecodeData(record.Value)
			partitionRecords[p] = append(partitionRecords[p], kv)
		}

		wg := new(sync.WaitGroup)
		for i := 0; i < v.vlogCfg.PartitionNums; i++ {
			if len(partitionRecords[i]) == 0 {
				continue
			}
			wg.Add(1)
			go v.partitions[i].PersistKvs(partitionRecords[i], wg, errC)
		}
		wg.Wait()

		if err := v.vlogStateSeg.Save(lastRecords.Index); err != nil {
			log.Panicf("can not flush kv state segment file %e", err)
		}
	}
}

func (v *ValueLog) Get(key []byte) (kv *marshal.KV, err error) {
	p := v.getKeyPartition(key)
	return v.partitions[p].Get(key)
}

func (v *ValueLog) Scan(low, high []byte) (kvs []*marshal.KV, err error) {
	if len(v.partitions) == 0 {
		return nil, nil
	}
	if len(high) > 0 && bytes.Compare(low, high) >= 0 {
		return nil, nil
	}
	if len(v.partitions) == 1 {
		return v.partitions[0].Scan(low, high)
	}

	partitionKVs := make([][]*marshal.KV, len(v.partitions))
	errC := make(chan error, len(v.partitions))
	wg := &sync.WaitGroup{}
	for i, p := range v.partitions {
		partitionID := i
		wg.Add(1)
		go func(p *partition.Partition, wg *sync.WaitGroup) {
			defer wg.Done()
			partKvs, err := p.Scan(low, high)
			if err != nil {
				select {
				case errC <- err:
				default:
				}
				return
			}
			partitionKVs[partitionID] = partKvs
		}(p, wg)
	}
	wg.Wait()
	select {
	case err = <-errC:
		return nil, err
	default:
	}
	return mergeSortedKVSources(partitionKVs, true), nil
}

func (v *ValueLog) getKeyPartition(key []byte) uint64 {
	if v.vlogCfg.PartitionNums <= 1 {
		return 0
	}
	return v.partitionHasher(key) % uint64(v.vlogCfg.PartitionNums)
}

func newPartitionHasher(cfg config.ValueLogConfig) func(key []byte) uint64 {
	switch cfg.NormalizePartitionHash() {
	case config.PartitionHashFNV64a:
		return fnv64a
	default:
		return func(key []byte) uint64 {
			hash := sha256.Sum256(key)
			return binary.BigEndian.Uint64(hash[:])
		}
	}
}

func fnv64a(key []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for _, c := range key {
		hash ^= uint64(c)
		hash *= prime64
	}
	return hash
}

func (v *ValueLog) Close() error {
	var firstErr error
	for _, p := range v.partitions {
		if err := p.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (v *ValueLog) Delete() error {
	v.vlogStateSeg.Remove()
	return os.RemoveAll(v.vlogCfg.ValueLogDir)
}
