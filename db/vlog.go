package db

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/Mulily0513/C2KV/config"
	"github.com/Mulily0513/C2KV/db/marshal"
	"github.com/Mulily0513/C2KV/db/partition"
	"github.com/Mulily0513/C2KV/db/wal"
	"github.com/Mulily0513/C2KV/log"
	"os"
	"path"
	"strings"
	"sync"
)

const PartitionFormat = "PARTITION_%d"
const Partition = "PARTITION"

type ValueLog struct {
	vlogCfg config.ValueLogConfig

	memFlushC chan *MemTable

	kvStateSeg *wal.KVStateSegment

	partitions []*partition.Partition
}

func OpenValueLog(vlogCfg config.ValueLogConfig, tableC chan *MemTable, stateSegment *wal.KVStateSegment) (vlog *ValueLog) {
	dirs, err := os.ReadDir(vlogCfg.ValueLogDir)
	if err != nil {
		log.Panicf("open wal dir failed", err)
	}

	partitions := make([]*partition.Partition, 0)
	vlog = &ValueLog{memFlushC: tableC, kvStateSeg: stateSegment, vlogCfg: vlogCfg}

	if len(dirs) == 0 {
		for i := 1; i <= vlog.vlogCfg.PartitionNums; i++ {
			partitionDir := path.Join(vlogCfg.ValueLogDir, fmt.Sprintf(PartitionFormat, i))
			if err = os.Mkdir(partitionDir, 0755); err != nil {
				log.Panicf("create partition dir failed", err)
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

func (v *ValueLog) ListenAndFlush() {
	errC := make(chan error, 1)
	for {
		mem := <-v.memFlushC
		kvs := mem.All()
		partitionRecords := make([][]*marshal.KV, v.vlogCfg.PartitionNums)
		//lastKV := kvs[len(kvs)-1]
		//lastRecords := marshal.DecodeData(lastKV.Value)

		//对key进行hash分区
		for _, record := range kvs {
			p := v.getKeyPartition(record.Key)
			kv := new(marshal.KV)
			kv.Key = record.Key
			kv.KeySize = len(record.Key)
			kv.Data = marshal.DecodeData(record.Value)
			partitionRecords[p] = append(partitionRecords[p], kv)
		}

		wg := &sync.WaitGroup{}
		for i := 0; i < v.vlogCfg.PartitionNums; i++ {
			if len(partitionRecords[i]) == 0 {
				continue
			}
			wg.Add(1)
			go v.partitions[i].PersistKvs(partitionRecords[i], wg, errC)
		}
		wg.Wait()

		//todo 索引刷新成功、vlog刷新成功、persistIndex刷新成功应该是一个原子操作
		//v.kvStateSeg.PersistIndex = lastRecords.Index
		//err := v.kvStateSeg.Flush()
		//if err != nil {
		//	log.Panicf("can not flush kv state segment file %e", err)
		//}
	}
}

func (v *ValueLog) Get(key []byte) (kv *marshal.KV, err error) {
	p := v.getKeyPartition(key)
	return v.partitions[p].Get(key)
}

func (v *ValueLog) Scan(low, high []byte) (kvs []*marshal.KV, err error) {
	KvsC := make(chan []*marshal.KV, v.vlogCfg.PartitionNums)
	errC := make(chan error, 1)
	wg := &sync.WaitGroup{}
	for _, p := range v.partitions {
		wg.Add(1)
		go func(p *partition.Partition, wg *sync.WaitGroup) {
			partKvs, err := p.Scan(low, high)
			//todo handle err
			if err != nil {
				errC <- err
			}
			KvsC <- partKvs
			wg.Done()
		}(p, wg)
	}
	wg.Wait()
	close(KvsC)
	for kvSlice := range KvsC {
		kvs = append(kvs, kvSlice...)
	}
	return
}

func (v *ValueLog) getKeyPartition(key []byte) uint64 {
	hash := sha256.Sum256(key)
	return binary.BigEndian.Uint64(hash[:]) % uint64(v.vlogCfg.PartitionNums)
}

func (v *ValueLog) Close() error {
	return nil
}

func (v *ValueLog) Delete() error {
	v.kvStateSeg.Remove()
	return os.RemoveAll(v.vlogCfg.ValueLogDir)
}
