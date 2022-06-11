package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	db := engine_util.CreateDB(conf.DBPath, false)
	return &StandAloneStorage{engine: engine_util.NewEngines(db, nil, conf.DBPath, "")}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

type standaloneReader struct {
	innerStorage *StandAloneStorage
	innerTxn     *badger.Txn
	iterCount    int
}

func (sr *standaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCF(sr.innerStorage.engine.Kv, cf, key)
	if err == badger.ErrKeyNotFound {
		return val, nil
	}
	return val, err
}

func (sr *standaloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.innerTxn)
}

func (sr *standaloneReader) Close() {
	if sr.iterCount > 0 {
		panic("Unclosed iterator")
	}
	sr.innerTxn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &standaloneReader{s, s.engine.Kv.NewTransaction(false), 0}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	wb := new(engine_util.WriteBatch)
	for _, modify := range batch {
		wb.SetCF(modify.Cf(), modify.Key(), modify.Value())
	}
	return s.engine.WriteKV(wb)
}
