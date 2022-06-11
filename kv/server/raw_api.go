package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	rsp := new(kvrpcpb.RawGetResponse)
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		rsp.Error = err.Error()
		return rsp, err
	}
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		rsp.Error = err.Error()
		return rsp, err
	}
	if len(val) <= 0 {
		rsp.NotFound = true
		return rsp, nil
	}
	rsp.Value = val
	return rsp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	rsp := new(kvrpcpb.RawPutResponse)
	data := storage.Put{
		req.Key,
		req.Value,
		req.Cf,
	}
	batch := []storage.Modify{{
		data,
	}}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		rsp.Error = err.Error()
		return rsp, nil
	}
	return rsp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	rsp := new(kvrpcpb.RawDeleteResponse)
	data := storage.Delete{
		req.Key,
		req.Cf,
	}
	batch := []storage.Modify{{
		data,
	}}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		rsp.Error = err.Error()
		return rsp, nil
	}
	return rsp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	rsp := new(kvrpcpb.RawScanResponse)
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		rsp.Error = err.Error()
		return rsp, err
	}
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	iter.Seek(req.StartKey)
	if !iter.Valid() || req.Limit <= 0 {
		return rsp, nil
	}
	itemCnt := uint32(0)
	for item := iter.Item(); iter.Valid() && itemCnt < req.Limit; iter.Next() {
		var key []byte
		value := make([]byte, item.ValueSize())
		key = item.KeyCopy(key)
		value, _ = item.ValueCopy(value)
		kv := &kvrpcpb.KvPair{Key: key, Value: value}
		rsp.Kvs = append(rsp.Kvs, kv)
		itemCnt++
	}
	return rsp, nil
}
