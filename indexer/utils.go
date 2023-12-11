package indexer

import (
	"bytes"
	"fmt"
	"math/big"
	"time"

	"github.com/klauspost/compress/zlib"

	"github.com/openrelayxyz/cardinal-evm/common"
	"github.com/openrelayxyz/cardinal-rpc"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/cardinal-types/metrics"
)

func trimPrefix(data []byte) []byte {
	if len(data) == 0 {
		return data
	}
	v := bytes.TrimLeft(data, string([]byte{0}))
	if len(v) == 0 {
		return []byte{0}
	}
	return v
}

func trimSuffix(data []byte) []byte {
	if len(data) == 0 {
		return data
	}
	v := bytes.TrimRight(data, string([]byte{0}))
	if len(v) == 0 {
		return []byte{0}
	}
	return v
}

var compressor *zlib.Writer
var compressionBuffer = bytes.NewBuffer(make([]byte, 0, 5*1024*1024))
var blockAgeTimer = metrics.NewMajorTimer("/flume/age")
var blockTime *time.Time

func compress(data []byte) []byte {
	if len(data) == 0 {
		return data
	}
	compressionBuffer.Reset()
	if compressor == nil {
		compressor = zlib.NewWriter(compressionBuffer)
	} else {
		compressor.Reset(compressionBuffer)
	}
	compressor.Write(data)
	compressor.Close()
	return compressionBuffer.Bytes()
}

func getCopy(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func getFuncSig(data []byte) []byte {
	if len(data) >= 4 {
		return data[:4]
	}
	return data[:]
}

func nullZeroAddress(addr common.Address) []byte {
	if addr == (common.Address{}) {
		return []byte{}
	}
	return addr.Bytes()
}

type bytesable interface {
	Bytes() []byte
}

// applyParameters applies a set of parameters into a SQL statement in a manner
// that will be safe for execution. Note that this should only be used in the
// context of blocks, transactions, and logs - beyond the datatypes used in
// those datatypes, safety is not guaranteed.
func ApplyParameters(query string, params ...interface{}) string {
	preparedParams := make([]interface{}, len(params))
	for i, param := range params {
		switch value := param.(type) {
		case []byte:
			if len(value) == 0 {
				preparedParams[i] = "NULL"
			} else {
				preparedParams[i] = fmt.Sprintf("X'%x'", value)
			}
		case *common.Address:
			if value == nil {
				preparedParams[i] = "NULL"
				continue
			}
			b := trimPrefix(value.Bytes())
			if len(b) == 0 {
				preparedParams[i] = "NULL"
			} else {
				preparedParams[i] = fmt.Sprintf("X'%x'", b)
			}
		case *types.Hash:
			if value == nil {
				preparedParams[i] = "NULL"
				continue
			}
			b := trimPrefix(value.Bytes())
			if len(b) == 0 {
				preparedParams[i] = "NULL"
			} else {
				preparedParams[i] = fmt.Sprintf("X'%x'", b)
			}
		case common.Address:
			b := trimPrefix(value.Bytes())
			if len(b) == 0 {
				preparedParams[i] = "NULL"
			} else {
				preparedParams[i] = fmt.Sprintf("X'%x'", b)
			}
		case *big.Int:
			if value == nil {
				preparedParams[i] = "NULL"
				continue
			}
			b := trimPrefix(value.Bytes())
			if len(b) == 0 {
				preparedParams[i] = "NULL"
			} else {
				preparedParams[i] = fmt.Sprintf("X'%x'", b)
			}
		case bytesable:
			if value == nil {
				preparedParams[i] = "NULL"
				continue
			}
			b := trimPrefix(value.Bytes())
			if len(b) == 0 {
				preparedParams[i] = "NULL"
			} else {
				preparedParams[i] = fmt.Sprintf("X'%x'", b)
			}
		case hexutil.Bytes:
			if len(value) == 0 {
				preparedParams[i] = "NULL"
			} else {
				preparedParams[i] = fmt.Sprintf("X'%x'", []byte(value[:]))
			}
		case *hexutil.Big:
			if value == nil {
				preparedParams[i] = "NULL"
			} else {
				preparedParams[i] = fmt.Sprintf("X'%x'", trimPrefix(value.ToInt().Bytes()))
			}
		case hexutil.Uint64:
			preparedParams[i] = fmt.Sprintf("%v", uint64(value))
		default:
			preparedParams[i] = fmt.Sprintf("%v", value)
		}
	}
	return fmt.Sprintf(query, preparedParams...)
}

func ApplyBlasterParameters(data interface{}) interface{} {
	switch value := data.(type) {
	case types.Hash:
		b := trimPrefix(value.Bytes())
			return fmt.Sprintf("X'%x'", b)
		}
	
	return nil
}

type HealthCheck struct {
	lastBlockTime time.Time
	processedCount uint
}

func (hc *HealthCheck) Healthy() rpc.HealthStatus {
	switch {
	case time.Since(hc.lastBlockTime) > 60 * time.Second:
		return rpc.Warning
	case hc.processedCount == 0:
		return rpc.Unavailable
	}
	return rpc.Healthy
}