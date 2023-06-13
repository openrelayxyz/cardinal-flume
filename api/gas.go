package api

import (
	"fmt"
	"context"
	"database/sql"
	"math/big"
	"sort"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/cardinal-types/metrics"
	"github.com/openrelayxyz/cardinal-flume/config"
	evm "github.com/openrelayxyz/cardinal-evm/types"
	"github.com/openrelayxyz/cardinal-rpc"
	eh "github.com/openrelayxyz/cardinal-flume/errhandle"
	"github.com/openrelayxyz/cardinal-flume/heavy"
	"github.com/openrelayxyz/cardinal-flume/plugins"
)

type GasAPI struct {
	db      *sql.DB
	network uint64
	pl      *plugins.PluginLoader
	cfg     *config.Config
}

func NewGasAPI(db *sql.DB, network uint64, pl *plugins.PluginLoader, cfg *config.Config) *GasAPI {
	return &GasAPI{
		db:      db,
		network: network,
		pl:      pl,
		cfg:     cfg,
	}
}

func (api *GasAPI) gasTip(ctx context.Context) (*big.Int, error) {
	latestBlock, err := getLatestBlock(ctx, api.db)
	if err != nil {
		return nil, err
	}
	rows, err := api.db.QueryContext(ctx, "SELECT gasPrice, baseFee from transactions.transactions INNER JOIN blocks.blocks ON transactions.block = blocks.number WHERE blocks.number > ?;", latestBlock-20)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tips := bigList{}
	for rows.Next() {
		var gasPrice int64
		var baseFeeBytes []byte
		if err := rows.Scan(&gasPrice, &baseFeeBytes); err != nil {
			return nil, err
		}
		tip := new(big.Int).Sub(big.NewInt(gasPrice), new(big.Int).SetBytes(baseFeeBytes))
		if tip.Cmp(new(big.Int)) > 0 {
			tips = append(tips, tip)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(tips) > 0 {
		sort.Sort(tips)
		return tips[(len(tips)*6)/10], nil
	}
	var gasPrice int64
	var baseFeeBytes []byte
	err = api.db.QueryRowContext(ctx, "SELECT gasPrice, baseFee from transactions.transactions INNER JOIN blocks.blocks ON transactions.block = blocks.number WHERE 1 ORDER BY id DESC LIMIT 1;").Scan(&gasPrice, &baseFeeBytes)
	return new(big.Int).Sub(big.NewInt(gasPrice), new(big.Int).SetBytes(baseFeeBytes)), err
}

func (api *GasAPI) nextBaseFee(ctx context.Context) (*big.Int, error) {
	// The below value will change after the Mumbai hardfork on Polygon but no other networks at this time. 
	baseFeeDenominator := api.cfg.GetBaseFeeDenominator(api.db)

	var baseFeeBytes []byte
	var gasLimit, gasUsed int64
	err := api.db.QueryRowContext(ctx, "SELECT baseFee, gasUsed, gasLimit FROM blocks.blocks ORDER BY blocks.number DESC LIMIT 1;").Scan(&baseFeeBytes, &gasUsed, &gasLimit)
	if err != nil {
		return nil, err
	}
	baseFee := new(big.Int).SetBytes(baseFeeBytes)
	gasTarget := gasLimit / 2
	if gasUsed == gasTarget {
		return baseFee, nil
	} else if gasUsed > gasTarget {
		delta := gasUsed - gasTarget
		baseFeeDelta := new(big.Int).Div(new(big.Int).Div(new(big.Int).Mul(baseFee, new(big.Int).SetInt64(delta)), new(big.Int).SetInt64(gasTarget)), baseFeeDenominator)
		if baseFeeDelta.Cmp(new(big.Int)) == 0 {
			baseFeeDelta = big.NewInt(1)
		}
		return new(big.Int).Add(baseFee, baseFeeDelta), nil
	}
	delta := gasTarget - gasUsed
	baseFeeDelta := new(big.Int).Div(new(big.Int).Div(new(big.Int).Mul(baseFee, new(big.Int).SetInt64(delta)), new(big.Int).SetInt64(gasTarget)), baseFeeDenominator)
	return new(big.Int).Sub(baseFee, baseFeeDelta), nil
}

var (
	gpHitMeter  = metrics.NewMinorMeter("/flume/gp/hit")
	gpMissMeter = metrics.NewMinorMeter("/flume/gp/miss")
)

func (api *GasAPI) GasPrice(ctx context.Context) (string, error) {
	// we need to do a light / heavy check here as the underlying gasTip method relies on current block
	latestBlock, err := getLatestBlock(ctx, api.db)
	if err != nil {
		return "", err
	}
	earliestRequiredBlock := latestBlock - 20
	if earliestRequiredBlock < int64(api.cfg.EarliestBlock) {
		log.Debug("eth_gasPrince sent to flume heavy")
		missMeter.Mark(1)
		gpMissMeter.Mark(1)
		price, err := heavy.CallHeavy[string](ctx, api.cfg.HeavyServer, "eth_gasPrice")
		if err != nil {
			return "", err
		}
		return *price, nil
	}

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("eth_gasPrice served from flume light")
		hitMeter.Mark(1)
		gpHitMeter.Mark(1)
	}

	tip, err := api.gasTip(ctx)
	if err != nil {
		return "", err
	}
	baseFee, err := api.nextBaseFee(ctx)
	if err != nil {
		return "", err
	}
	sum := big.NewInt(0)
	sum.Add(tip, baseFee)
	result := hexutil.EncodeBig(sum)
	return result, nil
}

var (
	mpfgHitMeter  = metrics.NewMinorMeter("/flume/mpfg/hit")
	mpfgMissMeter = metrics.NewMinorMeter("/flume/mpfg/miss")
)

func (api *GasAPI) MaxPriorityFeePerGas(ctx context.Context) (res string, err error) {
	// we need to do a light / heavy check here as the underlying gasTip method relies on current block
	latestBlock, err := getLatestBlock(ctx, api.db)
	if err != nil {
		return "", err
	}
	earliestRequiredBlock := latestBlock - 20

	if earliestRequiredBlock < int64(api.cfg.EarliestBlock) {
		log.Debug("eth_MaxPriorityFeePerGas sent to flume heavy")
		missMeter.Mark(1)
		mpfgMissMeter.Mark(1)
		price, err := heavy.CallHeavy[string](ctx, api.cfg.HeavyServer, "eth_maxPriorityFeePerGas")
		if err != nil {
			return "", err
		}
		return *price, nil
	}

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("eth_maxPriorityFeePerGas served from flume light")
		hitMeter.Mark(1)
		mpfgHitMeter.Mark(1)
	}

	defer eh.HandleErr(&err)
	return hexutil.EncodeBig(eh.CheckAndAssign(api.gasTip(ctx))), nil
}

var (
	gfhHitMeter  = metrics.NewMinorMeter("/flume/gfh/hit")
	gfhMissMeter = metrics.NewMinorMeter("/flume/gfh/miss")
)

func (api *GasAPI) ascendingCheck(rewardPercentiles []float64) error {
	var previous float64
	for i, p := range rewardPercentiles {
		if p < previous {
			err := rpc.NewRPCError(-32000, fmt.Sprintf("invalid reward percentile: #%v:%.6f > #%v:%.6f", (i-1), previous, i, p))
			return err
		}
		previous = p
	}
	return nil
}

func (api *GasAPI) FeeHistory(ctx context.Context, blockCount DecimalOrHex, terminalBlock rpc.BlockNumber, rewardPercentiles []float64) (res *feeHistoryResult, err error) {
	// The below value will change after the Mumbai hardfork on Polygon but no other networks at this time.
	baseFeeDenominator := api.cfg.GetBaseFeeDenominator(api.db)
	
	defer eh.HandleErr(&err)
	
	if blockCount > 128 {
		blockCount = DecimalOrHex(128)
	} else if blockCount == 0 {
		blockCount = DecimalOrHex(20)
	}

	var lastBlock rpc.BlockNumber
	var pbs *pendingBlockSimulator

	if int64(terminalBlock) < 0 {

		latestBlock, err := getLatestBlock(ctx, api.db)
		if err != nil {
			return nil, err
		}
		lastBlock = rpc.BlockNumber(latestBlock)

		if terminalBlock == rpc.PendingBlockNumber { 
			pbs, err = api.constructPendingBlock(ctx, lastBlock)
			if err != nil {
				log.Error("Error retrieving pending block", "err", err)
			}
			lastBlock++
		}

	} else {
		lastBlock = terminalBlock
	}

	earliestBlockInCall := (int64(lastBlock) - int64(blockCount) + 1)

	if (earliestBlockInCall) < int64(api.cfg.EarliestBlock) {
		log.Debug("eth_feeHistory sent to flume heavy")
		missMeter.Mark(1)
		gfhMissMeter.Mark(1)
		responseShell, err := heavy.CallHeavy[*feeHistoryResult](ctx, api.cfg.HeavyServer, "eth_feeHistory", blockCount, rpc.BlockNumber(earliestBlockInCall), rewardPercentiles)
		if err != nil {
			return nil, err
		}
		return *responseShell, nil
	}

	if len(api.cfg.HeavyServer) > 0 {
		log.Debug("eth_feeHistory served from flume light")
		hitMeter.Mark(1)
		gfhHitMeter.Mark(1)
	}

	rows := eh.CheckAndAssign(api.db.QueryContext(ctx, "SELECT baseFee, number, gasUsed, gasLimit FROM blocks.blocks WHERE number > ? LIMIT ?;", int64(lastBlock)-int64(blockCount), blockCount))

	result := &feeHistoryResult{
		OldestBlock:  (*hexutil.Big)(new(big.Int).SetInt64(int64(lastBlock) - int64(blockCount) + 1)),
		BaseFee:      make([]*hexutil.Big, int(blockCount) + 1),
		GasUsedRatio: make([]float64, int(blockCount)),
	}
	if len(rewardPercentiles) > 0 {
		result.Reward = make([][]*hexutil.Big, int(blockCount))
		if err := api.ascendingCheck(rewardPercentiles); err != nil {
			return nil, err
		}
	}
	var lastBaseFee *big.Int
	var lastGasUsed, lastGasLimit int64
	for i := 0; rows.Next(); i++ {
		var baseFeeBytes []byte
		var number uint64
		var gasUsed, gasLimit sql.NullInt64
		eh.Check(rows.Scan(&baseFeeBytes, &number, &gasUsed, &gasLimit))
		baseFee := new(big.Int).SetBytes(baseFeeBytes)
		lastBaseFee = baseFee
		result.BaseFee[i] = (*hexutil.Big)(baseFee)
		result.GasUsedRatio[i] = float64(gasUsed.Int64) / float64(gasLimit.Int64)
		lastGasUsed = gasUsed.Int64
		lastGasLimit = gasLimit.Int64
		if len(rewardPercentiles) > 0 {
			tips := sortGasAndReward{}
			txRows := eh.CheckAndAssign(api.db.QueryContext(ctx, "SELECT gasPrice, gasUsed FROM transactions.transactions WHERE block = ?;", number))
			for txRows.Next() {
				var gasPrice, txGasUsed uint64
				eh.Check(txRows.Scan(&gasPrice, &txGasUsed))
				tip := new(big.Int).Sub(new(big.Int).SetUint64(gasPrice), baseFee)
				tips = append(tips, txGasAndReward{reward: tip, gasUsed: txGasUsed})
			}
			eh.Check(txRows.Err())
			result.Reward[i] = make([]*hexutil.Big, len(rewardPercentiles))
			if len(tips) == 0 {
				for j := range rewardPercentiles {
					result.Reward[i][j] = new(hexutil.Big)
				}
				continue
			}
			sort.Sort(tips)
			var txIndex int
			sumGasUsed := tips[0].gasUsed
			for j, p := range rewardPercentiles {
				thresholdGasUsed := uint64(float64(gasUsed.Int64) * p / 100)
				for sumGasUsed < thresholdGasUsed && txIndex < len(tips)-1 {
					txIndex++
					sumGasUsed += tips[txIndex].gasUsed
				}
				result.Reward[i][j] = (*hexutil.Big)(tips[txIndex].reward)
			}
		}
		eh.Check(rows.Err())
	}

	if pbs != nil {
		result.GasUsedRatio[len(result.GasUsedRatio) -1] = pbs.gasUsedRatio

		if len(rewardPercentiles) > 0 {
			result.Reward[len(result.Reward) -1] = make([]*hexutil.Big, len(rewardPercentiles))
			if len(pbs.pendingTxns) > 0 {
				for i, p := range rewardPercentiles {
					idx := int((p / 100) * float64(len(pbs.pendingTxns)))
					result.Reward[len(result.Reward) -1][i] = (*hexutil.Big)(pbs.pendingTxns[idx].gasTipCap)
				}
			} else {
				for i, _ := range rewardPercentiles {
					result.Reward[len(result.Reward) -1][i] = (*hexutil.Big)(new(big.Int))
				}
			}
		}
		result.BaseFee[len(result.BaseFee) -2] = (*hexutil.Big)(pbs.baseFee)
		lastBaseFee = pbs.baseFee
		lastGasUsed = pbs.gasUsed
	}

	gasTarget := lastGasLimit / 2
	if lastGasUsed == gasTarget {
		result.BaseFee[len(result.BaseFee)-1] = (*hexutil.Big)(lastBaseFee)
	} else if lastGasUsed > gasTarget {
		delta := lastGasUsed - gasTarget
		baseFeeDelta := new(big.Int).Div(new(big.Int).Div(new(big.Int).Mul(lastBaseFee, new(big.Int).SetInt64(delta)), new(big.Int).SetInt64(gasTarget)), baseFeeDenominator)
		if baseFeeDelta.Cmp(new(big.Int)) == 0 {
			baseFeeDelta = big.NewInt(1)
		}
		result.BaseFee[len(result.BaseFee)-1] = (*hexutil.Big)(new(big.Int).Add(lastBaseFee, baseFeeDelta))
	} else {
		delta := gasTarget - lastGasUsed
		baseFeeDelta := new(big.Int).Div(new(big.Int).Div(new(big.Int).Mul(lastBaseFee, new(big.Int).SetInt64(delta)), new(big.Int).SetInt64(gasTarget)), baseFeeDenominator)
		result.BaseFee[len(result.BaseFee)-1] = (*hexutil.Big)(new(big.Int).Sub(lastBaseFee, baseFeeDelta))
	}

	return result, nil
}

type pendingBlockSimulator struct {
	baseFee *big.Int
	gasUsedRatio float64
	gasUsed int64
	pendingTxns  []pendingTransaction
}

type pendingTransaction struct {
	gas uint64
	gasPrice uint64
	gasFeeCap uint64 
	gasTipCap *big.Int
}

func (api *GasAPI) constructPendingBlock(ctx context.Context, lastBlock rpc.BlockNumber) (*pendingBlockSimulator, error) {

	var gasLimit uint64
	if err := api.db.QueryRowContext(ctx, "SELECT gasLimit FROM blocks.blocks WHERE number = ?;", int64(lastBlock)).Scan(&gasLimit); err != nil {
		log.Error("dq query error", "err", err)
	}

	nbf, err := api.nextBaseFee(ctx)
	if err != nil {
		log.Error("Error calculating next base fee while constructing pending block", "err", err)
		return nil, err
	}
	baseFee := nbf.Uint64()
	
	txRows := eh.CheckAndAssign(api.db.QueryContext(ctx, "SELECT gas, gasPrice, type, gasFeeCap, gasTipCap FROM mempool.transactions WHERE gasPrice > ? ORDER BY gasPrice DESC;", baseFee))
	
	defer txRows.Close()
	
	var pendingTxns []pendingTransaction
	for txRows.Next() {
		var gas, gasPrice uint64
		var gasFeeCapBytes, gasTipCapBytes []byte
		var txTypeRaw sql.NullInt32

		eh.Check(txRows.Scan(&gas, &gasPrice, &txTypeRaw, &gasFeeCapBytes, &gasTipCapBytes))
		
		txType := uint8(txTypeRaw.Int32)
		pt := pendingTransaction{}
		switch txType {
			case evm.DynamicFeeTxType:
				pt.gas = gas
				pt.gasPrice = gasPrice
				pt.gasFeeCap = new(big.Int).SetBytes(gasFeeCapBytes).Uint64()
				pt.gasTipCap = new(big.Int).SetBytes(gasTipCapBytes)
			default:
				pt.gas = gas
				pt.gasPrice = gasPrice
				pt.gasFeeCap = gasPrice
				pt.gasTipCap = new(big.Int).SetUint64(gasPrice)
		}
		eh.Check(txRows.Err())
		pendingTxns = append(pendingTxns, pt)
	}

	sort.Slice(pendingTxns, func(i, j int) bool {return pendingTxns[i].gasTipCap.Cmp(pendingTxns[j].gasTipCap) > 0})
	
	truncPendingTxns := []pendingTransaction{}

	var gasUsed uint64
	for _, ptx := range pendingTxns {
		if gasUsed + ptx.gas > gasLimit {
			continue
		}
		gasUsed += ptx.gas
		truncPendingTxns = append(truncPendingTxns, ptx)
		if gasUsed + 21000 >= gasLimit {
			break
		}
	}


	return &pendingBlockSimulator{
		baseFee: nbf,
		gasUsedRatio: float64(gasUsed) / float64(gasLimit),
		gasUsed: int64(gasUsed),
		pendingTxns: truncPendingTxns,
	}, nil

	
}
 