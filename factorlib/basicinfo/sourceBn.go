/*
- @Author: aztec
- @Date: 2024-01-18 10:54:29
- @Description: 基础数据源（币安）
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package basicinfo

import (
	"time"

	"github.com/aztecqt/dagger/api/binanceapi"
	"github.com/aztecqt/dagger/api/binanceapi/binancefutureapi"
	"github.com/aztecqt/dagger/api/binanceapi/binancespotapi"
	"github.com/aztecqt/dagger/api/binanceapi/cachedBn"
	"github.com/aztecqt/dagger/util"
	"github.com/aztecqt/qbench/common"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

type SourceBn struct {
	logPrefix string

	// instId每日更新一次
	instIds           []string
	instIdRefreshTime time.Time
}

func NewSourceBn() *SourceBn {
	s := &SourceBn{}
	s.refreshInstIds()
	s.logPrefix = "BasicInfoSrc-Bn"
	return s
}

func (s *SourceBn) FactorNames() []string {
	return []string{"open", "close", "high", "low", "volume"}
}

func (s *SourceBn) InstIds() []string {
	return s.instIds
}

func (s *SourceBn) refreshInstIds() {
	if util.DateOfTime(s.instIdRefreshTime) != util.DateOfTime(time.Now()) {
		s.instIdRefreshTime = time.Now()

		instIds := []string{}

		// 添加现货交易对
		if resp, err := binancespotapi.GetExchangeInfo_Symbols(""); err == nil {
			for _, symbol := range resp.Symbols {
				// 仅支持usdt交易对
				if util.StringEndWith(symbol.Symbol, "USDT") {
					if id, ok := common.ToCommonInstId(common.ExName_Binance, common.InstType_Spot, symbol.Symbol); ok {
						instIds = append(instIds, id)
					} else {
						common.LogError(s.logPrefix, "can't convert %s to instId", symbol)
					}
				}
			}
		} else {
			common.LogError(s.logPrefix, "get exchange info failed: %s", err.Error())
		}

		// 添加U本位合约交易对
		if resp, err := binancefutureapi.GetExchangeInfo_Symbols(binancefutureapi.API_ClassicUsdt); err == nil {
			for _, symbol := range resp.Symbols {
				if id, ok := common.ToCommonInstId(common.ExName_Binance, common.InstType_UmSwap, symbol.Symbol); ok {
					instIds = append(instIds, id)
				} else {
					common.LogError(s.logPrefix, "can't convert %s to instId", symbol)
				}
			}
		} else {
			common.LogError(s.logPrefix, "get exchange info failed: %s", err.Error())
		}

		// 添加币本位合约交易对
		if resp, err := binancefutureapi.GetExchangeInfo_Symbols(binancefutureapi.API_ClassicUsd); err == nil {
			for _, symbol := range resp.Symbols {
				if id, ok := common.ToCommonInstId(common.ExName_Binance, common.InstType_CmSwap, symbol.Symbol); ok {
					instIds = append(instIds, id)
				} else {
					common.LogError(s.logPrefix, "can't convert %s to instId", symbol)
				}
			}
		} else {
			common.LogError(s.logPrefix, "get exchange info failed: %s", err.Error())
		}
	}
}

func (s *SourceBn) GetDataSince(instId string, intervalSec int, since time.Time) (*dataframe.DataFrame, bool) {
	instType := common.GetInstType(instId)

	// 转换为binance的instId
	if v, ok := common.ToExchangeInstId(common.ExName_Binance, instId); ok {
		instId = v
		times := series.New(nil, series.Int, "time")
		opens := series.New(nil, series.Float, "open")
		closes := series.New(nil, series.Float, "close")
		highs := series.New(nil, series.Float, "high")
		lows := series.New(nil, series.Float, "low")
		volumes := series.New(nil, series.Float, "volume")
		kus := []binanceapi.KLineUnit{}
		if instType == common.InstType_Spot {
			if v, ok := cachedBn.GetSpotKline(instId, since.Add(time.Millisecond), time.Now(), intervalSec, nil); ok {
				kus = v
			} else {
				common.LogError(s.logPrefix, "get binance spot kline failed")
				return nil, false
			}
		} else {
			if v, ok := cachedBn.GetFutureKline(instId, since.Add(time.Millisecond), time.Now(), intervalSec, nil); ok {
				kus = v
			} else {
				common.LogError(s.logPrefix, "get binance future kline failed")
				return nil, false
			}
		}

		for _, ku := range kus {
			times.Append(ku.Time.UnixMilli())
			opens.Append(ku.Open.InexactFloat64())
			closes.Append(ku.Close.InexactFloat64())
			highs.Append(ku.High.InexactFloat64())
			lows.Append(ku.Low.InexactFloat64())
			volumes.Append(ku.VolumeUSD.InexactFloat64())
		}

		df := dataframe.New(times, opens, closes, highs, lows, volumes)
		return &df, true
	} else {
		common.LogError(s.logPrefix, "can't convert %s to ex-instid", instId)
		return nil, false
	}
}
