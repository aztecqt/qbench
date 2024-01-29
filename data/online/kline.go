/*
- @Author: aztec
- @Date: 2024-01-16 18:42:16
- @Description: k线数据的在线加载
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package online

import (
	"time"

	"github.com/aztecqt/bench/common"
	"github.com/aztecqt/dagger/api/binanceapi"
	"github.com/aztecqt/dagger/api/binanceapi/cachedBn"
	"github.com/aztecqt/dagger/api/okexv5api/cachedOk"
)

func LoadKline(t0, t1 time.Time, ex common.ExName, instType common.InstType, instId string, interval int) *common.KLine {
	if ex == common.ExName_Okx {
		if exInstId, ok := common.ToExchangeInstId(ex, instId); ok {
			if kus, ok := cachedOk.GetKline(exInstId, t0, t1, interval, nil); ok {
				// 格式转换
				kl := &common.KLine{InstId: instId}
				for _, ku := range kus {
					kl.Units = append(kl.Units,
						common.KlineUnit{
							Time:       ku.Time,
							OpenPrice:  ku.Open.InexactFloat64(),
							ClosePrice: ku.Close.InexactFloat64(),
							HighPrice:  ku.High.InexactFloat64(),
							LowPrice:   ku.Low.InexactFloat64(),
						})
				}
				return kl
			}
		}
	} else if ex == common.ExName_Binance {
		if exInstId, ok := common.ToExchangeInstId(ex, instId); ok {
			var kus []binanceapi.KLineUnit
			kusok := false
			if instType == common.InstType_Spot {
				kus, kusok = cachedBn.GetSpotKline(exInstId, t0, t1, interval, nil)
			} else {
				kus, kusok = cachedBn.GetFutureKline(exInstId, t0, t1, interval, nil)
			}

			if kusok {
				// 格式转换
				kl := &common.KLine{InstId: instId}
				for _, ku := range kus {
					kl.Units = append(kl.Units,
						common.KlineUnit{
							Time:       ku.Time,
							OpenPrice:  ku.Open.InexactFloat64(),
							ClosePrice: ku.Close.InexactFloat64(),
							HighPrice:  ku.High.InexactFloat64(),
							LowPrice:   ku.Low.InexactFloat64(),
						})
				}
				return kl
			}
		}
	}

	return nil
}
