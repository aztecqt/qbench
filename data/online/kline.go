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

	"github.com/aztecqt/dagger/api/binanceapi"
	"github.com/aztecqt/dagger/api/binanceapi/cachedbn"
	"github.com/aztecqt/dagger/api/okexv5api/cachedok"
	"github.com/aztecqt/qbench/common"
)

func LoadKline(t0, t1 time.Time, ex common.ExName, instType common.InstType, instId string, interval int) *common.KLine {
	if ex == common.ExName_Okx {
		if exInstId, ok := common.ToExchangeInstId(ex, instId); ok {
			if kus, ok := cachedok.GetKline(exInstId, t0, t1, interval, nil); ok {
				// 格式转换
				kl := &common.KLine{InstId: instId}
				for _, ku := range kus {
					kl.Units = append(kl.Units,
						common.KlineUnit{
							Time:       ku.Time,
							OpenPrice:  ku.Open,
							ClosePrice: ku.Close,
							HighPrice:  ku.High,
							LowPrice:   ku.Low,
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
				kus, kusok = cachedbn.GetSpotKline(exInstId, t0, t1, interval, nil)
			} else {
				kus, kusok = cachedbn.GetFutureKline(exInstId, t0, t1, interval, nil)
			}

			if kusok {
				// 格式转换
				kl := &common.KLine{InstId: instId}
				for _, ku := range kus {
					kl.Units = append(kl.Units,
						common.KlineUnit{
							Time:       ku.Time,
							OpenPrice:  ku.Open,
							ClosePrice: ku.Close,
							HighPrice:  ku.High,
							LowPrice:   ku.Low,
						})
				}
				return kl
			}
		}
	}

	return nil
}
