/*
- @Author: aztec
- @Date: 2024-01-15 18:21:49
- @Description: 抹平不同交易所的一些接口差异
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package data

import (
	"strings"

	"github.com/aztecqt/dagger/api/binanceapi/binancefutureapi"
	"github.com/aztecqt/dagger/api/binanceapi/binancespotapi"
	"github.com/aztecqt/dagger/api/coingeckoapi"
	"github.com/aztecqt/dagger/api/okexv5api"
	"github.com/aztecqt/dagger/util"
	"github.com/aztecqt/qbench/common"
)

// 交易品种
type Instrument struct {
	Id           string
	BaseCurrency string
	Vol24h       float64
	MarketCap    float64
}

// 获取所有交易品种（现货仅取usdt交易对）
func GetAllInstruments(exName common.ExName, symbolType common.InstType, withMarketcap bool) (insts []Instrument, ok bool) {
	insts = []Instrument{}
	ok = false
	switch exName {
	case common.ExName_Okx:
		if okSymbolType, valid := common.CommonInstType2Okx(symbolType); valid {
			if resp, err := okexv5api.GetTickers(okSymbolType); err == nil && resp.Code == "0" {
				for _, tr := range resp.Data {
					if symbolType == common.InstType_Spot && util.StringEndWith(tr.InstId, "USDT") ||
						symbolType == common.InstType_CmSwap && util.StringEndWith(tr.InstId, "USD-SWAP") ||
						symbolType == common.InstType_UmSwap && util.StringEndWith(tr.InstId, "USDT-SWAP") {
						baseCcy := strings.ToLower(strings.Split(tr.InstId, "-")[0])
						insts = append(insts, Instrument{Id: tr.InstId, BaseCurrency: baseCcy, Vol24h: tr.VolUsd24h.InexactFloat64()})
					}
				}
				ok = true
			} else {
				common.LogError("get instruments from okx failed: %s", err.Error())
			}
		}
	case common.ExName_Binance:
		if symbolType == common.InstType_Spot {
			if resp, err := binancespotapi.Get24hrTicker(); err == nil {
				s := *resp
				for _, t := range s {
					if util.StringEndWith(t.Symbol, "USDT") {
						baseCcy := strings.ToLower(strings.ReplaceAll(t.Symbol, "USDT", ""))
						insts = append(insts, Instrument{Id: t.Symbol, BaseCurrency: baseCcy, Vol24h: t.VolumeQuote.InexactFloat64()})
					}
				}
				ok = true
			} else {
				common.LogError("get instruments from okx failed: %s", err.Error())
			}
		} else {
			ac := util.ValueIf(symbolType == common.InstType_CmSwap, binancefutureapi.API_ClassicUsd, binancefutureapi.API_ClassicUsdt)
			if resp, err := binancefutureapi.Get24hrTicker(ac); err == nil {
				s := *resp
				for _, t := range s {
					if util.StringEndWith(t.Symbol, "USDT") {
						baseCcy := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(t.Symbol, "USDT", ""), "1000", ""))
						insts = append(insts, Instrument{Id: t.Symbol, BaseCurrency: baseCcy, Vol24h: t.VolumeQuote.InexactFloat64()})
					} else if util.StringEndWith(t.Symbol, "USD_PERP") {
						ctval := 10.0
						if t.Symbol == "BTCUSD_PERP" {
							ctval = 100
						}
						baseCcy := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(t.Symbol, "USD_PERP", ""), "1000", ""))
						insts = append(insts, Instrument{Id: t.Symbol, BaseCurrency: baseCcy, Vol24h: t.Volume.InexactFloat64() * ctval})
					}
				}
				ok = true
			} else {
				common.LogError("get instruments from okx failed: %s", err.Error())
			}
		}
	}

	// 如果有需要的话，从coingecko获取市值信息
	if withMarketcap {
		symbols := []string{}
		for _, i := range insts {
			symbols = append(symbols, i.BaseCurrency)
		}

		if resp, err := coingeckoapi.GetSimplePriceInfoBySymbol(symbols); err == nil {
			for i := range insts {
				if spi, ok := resp[insts[i].BaseCurrency]; ok {
					insts[i].MarketCap = spi.Marketcap
				}
			}
		} else {
			common.LogError("query from coingecko failed: %s", err.Error())
			ok = false
		}
	}

	return
}
