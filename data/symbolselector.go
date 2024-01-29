/*
- @Author: aztec
- @Date: 2024-01-15 17:58:57
- @Description: 品种选取器。根据一些列条件，从市场中选取一组品种作为后续处理的目标
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package data

import (
	"slices"

	"github.com/aztecqt/bench/common"
	"github.com/aztecqt/dagger/util"
)

// 根据24小时成交量（交易所内）选取InstId
func SelectInstIdsBy24hVol(exName common.ExName, instType common.InstType, desc bool, limit int) []string {
	logPrefix := "SelectInstIdsBy24hVol"
	common.LogNormal(logPrefix, "selecting inst id by 24hvol, ex=%s, instType=%s desc=%v, limit=%d", exName, instType, desc, limit)
	if insts, ok := GetAllInstruments(exName, instType, false); ok {
		common.LogNormal(logPrefix, "get %d insts", len(insts))
		slices.SortFunc(insts, func(a, b Instrument) int {
			if a.Vol24h < b.Vol24h {
				return util.ValueIf(desc, 1, -1)
			} else if a.Vol24h > b.Vol24h {
				return util.ValueIf(desc, -1, 1)
			} else {
				return 0
			}
		})

		if len(insts) > limit {
			insts = insts[:limit]
		}

		instIds := []string{}
		for _, i := range insts {
			instIds = append(instIds, i.Id)
		}

		common.LogNormal(logPrefix, "%d sorted", len(instIds))
		return instIds
	} else {
		common.LogError(logPrefix, "get all instruments from ex failed")
		return []string{}
	}
}

// 根据市值选取InstId
func SelectInstIdsByMarketCap(exName common.ExName, instType common.InstType, desc bool, limit int) []string {
	logPrefix := "SelectInstIdsByMarketCap"
	common.LogNormal(logPrefix, "selecting inst id by market cap, ex=%s, instType=%s desc=%v, limit=%d", exName, instType, desc, limit)
	if insts, ok := GetAllInstruments(exName, instType, true); ok {
		common.LogNormal(logPrefix, "get %d insts", len(insts))
		slices.SortFunc(insts, func(a, b Instrument) int {
			if a.MarketCap < b.MarketCap {
				return util.ValueIf(desc, 1, -1)
			} else if a.MarketCap > b.MarketCap {
				return util.ValueIf(desc, -1, 1)
			} else {
				return 0
			}
		})

		if len(insts) > limit {
			insts = insts[:limit]
		}

		instIds := []string{}
		for _, i := range insts {
			instIds = append(instIds, i.Id)
		}

		common.LogNormal(logPrefix, "%d sorted", len(instIds))
		return instIds
	} else {
		common.LogError(logPrefix, "get all instruments from ex failed")
		return []string{}
	}
}
