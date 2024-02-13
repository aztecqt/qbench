/*
- @Author: aztec
- @Date: 2024-01-31 10:53:36
- @Description: executor的行情驱动部分
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package backtest

import (
	"slices"
	"time"

	"github.com/aztecqt/dagger/util"
	"github.com/aztecqt/dagger/util/terminal"
	"github.com/aztecqt/qbench/common"
	"github.com/aztecqt/qbench/data/local"
)

// 加载指定品种的、指定时间段内的、指定类型行情
// klineIntervalSec填0表示不需要k线
func (e *Executor) LoadMarketInfo(
	ex common.ExName,
	instIds []string,
	t0, t1 time.Time,
	ticker, depth, trades, liquidations bool, klineIntervalSec int) bool {
	e.instIds = instIds
	e.instIdIndexs = map[string]int{}
	for i, v := range instIds {
		e.instIdIndexs[v] = i
	}

	// 估算加载进度：
	// 加载1个文件算1分(加载1个depth算3分)，sort算1分
	prgMax :=
		1 + float64(len(instIds))*
			(util.ValueIf(ticker, 1.0, 0)+
				util.ValueIf(depth, 3.0, 0)+
				util.ValueIf(trades, 1.0, 0)+
				util.ValueIf(liquidations, 1.0, 0)+
				util.ValueIf(klineIntervalSec > 0, 1.0, 0))

	tracker := terminal.GenTrackerWithHardwareInfo("行情加载", prgMax, 30, true, false, true, true, true)

	// 加载数据
	if ticker {
		if !e.loadTickers(t0, t1, ex, tracker) {
			tracker.MarkAsErrored()
			return false
		}
		e.useTicker = true
	}

	if depth {
		if !e.loadDepths(t0, t1, ex, tracker) {
			tracker.MarkAsErrored()
			return false
		}
		e.useDepth = true
	}

	if trades {
		if !e.loadTrades(t0, t1, ex, tracker) {
			tracker.MarkAsErrored()
			return false
		}
		e.useTrades = true
	}

	if liquidations {
		if !e.loadLiquidations(t0, t1, ex, tracker) {
			tracker.MarkAsErrored()
			return false
		}
		e.useLiquidations = true
	}

	if klineIntervalSec > 0 {
		if !e.loadKlines(t0, t1, ex, klineIntervalSec, tracker) {
			tracker.MarkAsErrored()
			return false
		}
		e.useKline = klineIntervalSec > 0
	}

	if e.useKline {
		e.pxbyKline = true
	} else if e.useTicker {
		e.pxbyTicker = true
	} else if e.useTrades {
		e.pxbyTrades = true
	} else if e.useDepth {
		e.pxbyDepth = true
	}

	// 初始化可视数据起始时间
	if len(e.marketInfoSeq) > 0 {
		e.dgNextRefreshTime = util.AlignTime(e.marketInfoSeq[0].time, e.cfg.ChartsIntervalMs)
	} else {
		common.LogError(logPrefix, "no data loaded!")
		return false
	}

	// 数据加载完毕，执行排序
	slices.SortFunc(e.marketInfoSeq, func(a, b marketInfoUnit) int {
		return a.time.Compare(b.time)
	})
	time.Sleep(time.Millisecond * 100)

	tracker.MarkAsDone()
	return true
}

func (e *Executor) loadTickers(t0, t1 time.Time, exName common.ExName, tracker *terminal.TrackerF) bool {
	validInstIds := local.GetValidTickerInstIds(exName)
	for _, instId := range e.instIds {
		if slices.Contains(validInstIds, instId) {
			if tmin, tmax, ok := local.GetValidTickerTimeRange(exName, instId); ok {
				if tmin.After(t0) || tmax.Before(t1) {
					common.LogError(logPrefix, "not enough ticker data for %s@%s", instId, exName)
					return false
				}
			} else {
				common.LogError(logPrefix, "get ticker time range failed for %s@%s", instId, exName)
				return false
			}
		} else {
			common.LogError(logPrefix, "no ticker data for %s@%s", instId, exName)
			return false
		}
	}

	for instId, index := range e.instIdIndexs {
		tickers := local.LoadTickers(t0, t1, exName, instId, func(i, n int) {
			tracker.Increment(1.0 / float64(n))
		})
		for _, t := range tickers {
			m := marketInfoUnit{instIdIndex: index, time: t.Time, data: t}
			e.marketInfoSeq = append(e.marketInfoSeq, m)
		}
	}

	return true
}

func (e *Executor) loadDepths(t0, t1 time.Time, exName common.ExName, tracker *terminal.TrackerF) bool {
	validInstIds := local.GetValidDepthInstIds(exName)
	for _, instId := range e.instIds {
		if slices.Contains(validInstIds, instId) {
			if tmin, tmax, ok := local.GetValidDepthTimeRange(exName, instId); ok {
				if tmin.After(t0) || tmax.Before(t1) {
					common.LogError(logPrefix, "not enough depth data for %s@%s", instId, exName)
					return false
				}
			} else {
				common.LogError(logPrefix, "get depth time range failed for %s@%s", instId, exName)
				return false
			}
		} else {
			common.LogError(logPrefix, "no depth data for %s@%s", instId, exName)
			return false
		}
	}

	for instId, index := range e.instIdIndexs {
		depths := local.LoadDepth(t0, t1, exName, instId, func(i, n int) {
			tracker.Increment(3.0 / float64(n))
		})
		for _, d := range depths {
			m := marketInfoUnit{instIdIndex: index, time: d.Time, data: d}
			e.marketInfoSeq = append(e.marketInfoSeq, m)
		}
	}

	return true
}

func (e *Executor) loadTrades(t0, t1 time.Time, exName common.ExName, tracker *terminal.TrackerF) bool {
	validInstIds := local.GetValidTradesInstIds(exName)
	for _, instId := range e.instIds {
		if slices.Contains(validInstIds, instId) {
			if tmin, tmax, ok := local.GetValidTradesTimeRange(exName, instId); ok {
				if tmin.After(t0) || tmax.Before(t1) {
					common.LogError(logPrefix, "not enough trades data for %s@%s", instId, exName)
					return false
				}
			} else {
				common.LogError(logPrefix, "get trades time range failed for %s@%s", instId, exName)
				return false
			}
		} else {
			common.LogError(logPrefix, "no trades data for %s@%s", instId, exName)
			return false
		}
	}

	for instId, index := range e.instIdIndexs {
		trades := local.LoadTrades(t0, t1, exName, instId, func(i, n int) {
			tracker.Increment(1.0 / float64(n))
		})
		for _, t := range trades {
			t.Tag = common.TradeTagNormal
			m := marketInfoUnit{instIdIndex: index, time: t.Time, data: t}
			e.marketInfoSeq = append(e.marketInfoSeq, m)
		}
	}

	return true
}

// 跟trade不同，有可能某些日期没有对应的爆仓数据，因此不做检查，仅做加载
func (e *Executor) loadLiquidations(t0, t1 time.Time, exName common.ExName, tracker *terminal.TrackerF) bool {
	for instId, index := range e.instIdIndexs {
		trades := local.LoadLiquidation(t0, t1, exName, instId, func(i, n int) {
			tracker.Increment(1.0 / float64(n))
		})
		for _, t := range trades {
			t.Tag = common.TradeTagLiquidation
			m := marketInfoUnit{instIdIndex: index, time: t.Time, data: t}
			e.marketInfoSeq = append(e.marketInfoSeq, m)
		}
	}

	return true
}

func (e *Executor) loadKlines(t0, t1 time.Time, exName common.ExName, klineIntervalSec int, tracker *terminal.TrackerF) bool {
	validInstIdsByInterval := local.GetValidKlineInstIds(exName)
	for _, instId := range e.instIds {
		if validInstIds, ok := validInstIdsByInterval[klineIntervalSec]; ok {
			if slices.Contains(validInstIds, instId) {
				if tmin, tmax, ok := local.GetValidKlineTimeRange(exName, instId, klineIntervalSec); ok {
					if tmin.After(t0) || tmax.Before(t1) {
						common.LogError(logPrefix, "not enough kline data for %s@%s", instId, exName)
						return false
					}
				} else {
					common.LogError(logPrefix, "get kline time range failed for %s@%s", instId, exName)
					return false
				}
			} else {
				common.LogError(logPrefix, "no kline data for %s@%s", instId, exName)
				return false
			}
		} else {
			common.LogError(logPrefix, "invalid kline interval %d for %s@%s", klineIntervalSec, instId, exName)
			return false
		}
	}

	for instId, index := range e.instIdIndexs {
		kl := local.LoadKLine(t0, t1, exName, instId, klineIntervalSec, func(i, n int) {
			tracker.Increment(1.0 / float64(n))
		})
		for _, ku := range kl.Units {
			m := marketInfoUnit{instIdIndex: index, time: ku.Time, data: ku}
			e.marketInfoSeq = append(e.marketInfoSeq, m)
		}
	}

	return true
}
