/*
- @Author: aztec
- @Date: 2024-01-16 15:45:41
- @Description: 获取一组品种在一段时间内的价格序列
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package data

import (
	"fmt"
	"time"

	"github.com/aztecqt/dagger/stratergy"
	"github.com/aztecqt/qbench/common"
	"github.com/aztecqt/qbench/data/local"
	"github.com/aztecqt/qbench/data/online"
)

// 针对一组标的，获取中心化交易所的价格数据
// 所有数据基于k线，最多可以取到1分钟数据
// instId全部采用通用instId格式
func GetPricesFromCEx(exName common.ExName, instType common.InstType, instIds []string, t0, t1 time.Time, intervalSec int, allowOnline bool) (priceSecSeq common.SectionSequence, ok bool, msg string) {
	logPrefix := "GetPricesFromCEx"

	// step 1: 先尝试取出所有原始价格
	dlPrices := map[string]*stratergy.DataLine{}
	for _, instId := range instIds {
		if v, ok := common.ToCommonInstId(exName, instType, instId); ok {
			instId = v
		} else {
			common.LogError(logPrefix, "convert %s to common inst id failed", instId)
			continue
		}

		// 尝试从本地加载
		validRange := local.GetValidKlineBarsAndTimeRange(exName, instId)
		selectedInterval := 0
		n := 0
		for itvl, tms := range validRange {
			if intervalSec%itvl == 0 && tms[0].Unix() <= t0.Unix() && tms[len(tms)-1].Unix() >= t1.Unix() {
				if itvl > selectedInterval {
					selectedInterval = itvl
					n = intervalSec / itvl
				}
			}
		}

		var kl *common.KLine
		if selectedInterval > 0 && n > 0 {
			// 尝试加载本地数据（本地数据采用通用instId）
			kl = local.LoadKLine(t0, t1, exName, instId, selectedInterval)
			kl = local.LoadKLine(t0, t1, exName, instId, selectedInterval)
			if kl == nil {
				common.LogNormal(logPrefix, "load %s(%s) local data failed", instId, string(exName))
			} else {
				common.LogNormal(logPrefix, "load %s(%s) local data successed, %d loaded", instId, string(exName), len(kl.Units))

				if kl != nil {
					dl := &stratergy.DataLine{}
					dl.Init("", 0, int64(intervalSec)*1000, 0)
					for i := 0; i < len(kl.Units); i += n {
						dl.Update(kl.Units[i].Time.UnixMilli(), kl.Units[i].OpenPrice)
					}
					dlPrices[instId] = dl
				}
			}
		}

		if kl == nil && allowOnline {
			// 尝试加载在线数据（在线数据需要做instId转换，所以需要instType参与）
			kl = online.LoadKline(t0, t1, exName, instType, instId, intervalSec)
			if kl == nil {
				common.LogNormal(logPrefix, "load %s(%s) online data failed", instId, string(exName))
			} else {
				common.LogNormal(logPrefix, "load %s(%s) online data successed, %d loaded", instId, string(exName), len(kl.Units))
			}

			if kl != nil {
				dl := &stratergy.DataLine{}
				dl.Init("", 0, int64(intervalSec)*1000, 0)
				for i := 0; i < len(kl.Units); i++ {
					dl.Update(kl.Units[i].Time.UnixMilli(), kl.Units[i].OpenPrice)
				}
				dlPrices[instId] = dl
			}
		}
	}

	if len(dlPrices) == 0 {
		ok = false
		msg = "no price loaded"
		common.LogError(logPrefix, msg)
		return
	}

	// step 2: 检查原始价格是否可以对齐
	if len(dlPrices) > 0 {
		n := 0
		ts0 := int64(0)
		for instId, dl := range dlPrices {
			if n == 0 {
				n = dl.Length()
				ts0 = dl.Times[0]
			} else {
				if dl.Length() != n || dl.Times[0] != ts0 {
					ok = false
					msg = fmt.Sprintf("%s price data-line mismatch", instId)
					common.LogError(logPrefix, msg)
					return
				}
			}
		}
	}

	// step 3: 价格对齐无误，将纵向的原始价格合并为横向的截面序列
	priceSecSeq = common.SectionSequence{}

	// step 3.1: 先构建instId
	instIds = []string{}
	for k := range dlPrices {
		instIds = append(instIds, k)
	}
	priceSecSeq.InstIds = instIds

	// step 3.2: 构建sections
	iSections := 0
	finished := false
	for !finished {
		s := common.SectionData{}
		iInst := 0
		for i, instId := range instIds {
			dl := dlPrices[instId]
			if i == 0 {
				// 长度检测
				if iSections >= dl.Length() {
					finished = true
					break
				}

				// section公共数据
				s.InstIds = instIds
				s.Time = time.UnixMilli(dl.Times[iSections])
				s.Values = make([]float64, len(dlPrices))
			}

			// section各品种价格
			s.Values[iInst] = dl.Values[iSections]
			iInst++
		}

		if !finished {
			priceSecSeq.Data = append(priceSecSeq.Data, s)
			iSections++
		}
	}

	if priceSecSeq.Valid() {
		ok = true
		msg = "ok"
		return
	} else {
		ok = false
		msg = "invalid section sequence"
		return
	}
}
