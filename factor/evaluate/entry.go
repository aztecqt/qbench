/*
- @Author: aztec
- @Date: 2024-01-15 09:51:41
- @Description: 因子评估
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package evaluate

import (
	"fmt"
	"math"
	"slices"

	"github.com/aztecqt/bench/common"
	"github.com/aztecqt/dagger/util"
)

// 对应alphalens的get_clean_factor_and_forward_returns
// 输入因子值与价格数据，对其进行数据清理（暂无）、分组（暂无）、分层、以及未来收益率计算
// 返回预处理结果。如果处理失败，msg为失败原因
func Preprocess(factors []common.SectionData, prices []common.SectionData, cfg PrepConfig) (pRstSeq PrepResultSeq, ok bool, msg string) {
	// 首先验证factors数据和prices数据
	if len(factors) == 0 || len(prices) == 0 {
		// 空数据情况
		ok = false
		msg = "no factor or prices data"
		return
	}

	if len(factors) != len(prices) {
		// 长度不匹配的情况
		ok = false
		msg = "length mismatch"
		return
	}

	ls := len(factors) // 截面数量
	for i := 0; i < ls; i++ {
		if factors[i].Time != prices[i].Time {
			// 时间未对齐的情况
			ok = false
			msg = fmt.Sprintf("time not match at index %d, tFactor=%s, tPrice=%s", i, factors[i].Time, prices[i].Time)
			return
		}
	}

	instIds := prices[0].InstIds
	li := len(instIds) // 品种数量
	pricesAndFactors := slices.Clone(prices)
	pricesAndFactors = append(pricesAndFactors, factors...)
	for _, p := range pricesAndFactors {
		if !p.Valid() {
			// price或者factor自身invalid
			ok = false
			msg = "invalid section"
			return
		}

		if len(p.InstIds) != len(instIds) {
			// 品种数量不一致的情况
			ok = false
			msg = "instIds count mismatch"
			return
		}

		for i := 0; i < li; i++ {
			if p.InstIds[i] != instIds[i] {
				// 同索引的品种不一致的情况
				ok = false
				msg = "instId mismatch"
				return
			}
		}
	}

	// 长度一致、时间对齐、价格类型一致，说明数据完全ok，后续无需再做任何错误检查
	// 接下来按照时间，逐一进行预处理
	// i=截面索引
	pRstSeq = PrepResultSeq{Periods: cfg.Periods}
	for i := 0; i < ls; i++ {
		pr := PrepResult{
			Details: make([]PrepResultDetailOfUnit, li),
			Periods: cfg.Periods,
		}
		fct := factors[i]
		pr.Time = fct.Time
		pr.InstIds = fct.InstIds

		// 对品种做第一轮遍历，填写基础信息、计算未来收益
		// j=品种索引
		for j, fvalue := range fct.Values {
			detail := PrepResultDetailOfUnit{InstId: fct.InstIds[j], FactorValue: fvalue}
			for _, period := range pr.Periods {
				// 未来索引
				iFuture := i + period
				if iFuture < ls {
					// 未来索引在范围内，可以计算未来收益率
					px0 := prices[i].Values[j]
					px1 := prices[iFuture].Values[j]
					detail.ForwardReturns[period] = (px1 - px0) / px0
				} else {
					// 未来索引超标
					detail.ForwardReturns[period] = math.NaN()
				}
			}
			pr.Details[j] = detail
		}

		// 计算分层
		// 先将本截面中所有detail组织成slice
		details := make([]*PrepResultDetailOfUnit, 0, len(pr.Details))
		for i := range pr.Details {
			details = append(details, &pr.Details[i])
		}

		// 对其进行排序
		slices.SortFunc(details, func(a, b *PrepResultDetailOfUnit) int {
			if a.FactorValue < b.FactorValue {
				return -1
			} else if a.FactorValue > b.FactorValue {
				return 1
			} else {
				return 0
			}
		})

		// 对排序后的detail，每人分配一个分位序号
		step := float64(cfg.Quantiles)/float64(len(details)) + 0.00001
		accQuantile := 0.0
		for i := range details {
			details[i].Quantile = int(accQuantile)
			accQuantile += step
		}

		// 本截面处理完成
		pRstSeq.Data = append(pRstSeq.Data, pr)
	}

	// 预处理全部完成
	ok = true
	msg = "ok"
	return
}

// 对应alphalens的create_information_tear_sheet/create_returns_tear_sheet
// 对预处理过的数据，进行IC、收益分析
// 生成分析结果页面，并自动展示
func Analysis(pRstSeq PrepResultSeq) AnalysisResultSeq {
	aRstSeq := AnalysisResultSeq{Periods: pRstSeq.Periods}
	aRstSeq.Data = make([]AnalysisResult, len(pRstSeq.Data))

	// 计算ic值
	for _, pr := range pRstSeq.Data {
		ar := AnalysisResult{Time: pr.Time, Periods: pr.Periods}
		lp := len(pr.Periods)
		ar.ICs = make([]float64, lp)

		// 先收集所有品种的因子值和各持仓周期的未来收益率
		factorValues := []float64{}
		forwardReturns := make([][]float64, lp)
		for _, pru := range pr.Details {
			factorValues = append(factorValues, pru.FactorValue)
			for i, v := range pru.ForwardReturns {
				forwardReturns[i] = append(forwardReturns[i], v)
			}
		}

		// 因子值与未来收益率之间的Spearman相关系数即为ic值
		for i := 0; i < lp; i++ {
			ar.ICs[i] = util.SpearmanCorr(factorValues, forwardReturns[i])
		}

		aRstSeq.Data = append(aRstSeq.Data, ar)
	}

	return aRstSeq
}
