/*
- @Author: aztec
- @Date: 2024-01-15 11:44:19
- @Description:
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package evaluate

import "time"

// 因子预处理配置参数
// Prep=PreProcessing
// （alphalens还有很多很多参数，但是很少用到，所以这里暂时仅支持一小部分）
type PrepConfig struct {
	Quantiles int   // 分多少个分位（等数量法）
	Periods   []int // 持仓周期。用来计算未来收益
}

// 默认参数
func NewPrepConfig() PrepConfig {
	return PrepConfig{Quantiles: 10, Periods: []int{1, 5, 10}}
}

// 因子预处理结果(单品种)
type PrepResultDetailOfUnit struct {
	InstId         string    // 品种
	FactorValue    float64   // 因子值
	ForwardReturns []float64 // 未来收益，对应Periods
	Quantile       int       // 所属分位[0~n]
}

// 因子预处理结果
type PrepResult struct {
	Time    time.Time
	InstIds []string                 // 品种
	Periods []int                    // 持仓周期，用来计算未来收益。单位由使用者自己定义
	Details []PrepResultDetailOfUnit // 品种-Detail
}

// 因子预处理结果序列
type PrepResultSeq struct {
	Periods []int        // 持仓周期
	Data    []PrepResult // 数据序列
}

// 一个横截面上的分析结果
type AnalysisResult struct {
	Time    time.Time
	Periods []int     // 持仓周期
	ICs     []float64 // 不同持仓周期上的IC值
}

// 分析结果序列
type AnalysisResultSeq struct {
	Periods []int            // 持仓周期
	Data    []AnalysisResult // 数据序列
}
