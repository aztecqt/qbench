/*
- @Author: aztec
- @Date: 2024-02-01 11:08:06
- @Description: 策略接口
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package backtest

import (
	"time"

	"github.com/aztecqt/dagger/util/datavisual"
	"github.com/aztecqt/qbench/common"
	"github.com/shopspring/decimal"
)

// 策略
// 策略运行在一个上下文（context）环境中，这个context应该能提供资产、仓位、订单等数据的查询，同时能接受交易信号）
// 策略接受行情数据驱动后，结合上下文，给出交易信号
type strategy interface {
	// 基本信息
	Class() string

	// 行情驱动
	OnTicker(instId string, t common.Ticker, c Context)
	OnDepth(instId string, d common.Depth, c Context)
	OnTrade(instId string, t common.Trade, c Context)
	OnKlineUnit(instId string, k common.KlineUnit, c Context)
	OnLiquidation(instId string, t common.Trade, c Context)

	// 可视化数据的收集与保存
	OnVisualDataInit(intervalMs int64, c Context)
	OnVisualDataRefeshing(dgDefault *datavisual.DataGroup, c Context)
	OnVisualDataSaving(rootDir string, lcDefault **datavisual.LayoutConfig, c Context)
}

// 策略上下文
type Context interface {
	// 数据访问
	GetTime() time.Time
	GetBalance(ccy string) (decimal.Decimal, bool)
	GetPosition(instId string) (amount decimal.Decimal, avgPrice decimal.Decimal)
	GetLatestPrice(instId string) (decimal.Decimal, bool)
	GetDepth(instId string) (common.Depth, bool)

	// 交易信号输出
	SignalTaker(instId string, price, amount decimal.Decimal, isSell bool)
}
