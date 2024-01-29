/*
- @Author: aztec
- @Date: 2024-01-29 10:37:00
- @Description:
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package common

import (
	"fmt"
	"time"

	"github.com/aztecqt/dagger/cex/common"
	"github.com/aztecqt/dagger/util"
	"github.com/shopspring/decimal"
)

type ProfitRecord struct {
	Profit      decimal.Decimal // 本次平仓利润
	ProfitTotal decimal.Decimal // 总计利润
	Time        time.Time
}

// 模拟一个合约仓位，计算其盈利、手续费等
type ContractPosition struct {
	enableLog bool

	// U本位合约 or 币本位合约
	// U本位合约的保证金为USDT，仓位单位为币
	// 币本位合约保证金为币，仓位单位为USDT
	// 两种本位的收益计算方式也不同
	isUsdt    bool
	MarginCcy string

	FeeRateTaker          decimal.Decimal // Maker费率
	FeeRateMaker          decimal.Decimal // Taker费率
	TotalFee              decimal.Decimal // 总手续费支出，正数表示支出，负数表示收入
	Position              decimal.Decimal // 当前仓位
	PositionAvgPriceOpen  decimal.Decimal // 开仓均价
	PositionAvgPriceClose decimal.Decimal // 平仓均价
	RealizedProfit        decimal.Decimal // 已实现利润
	UnRealizedProfit      decimal.Decimal // 未实现利润
	UnRealizedProfitRatio decimal.Decimal // 未实现利润率
	BuyAmountTotal        decimal.Decimal // 累计买入数量
	SellAmountTotal       decimal.Decimal // 累计卖出数量
	BuyPriceAvg           decimal.Decimal // 整体买入均价
	SellPriceAvg          decimal.Decimal // 整体卖出均价
	TotalVolume           decimal.Decimal // 总成交量
	ClearCount            int             // 完全平仓次数
	ProfitRecords         []ProfitRecord  // 每次平仓后，记录仓前总利润
	maxPositionAbs        decimal.Decimal // 最大仓位数量
}

func NewContractPosition(feeRateMaker, feeRateTaker decimal.Decimal, isUsdt, enableLog bool, marginCcy string) *ContractPosition {
	c := new(ContractPosition)
	c.FeeRateMaker = feeRateMaker
	c.FeeRateTaker = feeRateTaker
	c.isUsdt = isUsdt
	c.enableLog = enableLog
	c.MarginCcy = marginCcy
	return c
}

// 收益计算，只能在平仓时调用
// openPx：开仓均价
// closePx：平仓价格
// amount：平仓数量
// 返回值：收益率、收益
// U本位合约收益计算方式：
//
//	平仓数量（币）*开仓均价=保证金数量（U）
//	多仓收益率 =（平仓均价-开仓均价）/ 开仓均价
//	空仓收益率 =（开仓均价-平仓均价）/ 开仓均价
//	收益率 * 保证金数量 = 利润（U）
//
// 币本位合约收益计算方式：
//
//	平仓数量（U）/ 开仓均价 = 保证金数量（币）
//	多仓收益率 = （平仓价格-开仓价格）/平仓价格
//	空仓收益率 = （开仓价格-平仓价格）/平仓均价
//	收益率 * 保证金数量 = 利润（币）
func (c *ContractPosition) calProfit(openPx, closePx decimal.Decimal, amount decimal.Decimal) (decimal.Decimal, decimal.Decimal) {
	if c.isUsdt {
		margin := amount.Mul(openPx)
		if amount.IsNegative() {
			// 平仓数量为负，说明为平多
			profitRate := closePx.Sub(openPx).Div(openPx)
			return profitRate, profitRate.Mul(margin)
		} else {
			// 平仓数量为正，说明为平空
			profitRate := openPx.Sub(closePx).Div(openPx)
			return profitRate, profitRate.Mul(margin)
		}
	} else {
		margin := amount.Div(openPx)
		if amount.IsNegative() {
			// 平仓数量为负，说明为平多
			profitRate := closePx.Sub(openPx).Div(closePx)
			return profitRate, profitRate.Mul(margin)
		} else {
			// 平仓数量为正，说明为平空
			profitRate := openPx.Sub(closePx).Div(closePx)
			return profitRate, profitRate.Mul(margin)
		}
	}
}

// 计算手续费
func (c *ContractPosition) calFee(price, amount decimal.Decimal, taker bool) decimal.Decimal {
	feeRate := util.ValueIf(taker, c.FeeRateTaker, c.FeeRateMaker)
	if c.isUsdt {
		return amount.Abs().Mul(price).Mul(feeRate)
	} else {
		return amount.Abs().Div(price).Mul(feeRate)
	}
}

// 计算一次成家发生后的持仓均价
// price: 成交价格
// amountAbs：成交数量
// priceAvg：成交之前的持仓均价
// positionAbs：成交之前的持仓数量
func calAvgPrice(price, amountAbs, priceAvg, positionAbs decimal.Decimal) decimal.Decimal {
	x := positionAbs.Add(amountAbs)
	y := decimal.Zero
	if positionAbs.IsPositive() && priceAvg.IsPositive() {
		y = positionAbs.Div(priceAvg)
	}
	z := amountAbs.Div(price)
	return x.Div(y.Add(z))
}

// 持仓方向
func (c *ContractPosition) PositionDir() common.OrderDir {
	if c.Position.IsPositive() {
		return common.OrderDir_Buy
	} else if c.Position.IsNegative() {
		return common.OrderDir_Sell
	} else {
		return common.OrderDir_None
	}
}

// 总收益
func (c *ContractPosition) TotalProfit() decimal.Decimal {
	return c.RealizedProfit.Add(c.UnRealizedProfit).Sub(c.TotalFee)
}

// 记录一次交易
// amount正数为买入，负数为卖出
// fnPosClear为完全平仓时的回调。一般不用传。
func (c *ContractPosition) Deal(price decimal.Decimal, amount decimal.Decimal, taker bool, t time.Time, fnPosClear func()) {
	amountAbs := amount.Abs()
	positionAbs := c.Position.Abs()
	if amount.IsPositive() && c.Position.IsPositive() || amount.IsNegative() && c.Position.IsNegative() {
		// 开仓情况，更新总仓位，开仓均价，最大仓位
		if c.Position.IsZero() {
			// 全新仓位
			c.maxPositionAbs = decimal.Zero
			c.PositionAvgPriceClose = decimal.Zero
			c.PositionAvgPriceOpen = decimal.Zero
		}

		// 计算开仓均价
		c.PositionAvgPriceOpen = calAvgPrice(price, amountAbs, c.PositionAvgPriceOpen, positionAbs)

		// 刷新当前仓位、最大持仓
		c.Position = c.Position.Add(amount)
		positionAbs = c.Position.Abs()
		if positionAbs.GreaterThan(c.maxPositionAbs) {
			c.maxPositionAbs = positionAbs
		}

		if c.enableLog {
			fmt.Printf("{%s}: open: deal {%v} at price {%v}, totalPosition={%v}, avgPrice={%v}\n", t.Format(time.DateTime), amount, price, c.Position, c.PositionAvgPriceOpen)
		}
	} else {
		// 平仓情况，更新总仓位，已实现利润，平仓均价
		if amountAbs.GreaterThan(positionAbs) {
			// 分两次计算
			amount0 := c.Position.Neg()
			amount1 := amount.Add(c.Position)
			c.Deal(price, amount0, taker, t, fnPosClear)
			c.Deal(price, amount1, taker, t, fnPosClear)
			return
		}

		// 此时amount的绝对值必然小于等于Position
		// 计算已实现盈亏
		openPrice := c.PositionAvgPriceOpen
		closePrice := price
		_, profit := c.calProfit(openPrice, closePrice, amount)
		c.RealizedProfit = c.RealizedProfit.Add(profit)

		// 计算平仓均价（这里稍微有点绕）
		// 当前仓位距离最大持仓的差，即为已平仓数量
		// 将平仓理解为另一种开仓，则已平仓数量即为当前持仓数量
		totalClosedAmountAbs := c.maxPositionAbs.Sub(positionAbs)
		c.PositionAvgPriceClose = calAvgPrice(price, amountAbs, c.PositionAvgPriceClose, totalClosedAmountAbs)

		posOrign := c.Position
		c.Position = c.Position.Add(amount)

		if posOrign.IsPositive() && c.Position.IsZero() || posOrign.IsNegative() && c.Position.IsZero() {
			// 达成一次平仓
			c.ClearCount++
			pr := ProfitRecord{Time: t, ProfitTotal: c.RealizedProfit}
			if len(c.ProfitRecords) == 0 {
				pr.Profit = pr.ProfitTotal
			} else {
				pr.Profit = pr.ProfitTotal.Sub(c.ProfitRecords[len(c.ProfitRecords)-1].ProfitTotal)
			}
			c.ProfitRecords = append(c.ProfitRecords, pr)

			if c.enableLog {
				fmt.Printf("%s: pft=%.v, total=%.v\n", pr.Time.Format(time.DateTime), pr.Profit, pr.ProfitTotal)
			}

			c.PositionAvgPriceClose = decimal.Zero
			c.PositionAvgPriceOpen = decimal.Zero

			if fnPosClear != nil {
				fnPosClear()
			}
		}

		if c.enableLog {
			//fmt.Printf("%s: close: deal {%v} at price {%v}, totalPosition={%v}, avgPrice={%v}, realized profit:{%v}\n", t.Format(time.DateTime), amount, price, c.Position, c.PositionAvgPriceOpen, profit)
		}
	}

	// 计算手续费
	c.TotalFee = c.TotalFee.Add(c.calFee(price, amount, taker))

	// 计算整体买入/卖出均价
	if amount.IsPositive() {
		c.BuyPriceAvg = calAvgPrice(price, amountAbs, c.BuyPriceAvg, c.BuyAmountTotal)
		c.BuyAmountTotal = c.BuyAmountTotal.Add(amountAbs)
	} else {
		c.SellPriceAvg = calAvgPrice(price, amountAbs, c.SellPriceAvg, c.SellAmountTotal)
		c.SellAmountTotal = c.SellAmountTotal.Add(amountAbs)
	}

	if c.isUsdt {
		c.TotalVolume = c.TotalVolume.Add(amountAbs.Mul(price))
	} else {
		c.TotalVolume = c.TotalVolume.Add(amountAbs.Div(price))
	}
}

// 根据当前价格，重新计算未实现盈亏
func (c *ContractPosition) Update(currentPrice decimal.Decimal) {
	openPrice := c.PositionAvgPriceOpen
	closePrice := currentPrice
	pr, pft := c.calProfit(openPrice, closePrice, c.Position.Neg())
	c.UnRealizedProfitRatio = pr
	c.UnRealizedProfit = pft
}

// 最近一次完整平仓的收益
func (c *ContractPosition) LastPositionProfit() decimal.Decimal {
	if len(c.ProfitRecords) == 0 {
		return decimal.Zero
	} else {
		return c.ProfitRecords[len(c.ProfitRecords)-1].Profit
	}
}
