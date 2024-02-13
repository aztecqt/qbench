/*
- @Author: aztec
- @Date: 2024-02-01 15:43:48
- @Description: 实现context接口
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package backtest

import (
	"time"

	"github.com/aztecqt/qbench/common"
	"github.com/shopspring/decimal"
)

func (e *Executor) GetTime() time.Time {
	return e.Time
}

func (e *Executor) GetBalance(ccy string) (decimal.Decimal, bool) {
	if v, ok := e.balance[ccy]; ok {
		return v, true
	} else {
		return decimal.Zero, false
	}
}

func (e *Executor) GetPosition(instId string) (amount decimal.Decimal, avgPrice decimal.Decimal) {
	if pos, ok := e.positions[instId]; ok {
		amount = pos.Position
		avgPrice = pos.PositionAvgPriceOpen
	} else {
		amount = decimal.Zero
		avgPrice = decimal.Zero
	}
	return
}

func (e *Executor) GetLatestPrice(instId string) (decimal.Decimal, bool) {
	if v, ok := e.priceOfInsts[instId]; ok {
		return v, true
	} else {
		return decimal.Zero, false
	}
}

func (e *Executor) GetDepth(instId string) (common.Depth, bool) {
	if v, ok := e.depthOfInsts[instId]; ok {
		return v, true
	} else {
		return common.Depth{}, false
	}
}

func (e *Executor) SignalTaker(instId string, price, amount decimal.Decimal, isSell bool) {
	// 如果有盘口数据，先按照盘口深度，计算出最大交易量，对amount进行剪裁，然后计算真实成交价格和真实成交数量
	// 如果没有盘口数据，则跳过这一步
	if v, ok := e.depthOfInsts[instId]; ok {
		maxAmount := v.GetMaxAmount(price, isSell)
		if maxAmount.LessThan(amount) {
			amount = maxAmount
		}

		// 根据数量，反算成交价格
		price, amount = v.GetAvgPrice(amount, isSell)
	}

	// 执行交易
	if common.GetInstType(instId) == common.InstType_Spot {
		baseCcy, quoteCcy := common.InstId2Ccys(instId)
		if isSell {
			e.spotSell(baseCcy, quoteCcy, price, amount, true)
		} else {
			e.spotBuy(baseCcy, quoteCcy, price, amount, true)
		}
	} else {
		if isSell {
			amount = amount.Neg()
		}
		e.contractDeal(instId, price, amount, true)
	}
}
