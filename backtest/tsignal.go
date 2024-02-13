/*
- @Author: aztec
- @Date: 2024-02-01 10:30:49
- @Description: 交易信号
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package backtest

import "github.com/shopspring/decimal"

// 交易信号接收器
// 实现了这个接口的对象，就可以响应strategy发出的交易信号
type tradeSignalReceiver interface {
	// 吃单信号
	onSignalTaker(instId string, price, amount decimal.Decimal, isSell bool)
}
