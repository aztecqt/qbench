/*
- @Author: aztec
- @Date: 2024-01-31 10:30:29
- @Description: 一个最基本的行情单元
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package backtest

import "time"

type marketInfoUnit struct {
	// 为了减少开销，行情单元里不直接保存instId字符串，而是保存其索引
	// 因此需要配合instId表使用
	instIdIndex int

	// 时间
	time time.Time

	// 行情类型。可以是以下类型：
	// common.Ticker
	// common.KlineUnit
	// common.Trade
	// common.Depth
	// 使用时需要做动态类型断言
	data interface{}
}
