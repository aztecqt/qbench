/*
- @Author: aztec
- @Date: 2024-01-18 10:39:03
- @Description: 基础信息源。能提供一系列基础因子
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package basicinfo

import (
	"time"

	"github.com/aztecqt/bench/common"
	"github.com/go-gota/gota/dataframe"
)

type Source interface {
	// 支持的因子名。一般为open/close/high/low/volume
	FactorNames() []string

	// 支持的品种列表。以通用形式表达
	InstIds() []string

	// 返回某品种自某时间以来的所有数据，按指定的时间间隔对齐
	// dataFrame中的行为时间，列为因子名
	GetDataSince(instId string, intervalSec int, since time.Time) (*dataframe.DataFrame, bool)
}

func NewSource(exName common.ExName) Source {
	switch exName {
	case common.ExName_Okx:
		panic("okx not supported")
	case common.ExName_Binance:
		return NewSourceBn()
	default:
		panic("invalid exName")
	}
}
