/*
- @Author: aztec
- @Date: 2024-01-31 17:54:50
- @Description: 成交记录
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package backtest

import (
	"time"

	"github.com/shopspring/decimal"
)

type deal struct {
	time   time.Time
	price  decimal.Decimal
	amount decimal.Decimal
	isSell bool
}
