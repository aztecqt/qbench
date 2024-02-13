/*
- @Author: aztec
- @Date: 2024-02-06 10:42:38
- @Description: 爆仓数据
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package local

import (
	"fmt"
	"time"

	"github.com/aztecqt/dagger/util"
	"github.com/aztecqt/qbench/common"
)

// 加载爆仓成交
func LoadLiquidation(t0, t1 time.Time, ex common.ExName, instId string, fnprg func(i, n int)) []common.Trade {
	dt0 := util.DateOfTime(t0)
	dt1 := util.DateOfTime(t1)
	trades := []common.Trade{}
	i := 0
	n := int(dt1.Sub(dt0).Hours()/24) + 1
	for d := dt0; d.Unix() <= dt1.Unix(); d = d.AddDate(0, 0, 1) {
		path := fmt.Sprintf("%s/liquidation/%s/%s/%s.trades", LocalDataPath, ex, instId, d.Format(time.DateOnly))
		if bf, err := LoadZipOrRawFile(path); err == nil {
			util.DeserializeToObjects(
				bf,
				func() *common.Trade { return &common.Trade{} },
				func(dp *common.Trade) bool {
					if dp.Time.UnixMilli() >= t0.UnixMilli() && dp.Time.UnixMilli() <= t1.UnixMilli() {
						trades = append(trades, *dp)
					}
					return dp.Time.UnixMilli() < t1.UnixMilli()
				})
		}

		i++
		if fnprg != nil {
			fnprg(i, n)
		}
	}

	return trades
}
