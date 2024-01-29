/*
- @Author: aztec
- @Date: 2024-01-24 11:19:53
- @Description:
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package local

import (
	"fmt"
	"time"

	"github.com/aztecqt/bench/common"
	"github.com/aztecqt/dagger/util"
)

// 查询本地成交的可用instId
func GetValidTradesInstIds(ex common.ExName) []string {
	dir := fmt.Sprintf("%s/trades/%s", LocalDataPath, ex)
	return GetInstIdsOfDir(dir)
}

// 查询本地成交的时间范围
func GetValidTradesTimeRange(ex common.ExName, instId string) (t0, t1 time.Time, ok bool) {
	dir := fmt.Sprintf("%s/trades/%s/%s", LocalDataPath, ex, instId)
	return GetTimeRangeOfDir(dir)
}

// 加载成交
func LoadTrades(t0, t1 time.Time, ex common.ExName, instId string) []common.Trade {
	dt0 := util.DateOfTime(t0)
	dt1 := util.DateOfTime(t1)
	trades := []common.Trade{}
	for d := dt0; d.Unix() <= dt1.Unix(); d = d.AddDate(0, 0, 1) {
		path := fmt.Sprintf("%s/trades/%s/%s/%s.trades", LocalDataPath, ex, instId, d.Format(time.DateOnly))
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
	}

	return trades
}
