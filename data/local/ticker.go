/*
- @Author: aztec
- @Date: 2024-01-23 17:35:27
- @Description:ticker 的加载
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

// 查询本地ticker的可用InstId
func GetValidTickerInstIds(ex common.ExName) []string {
	dir := fmt.Sprintf("%s/tickers/%s", LocalDataPath, ex)
	return GetInstIdsOfDir(dir)
}

// 查询本地ticker的时间范围
func GetValidTickerTimeRange(ex common.ExName, instId string) (t0, t1 time.Time, ok bool) {
	dir := fmt.Sprintf("%s/tickers/%s/%s", LocalDataPath, ex, instId)
	return GetTimeRangeOfDir(dir)
}

// 加载tickers
func LoadTickers(t0, t1 time.Time, ex common.ExName, instId string) []common.Ticker {
	dt0 := util.DateOfTime(t0)
	dt1 := util.DateOfTime(t1)
	tickers := []common.Ticker{}
	for d := dt0; d.Unix() <= dt1.Unix(); d = d.AddDate(0, 0, 1) {
		path := fmt.Sprintf("%s/tickers/%s/%s/%s.ticker", LocalDataPath, ex, instId, d.Format(time.DateOnly))
		if bf, err := LoadZipOrRawFile(path); err == nil {
			util.DeserializeToObjects(
				bf,
				func() *common.Ticker { return &common.Ticker{} },
				func(tk *common.Ticker) bool {
					if tk.TimeStamp >= t0.UnixMilli() && tk.TimeStamp <= t1.UnixMilli() {
						tickers = append(tickers, *tk)
					}
					return tk.TimeStamp < t1.UnixMilli()
				})
		}
	}

	return tickers
}
