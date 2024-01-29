/*
- @Author: aztec
- @Date: 2024-01-16 16:18:51
- @Description: 本地k线数据加载
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package local

import (
	"fmt"
	"os"
	"time"

	"github.com/aztecqt/bench/common"
	"github.com/aztecqt/dagger/util"
)

// 查询本地kline的可用InstIds
// 返回格式：interval-[]string
func GetValidKlineInstIds(ex common.ExName) map[int][]string {
	result := map[int][]string{}

	// 交易所目录
	exRoot := fmt.Sprintf("%s/klines/%s", LocalDataPath, ex)

	// 交易所目录下为所有可用bar
	if des, err := os.ReadDir(exRoot); err == nil {
		for _, de := range des {
			if de.IsDir() {
				bar := common.Bar(de.Name())
				if interval, ok := common.Bar2Interval(bar); ok {
					barPath := fmt.Sprintf("%s/%s", exRoot, bar)

					// barPath下为所有instId文件夹
					instIds := GetInstIdsOfDir(barPath)
					result[interval] = instIds
				}
			}
		}
	}

	return result
}

// 查询本地kline数据可选interval和时间范围
// 返回格式：interval-[t0, t1]
func GetValidKlineBarsAndTimeRange(ex common.ExName, instId string) map[int][]time.Time {
	result := map[int][]time.Time{}

	// 交易所目录
	exRoot := fmt.Sprintf("%s/klines/%s", LocalDataPath, ex)

	// 交易所目录下为所有可用bar
	if des, err := os.ReadDir(exRoot); err == nil {
		for _, de := range des {
			if de.IsDir() {
				bar := common.Bar(de.Name())
				if interval, ok := common.Bar2Interval(bar); ok {
					barPath := fmt.Sprintf("%s/%s/%s", exRoot, bar, instId)
					// barPath下为所有kline文件
					if t0, t1, ok := GetTimeRangeOfDir(barPath); ok {
						result[interval] = []time.Time{t0, t1}
					}
				}
			}
		}
	}

	return result
}

// 加载k线
func LoadKLine(t0, t1 time.Time, ex common.ExName, instId string, interval int) *common.KLine {
	if bar, ok := common.Interval2Bar(interval); ok {
		dt0 := util.DateOfTime(t0)
		dt1 := util.DateOfTime(t1)
		kline := &common.KLine{InstId: instId}
		for d := dt0; d.Unix() <= dt1.Unix(); d = d.AddDate(0, 0, 1) {
			path := fmt.Sprintf("%s/klines/%s/%s/%s/%s.kline", LocalDataPath, ex, bar, instId, d.Format(time.DateOnly))
			if bf, err := LoadZipOrRawFile(path); err == nil {
				util.DeserializeToObjects(
					bf,
					func() *common.KlineUnit { return &common.KlineUnit{} },
					func(ku *common.KlineUnit) bool {
						if ku.Time.Unix() >= t0.Unix() && ku.Time.Unix() <= t1.Unix() {
							kline.Units = append(kline.Units, *ku)
						}
						return ku.Time.Unix() < t1.Unix()
					})
			}
		}
		return kline
	} else {
		return nil
	}
}
