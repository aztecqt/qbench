/*
- @Author: aztec
- @Date: 2024-01-24 09:15:54
- @Description: 深度数据的加载
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

// 查询本地深度的可用instId
func GetValidDepthInstIds(ex common.ExName) []string {
	dir := fmt.Sprintf("%s/depth/%s", LocalDataPath, ex)
	return GetInstIdsOfDir(dir)
}

// 查询本地深度的时间范围
func GetValidDepthTimeRange(ex common.ExName, instId string) (t0, t1 time.Time, ok bool) {
	dir := fmt.Sprintf("%s/depth/%s/%s", LocalDataPath, ex, instId)
	return GetTimeRangeOfDir(dir)
}

// 加载深度
// 填写cacheGroup则本地保存解压后的缓存，以提升速度
func LoadDepth(t0, t1 time.Time, ex common.ExName, instId string, fnprg func(i, n int)) []common.Depth {
	dt0 := util.DateOfTime(t0)
	dt1 := util.DateOfTime(t1)
	depths := []common.Depth{}
	i := 0
	n := int(dt1.Sub(dt0).Hours()/24) + 1
	for d := dt0; d.Unix() <= dt1.Unix(); d = d.AddDate(0, 0, 1) {
		path := fmt.Sprintf("%s/depth/%s/%s/%s.depth", LocalDataPath, ex, instId, d.Format(time.DateOnly))
		if bf, err := LoadZipOrRawFile(path); err == nil {
			util.DeserializeToObjects(
				bf,
				func() *common.Depth { return &common.Depth{} },
				func(dp *common.Depth) bool {
					if dp.Time.UnixMilli() >= t0.UnixMilli() && dp.Time.UnixMilli() <= t1.UnixMilli() {
						depths = append(depths, *dp)
					}
					return dp.Time.UnixMilli() < t1.UnixMilli()
				})
		}

		i++
		if fnprg != nil {
			fnprg(i, n)
		}
	}

	return depths
}
