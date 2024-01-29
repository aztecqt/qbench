/*
- @Author: aztec
- @Date: 2024-01-18 16:30:45
- @Description:
- @因子库。从内部看，是一个负责行情获取、因子重新计算并写入数据库的苦力。
- @从外部看，是一个http服务，提供因子计算支持，因子查询，元数据查询等功能
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package factorlib

import (
	"time"

	"github.com/aztecqt/dagger/util"
	"github.com/aztecqt/dagger/util/influxdb"
	"github.com/aztecqt/qbench/factorlib/basicinfo"
	"github.com/influxdata/influxdb/client/v2"
)

type FactorLib struct {
	// 配置
	lc *LaunchConfig

	// 数据库连接
	rc *util.RedisClient
	ic client.Client

	// 基础信息
	basicSrc basicinfo.Source

	// 更新频率控制
	lastRoundUpdateStartTime time.Time
	lastInstUpdateStartTime  time.Time

	// 全品种
	instIds   []string
	instIndex int
}

func NewFactorLib(lc *LaunchConfig) *FactorLib {
	f := &FactorLib{lc: lc}

	// 创建数据库连接
	f.rc = &util.RedisClient{}
	f.rc.InitFromConfig(lc.RedisCfg)
	f.ic = influxdb.CreateConn(lc.InfluxCfg)

	// 创建基础因子源
	f.basicSrc = basicinfo.NewSource(lc.ExName)
	if f.basicSrc == nil {
		panic("create basic src failed")
	}

	// TODO：启动http服务

	return f
}

func (f *FactorLib) Run() {
	go func() {
		// 按配置的节奏，反复更新所有数据
		for {
			now := time.Now()
			if now.Unix()-f.lastRoundUpdateStartTime.Unix() > int64(f.lc.RoundIntervalSec) {
				f.lastRoundUpdateStartTime = now

				// 新一轮更新
				f.instIds = f.basicSrc.InstIds()
				f.instIndex = 0
				for f.instIndex < len(f.instIds) {
					now = time.Now()
					if now.Unix()-f.lastInstUpdateStartTime.Unix() > int64(f.lc.InstIntervalSec) {
						f.refreshInst(f.instIds[f.instIndex])
						f.instIndex++
					}
					time.Sleep(time.Second)
				}
			}
			time.Sleep(time.Second)
		}
	}()
}

// 更新一个品种
func (f *FactorLib) refreshInst(instId string) {

}
