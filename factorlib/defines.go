/*
- @Author: aztec
- @Date: 2024-01-18 10:20:19
- @Description: 因子库的数据定义
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package factorlib

import (
	"time"

	"github.com/aztecqt/dagger/util"
	"github.com/aztecqt/dagger/util/influxdb"
	"github.com/aztecqt/qbench/common"
)

var logPrefix = "factorlib"

type LaunchConfig struct {
	// 因子库名称
	Name string `json:"name"`

	// http端口
	HttpPort int `json:"port"`

	// redis主要用来存储metadata、更新状态等数据
	RedisCfg util.RedisConfig `json:"redis"`

	// influx中以时间、品种名、因子名为key，存储所有因子的value
	InfluxCfg influxdb.ConnConfig `json:"influx"`

	// 交易所名称。用来创建基础信息源
	ExName common.ExName `json:"ex"`

	// 时间窗口大小（秒），需要匹配dcommon.Bar中的某一种间隔
	WindowSize int `json:"win_sz"`

	// 循环更新间隔。多长时间更新一次全量数据
	RoundIntervalSec int `json:"round_interval_sec"`

	// 品种更新间隔。两个品种的更新之间留多少间隔
	InstIntervalSec int `json:"inst_interval_sec"`

	// 起始时间。首次刷新时以此为起始时间。非首次刷新时，以上次的时间为起始时间
	StartTime time.Time `json:"start_time"`
}
