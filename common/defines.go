/*
- @Author: aztec
- @Date: 2024-01-18 16:00:09
- @Description: 通用数据定义
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package common

import (
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/aztecqt/dagger/util"
	"github.com/jedib0t/go-pretty/table"
)

type FnLog func(format string, args ...interface{})

var logNormal FnLog
var logError FnLog

func Init(fnLogNormal, fnLogError FnLog) {
	logNormal = fnLogNormal
	logError = fnLogError
}

func LogNormal(prefix, format string, args ...interface{}) {
	if logNormal != nil {
		logNormal(fmt.Sprintf("[%s] %s", prefix, format), args...)
	}
}

func LogError(prefix, format string, args ...interface{}) {
	if logError != nil {
		logError(fmt.Sprintf("[%s] %s", prefix, format), args...)
	}
}

// 交易所名称
type ExName string

const (
	ExName_Okx     ExName = "okx"
	ExName_Binance        = "binance"
)

// inst类型
type InstType string

const (
	InstType_Spot   InstType = "spot"
	InstType_UmSwap          = "umswap"
	InstType_CmSwap          = "cmswap"
)

func CommonInstType2Okx(st InstType) (string, bool) {
	switch st {
	case InstType_Spot:
		return "SPOT", true
	case InstType_CmSwap:
		return "SWAP", true
	case InstType_UmSwap:
		return "SWAP", true
	default:
		return "", false
	}
}

// 交易所InstId（symbol）转为通用instId
// 通用instId格式：
// 现货：btc_usdt(仅支持usdt交易对)
// U本位永续：btc_usdt_swap
// 币本位永续：btc_usd_swap
func ToCommonInstId(exName ExName, instType InstType, instId string) (string, bool) {
	if exName == ExName_Okx {
		return strings.ToLower(strings.ReplaceAll(instId, "-", "_")), true
	} else if exName == ExName_Binance {
		if instType == InstType_Spot {
			// 仅支持usdt交易对
			if util.StringEndWith(instId, "USDT") {
				return strings.ToLower(strings.ReplaceAll(instId, "USDT", "")) + "_usdt", true
			} else {
				return "", false
			}
		} else if instType == InstType_UmSwap {
			return strings.ToLower(strings.ReplaceAll(instId, "USDT", "")) + "_usdt_swap", true
		} else if instType == InstType_CmSwap {
			return strings.ToLower(strings.ReplaceAll(instId, "USD_PERP", "_usd_swap")), true
		} else {
			return "", false
		}
	} else {
		return "", false
	}
}

// 通用instId转为交易所instId
func ToExchangeInstId(exName ExName, instId string) (string, bool) {
	if exName == ExName_Okx {
		return strings.ToUpper(strings.ReplaceAll(instId, "_", "-")), true
	} else if exName == ExName_Binance {
		if util.StringEndWith(instId, "_usdt") {
			// 仅支持usdt交易对的现货
			return strings.ToUpper(strings.ReplaceAll(instId, "_", "")), true
		} else if util.StringEndWith(instId, "_usdt_swap") {
			return strings.ToUpper(strings.ReplaceAll(instId, "_usdt_swap", "USDT")), true
		} else if util.StringEndWith(instId, "_usd_swap") {
			return strings.ToUpper(strings.ReplaceAll(instId, "_usd_swap", "USD_PERP")), true
		} else {
			return "", false
		}
	} else {
		return "", false
	}
}

// 通用InstId转InstType
func GetInstType(instId string) InstType {
	if util.StringEndWith(instId, "_usd_swap") {
		return InstType_CmSwap
	} else if util.StringEndWith(instId, "_usdt_swap") {
		return InstType_UmSwap
	} else {
		return InstType_Spot
	}
}

// 截面数据
// 某一时刻各个品种的某一数据
type SectionData struct {
	Time    time.Time // 时间
	InstIds []string  // 品种
	Values  []float64 // 值。长度与Insts相同。可以代表价格、因子值等数据
}

func (s SectionData) Valid() bool {
	if len(s.InstIds) != len(s.Values) {
		return false
	}
	return true
}

func (s SectionData) ToTable() table.Writer {
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.SetAutoIndex(true)
	t.SetTitle(s.Time.Format(time.DateTime))
	t.AppendHeader(table.Row{"InstId", "Value"})
	l := len(s.InstIds)
	for i := 0; i < l; i++ {
		t.AppendRow(table.Row{s.InstIds[i], s.Values[i]})
	}
	return t
}

// 截面序列
// 截面序列中的data，共享相同的InstIds，数量和顺序都需要一致
type SectionSequence struct {
	InstIds []string
	Data    []SectionData
}

func (s SectionSequence) Valid() bool {
	for _, sd := range s.Data {
		if !sd.Valid() {
			return false
		}

		if slices.Compare(sd.InstIds, s.InstIds) != 0 {
			return false
		}
	}

	return true
}

// 单行数据太多时，最多显示n列
func (s SectionSequence) ToTable(n int) table.Writer {
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.SetAutoIndex(true)

	l := len(s.InstIds)
	overlen := l > n
	header := table.Row{"time"}
	for i := 0; i < l && i < n; i++ {
		header = append(header, s.InstIds[i])
	}
	if overlen {
		header = append(header, fmt.Sprintf("%d more...", l-n))
	}
	t.AppendHeader(header)

	for _, sd := range s.Data {
		row := table.Row{sd.Time.Format(time.DateTime)}
		for i := 0; i < l && i < n; i++ {
			row = append(row, sd.Values[i])
		}
		t.AppendRow(row)
	}

	return t
}

// #region from MarketCollector
// k线单位(跟MarketCollector保持一致)
type KlineUnit struct {
	Time       time.Time
	OpenPrice  float64
	ClosePrice float64
	HighPrice  float64
	LowPrice   float64
	Volume     float64
}

func (k *KlineUnit) Deserialize(r io.Reader) bool {
	ts := int64(0)
	if e := binary.Read(r, binary.LittleEndian, &ts); e != nil {
		return false
	}
	k.Time = time.UnixMilli(ts)

	if e := binary.Read(r, binary.LittleEndian, &k.OpenPrice); e != nil {
		return false
	}

	if e := binary.Read(r, binary.LittleEndian, &k.ClosePrice); e != nil {
		return false
	}

	if e := binary.Read(r, binary.LittleEndian, &k.LowPrice); e != nil {
		return false
	}

	if e := binary.Read(r, binary.LittleEndian, &k.HighPrice); e != nil {
		return false
	}

	if e := binary.Read(r, binary.LittleEndian, &k.Volume); e != nil {
		return false
	}

	return true
}

// k线
type KLine struct {
	InstId string
	Units  []KlineUnit
}

type Bar string

const (
	Bar_1m      Bar = "1m"
	Bar_5m          = "5m"
	Bar_15m         = "15m"
	Bar_30m         = "30m"
	Bar_1h          = "1h"
	Bar_2h          = "2h"
	Bar_4h          = "4h"
	Bar_8h          = "8h"
	Bar_1d          = "1d"
	Bar_Invalid     = ""
)

// bar
var interval2Bar = map[int]Bar{60: Bar_1m, 300: Bar_5m, 900: Bar_15m, 1800: Bar_30m, 3600: Bar_1h, 7200: Bar_2h, 14400: Bar_4h, 28800: Bar_8h, 86400: Bar_1d}
var bar2Interval = map[Bar]int{Bar_1m: 60, Bar_5m: 300, Bar_15m: 900, Bar_30m: 1800, Bar_1h: 3600, Bar_2h: 7200, Bar_4h: 14400, Bar_8h: 28800, Bar_1d: 86400}

func Interval2Bar(interval int) (Bar, bool) {
	if v, ok := interval2Bar[interval]; ok {
		return v, true
	} else {
		return Bar_Invalid, false
	}
}

func Bar2Interval(bar Bar) (int, bool) {
	if v, ok := bar2Interval[bar]; ok {
		return v, true
	} else {
		return 0, false
	}
}

type Ticker struct {
	InstrumentId string  `json:"inst"`
	TimeStamp    int64   `json:"ts"`
	Price        float64 `json:"px"`
	Buy1         float64 `json:"b1"`
	Sell1        float64 `json:"s1"`
}

func (t *Ticker) Serialize(w io.Writer) {
	binary.Write(w, binary.LittleEndian, t.TimeStamp)
	binary.Write(w, binary.LittleEndian, t.Price)
	binary.Write(w, binary.LittleEndian, t.Buy1)
	binary.Write(w, binary.LittleEndian, t.Sell1)
}

func (t *Ticker) Deserialize(r io.Reader) bool {
	if binary.Read(r, binary.LittleEndian, &t.TimeStamp) != nil {
		return false
	}

	if binary.Read(r, binary.LittleEndian, &t.Price) != nil {
		return false
	}

	if binary.Read(r, binary.LittleEndian, &t.Buy1) != nil {
		return false
	}

	if binary.Read(r, binary.LittleEndian, &t.Sell1) != nil {
		return false
	}

	return true
}

// 订单簿
type DepthUnit struct {
	Price      float64
	Amount     float64
	OrderCount int16
}

type Depth struct {
	Time time.Time
	Asks []DepthUnit
	Bids []DepthUnit
}

func (d Depth) Serialize(w io.Writer) bool {
	if len(d.Asks) != len(d.Bids) {
		fmt.Println("depth length not match")
		return false
	}

	binary.Write(w, binary.LittleEndian, d.Time.UnixMilli())

	l := int8(len(d.Asks))
	binary.Write(w, binary.LittleEndian, l)
	for i := 0; i < int(l); i++ {
		binary.Write(w, binary.LittleEndian, d.Asks[i].Price)
		binary.Write(w, binary.LittleEndian, d.Asks[i].Amount)
		binary.Write(w, binary.LittleEndian, d.Asks[i].OrderCount)
		binary.Write(w, binary.LittleEndian, d.Bids[i].Price)
		binary.Write(w, binary.LittleEndian, d.Bids[i].Amount)
		binary.Write(w, binary.LittleEndian, d.Bids[i].OrderCount)
	}
	return true
}

func (d *Depth) Deserialize(r io.Reader) bool {
	ms := int64(0)
	if binary.Read(r, binary.LittleEndian, &ms) != nil {
		return false
	}
	d.Time = time.UnixMilli(ms)

	l := int8(0)
	if binary.Read(r, binary.LittleEndian, &l) == nil {
		d.Asks = make([]DepthUnit, l)
		d.Bids = make([]DepthUnit, l)
		for i := 0; i < int(l); i++ {
			if binary.Read(r, binary.LittleEndian, &d.Asks[i].Price) != nil {
				return false
			}

			if binary.Read(r, binary.LittleEndian, &d.Asks[i].Amount) != nil {
				return false
			}

			if binary.Read(r, binary.LittleEndian, &d.Asks[i].OrderCount) != nil {
				return false
			}

			if binary.Read(r, binary.LittleEndian, &d.Bids[i].Price) != nil {
				return false
			}

			if binary.Read(r, binary.LittleEndian, &d.Bids[i].Amount) != nil {
				return false
			}

			if binary.Read(r, binary.LittleEndian, &d.Bids[i].OrderCount) != nil {
				return false
			}
		}
		return true
	} else {
		return false
	}
}

// 市场成交
type Trade struct {
	Time  time.Time
	Price float64
	Size  float64
	Side  byte // 'b','s'
}

func (t Trade) Serialize(w io.Writer) bool {
	binary.Write(w, binary.LittleEndian, t.Time.UnixMilli())
	binary.Write(w, binary.LittleEndian, t.Price)
	binary.Write(w, binary.LittleEndian, t.Size)
	binary.Write(w, binary.LittleEndian, t.Side)
	return true
}

func (t *Trade) Deserialize(r io.Reader) bool {
	ms := int64(0)
	if binary.Read(r, binary.LittleEndian, &ms) != nil {
		return false
	}
	t.Time = time.UnixMilli(ms)

	if binary.Read(r, binary.LittleEndian, &t.Price) != nil {
		return false
	}

	if binary.Read(r, binary.LittleEndian, &t.Size) != nil {
		return false
	}

	if binary.Read(r, binary.LittleEndian, &t.Side) != nil {
		return false
	}

	return true
}

//
// #endregion
