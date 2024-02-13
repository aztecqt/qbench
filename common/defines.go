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
	"math"
	"slices"
	"strings"
	"time"

	"github.com/aztecqt/dagger/util"
	"github.com/jedib0t/go-pretty/table"
	"github.com/shopspring/decimal"
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

// 是否为U本位合约
func IsUsdtContract(instId string) bool {
	return util.StringEndWith(instId, "_usdt_swap")
}

// 获取合约的保证金币种
func InstId2MarginCcy(instId string) string {
	if IsUsdtContract(instId) {
		return "usdt"
	} else {
		return strings.Split(instId, "_")[0]
	}
}

// 获取现货instId的交易币种
func InstId2Ccys(instId string) (baseCcy, quoteCcy string) {
	ss := strings.Split(instId, "_")
	return ss[0], ss[1]
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
	OpenPrice  decimal.Decimal
	ClosePrice decimal.Decimal
	HighPrice  decimal.Decimal
	LowPrice   decimal.Decimal
	Volume     decimal.Decimal
}

func (k *KlineUnit) Deserialize(r io.Reader) bool {
	ts := int64(0)
	if e := binary.Read(r, binary.LittleEndian, &ts); e != nil {
		return false
	}
	k.Time = time.UnixMilli(ts)

	val := 0.0
	if e := binary.Read(r, binary.LittleEndian, &val); e != nil {
		return false
	}
	k.OpenPrice = decimal.NewFromFloat(val)

	if e := binary.Read(r, binary.LittleEndian, &val); e != nil {
		return false
	}
	k.ClosePrice = decimal.NewFromFloat(val)

	if e := binary.Read(r, binary.LittleEndian, &val); e != nil {
		return false
	}
	k.LowPrice = decimal.NewFromFloat(val)

	if e := binary.Read(r, binary.LittleEndian, &val); e != nil {
		return false
	}
	k.HighPrice = decimal.NewFromFloat(val)

	if e := binary.Read(r, binary.LittleEndian, &val); e != nil {
		return false
	}
	k.Volume = decimal.NewFromFloat(val)

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

// Ticker
type Ticker struct {
	InstrumentId string          `json:"inst"`
	TimeStamp    int64           `json:"ts"`
	Price        decimal.Decimal `json:"px"`
	Buy1         decimal.Decimal `json:"b1"`
	Sell1        decimal.Decimal `json:"s1"`
	Time         time.Time
}

func (t *Ticker) Serialize(w io.Writer) {
	binary.Write(w, binary.LittleEndian, t.TimeStamp)
	binary.Write(w, binary.LittleEndian, t.Price.InexactFloat64())
	binary.Write(w, binary.LittleEndian, t.Buy1.InexactFloat64())
	binary.Write(w, binary.LittleEndian, t.Sell1.InexactFloat64())
}

func (t *Ticker) Deserialize(r io.Reader) bool {
	if binary.Read(r, binary.LittleEndian, &t.TimeStamp) != nil {
		return false
	}

	val := 0.0
	if binary.Read(r, binary.LittleEndian, &val) != nil {
		return false
	}
	t.Price = decimal.NewFromFloat(val)

	if binary.Read(r, binary.LittleEndian, &val) != nil {
		return false
	}
	t.Buy1 = decimal.NewFromFloat(val)

	if binary.Read(r, binary.LittleEndian, &val) != nil {
		return false
	}
	t.Sell1 = decimal.NewFromFloat(val)

	t.Time = time.UnixMilli(t.TimeStamp)

	return true
}

// 订单簿
// 为了提升加载速度，这里使用float存储
// 运算时要注意精度
type DepthUnit struct {
	Price      decimal.Decimal
	Amount     decimal.Decimal
	OrderCount int16
}

type Depth struct {
	Time  time.Time
	Asks  []DepthUnit
	Bids  []DepthUnit
	Sell1 decimal.Decimal
	Buy1  decimal.Decimal
	Mid   decimal.Decimal
}

func NewDepthFromTicker(t Ticker) Depth {
	d := Depth{}
	d.Time = t.Time
	d.Asks = append(d.Asks, DepthUnit{Price: t.Sell1, Amount: decimal.NewFromInt32(math.MaxInt32), OrderCount: 1})
	d.Bids = append(d.Bids, DepthUnit{Price: t.Buy1, Amount: decimal.NewFromInt32(math.MaxInt32), OrderCount: 1})
	d.parse()
	return d
}

func (d *Depth) parse() bool {
	if len(d.Bids) > 0 && len(d.Asks) > 0 {
		d.Buy1 = d.Bids[0].Price
		d.Sell1 = d.Asks[0].Price
		d.Mid = d.Buy1.Add(d.Sell1).Div(util.DecimalTwo)
		return true
	} else {
		return false
	}
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
			val := 0.0
			if binary.Read(r, binary.LittleEndian, &val) != nil {
				return false
			}
			d.Asks[i].Price = decimal.NewFromFloat(val)

			if binary.Read(r, binary.LittleEndian, &val) != nil {
				return false
			}
			d.Asks[i].Amount = decimal.NewFromFloat(val)

			if binary.Read(r, binary.LittleEndian, &d.Asks[i].OrderCount) != nil {
				return false
			}

			if binary.Read(r, binary.LittleEndian, &val) != nil {
				return false
			}
			d.Bids[i].Price = decimal.NewFromFloat(val)

			if binary.Read(r, binary.LittleEndian, &val) != nil {
				return false
			}
			d.Bids[i].Amount = decimal.NewFromFloat(val)

			if binary.Read(r, binary.LittleEndian, &d.Bids[i].OrderCount) != nil {
				return false
			}
		}

		return d.parse()
	} else {
		return false
	}
}

// 查询盘口数量
func (d Depth) GetMaxAmount(price decimal.Decimal, isSell bool) decimal.Decimal {
	amount := decimal.Zero
	if isSell {
		for _, du := range d.Bids {
			if du.Price.GreaterThanOrEqual(price) {
				amount = amount.Add(du.Amount)
			}
		}
	} else {
		for _, du := range d.Asks {
			if du.Price.LessThanOrEqual(price) {
				amount = amount.Add(du.Amount)
			}
		}
	}

	return amount
}

// 预估成交价格
func (d Depth) GetAvgPrice(amount decimal.Decimal, isSell bool) (avgPrice, amountReal decimal.Decimal) {
	amountMulPrice := decimal.Zero
	amountReal = decimal.Zero

	dus := d.Asks
	if isSell {
		dus = d.Bids
	}

	for _, du := range dus {
		if du.Amount.GreaterThanOrEqual(amount) {
			amountMulPrice = amountMulPrice.Add(amount.Mul(du.Price))
			amountReal = amountReal.Add(amount)
			amount = decimal.Zero
			break
		} else {
			amountMulPrice = amountMulPrice.Add(du.Amount.Mul(du.Price))
			amountReal = amountReal.Add(du.Amount)
			amount = amount.Mul(du.Amount)
		}
	}

	if amountReal.IsPositive() {
		avgPrice = amountMulPrice.Div(amountReal)
	}
	return
}

type TradeTag int

const (
	TradeTagNormal TradeTag = iota
	TradeTagLiquidation
)

// 市场成交
type Trade struct {
	Time  time.Time
	Price decimal.Decimal
	Size  decimal.Decimal
	Side  byte // 'b','s'
	Tag   TradeTag
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

	val := 0.0
	if binary.Read(r, binary.LittleEndian, &val) != nil {
		return false
	}
	t.Price = decimal.NewFromFloat(val)

	if binary.Read(r, binary.LittleEndian, &val) != nil {
		return false
	}
	t.Size = decimal.NewFromFloat(val)

	if binary.Read(r, binary.LittleEndian, &t.Side) != nil {
		return false
	}

	return true
}

//
// #endregion
