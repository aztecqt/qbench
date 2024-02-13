/*
- @Author: aztec
- @Date: 2024-01-31 10:19:32
- @Description: 策略执行器，提供行情，管理仓位资产，接受并执行交易信号
- @
- @Copyright (c) 2024 by aztec, All Rights Reserved.
*/
package backtest

import (
	"fmt"
	"maps"
	"os/exec"
	"time"

	"github.com/aztecqt/dagger/util"
	"github.com/aztecqt/dagger/util/datavisual"
	"github.com/aztecqt/dagger/util/terminal"
	"github.com/aztecqt/qbench/common"
	"github.com/aztecqt/qbench/data/local"
	"github.com/shopspring/decimal"
)

const logPrefix = "backtest"

type ExecutorConfig struct {
	// 图表的时间粒度
	ChartsIntervalMs int64 `json:"charts_interval_ms"`
	chartsInterval   time.Duration

	// 手续费率
	FeeSpotMaker     decimal.Decimal `json:"fee_spot_maker"`
	FeeSpotTaker     decimal.Decimal `json:"fee_spot_taker"`
	FeeContractMaker decimal.Decimal `json:"fee_contract_maker"`
	FeeContractTaker decimal.Decimal `json:"fee_contract_taker"`
}

func (e *ExecutorConfig) parse() {
	e.chartsInterval = time.Millisecond * time.Duration(e.ChartsIntervalMs)
}

func ExecutorConfigDefault() ExecutorConfig {
	return ExecutorConfig{
		ChartsIntervalMs: 1000 * 60}
}

type Executor struct {
	cfg ExecutorConfig

	// 行情序列
	// 在行情加载阶段，我们把所有类型的行情，按照时间顺序，塌缩到一个一维的时间序列上，比如
	// [品种A盘口]-[品种A成交]-[品种B成交]-[品种B盘口]-[品种B盘口]...
	// 这样在运行阶段，只要遍历这个时间序列即可
	marketInfoSeq                                             []marketInfoUnit
	useTicker, useDepth, useTrades, useLiquidations, useKline bool
	pxbyTicker, pxbyDepth, pxbyTrades, pxbyKline              bool

	// 行情品种（采用通用instId语法）
	instIds      []string
	instIdIndexs map[string]int

	// 当前时间
	time.Time

	// 初始资产
	initBalance map[string]decimal.Decimal

	// 资产余额 btc->0.1
	balance map[string]decimal.Decimal

	// 浮动盈亏
	unrealizedPnl map[string]decimal.Decimal

	// 合约持仓，key=instId
	positions map[string]*common.ContractPosition

	// 各品种当前盘口数据
	depthOfInsts map[string]common.Depth

	// 各品种的最新价格
	priceOfInsts map[string]decimal.Decimal

	// 数据可视化
	dgDefault         *datavisual.DataGroup
	dgNextRefreshTime time.Time
}

func NewExecutor(localDataPath string, cfg ExecutorConfig) *Executor {
	local.Init(localDataPath)
	cfg.parse()
	e := &Executor{
		cfg:           cfg,
		balance:       map[string]decimal.Decimal{},
		unrealizedPnl: map[string]decimal.Decimal{},
		positions:     map[string]*common.ContractPosition{},
		depthOfInsts:  map[string]common.Depth{},
		priceOfInsts:  map[string]decimal.Decimal{},
		dgDefault:     datavisual.NewDataGroup(cfg.ChartsIntervalMs)}
	return e
}

// 设置资产数量（一般用来初始化）
func (e *Executor) SetBalance(ccy string, amount decimal.Decimal) {
	e.balance[ccy] = amount
}

// 使用行情驱动策略和自身
func (e *Executor) Run(s strategy) {
	tracker := terminal.GenTrackerWithHardwareInfo("回测", float64(len(e.marketInfoSeq)), 30, true, false, true, true, false)

	// 记录初始资产
	e.initBalance = maps.Clone(e.balance)

	// 可视数据初始化
	e.initVisualData(s)

	for i, miu := range e.marketInfoSeq {
		tracker.SetValue(float64(i))
		e.Time = miu.time
		instId := e.instIds[miu.instIdIndex]

		if e.useTicker {
			if v, ok := miu.data.(common.Ticker); ok {
				// 刷新当前价格、浮盈
				if e.pxbyTicker {
					e.onLatestPrice(instId, v.Price, v.Time)
				}

				// ticker代替深度
				if !e.useDepth {
					e.depthOfInsts[instId] = common.NewDepthFromTicker(v)
				}

				// 驱动策略
				s.OnTicker(instId, v, e)
			}
		}

		if e.useDepth {
			if v, ok := miu.data.(common.Depth); ok {
				// 刷新深度
				e.depthOfInsts[instId] = v

				// 刷新当前价格、浮盈
				if e.pxbyDepth {
					e.onLatestPrice(instId, v.Mid, v.Time)
				}

				// 驱动策略
				s.OnDepth(instId, v, e)
			}
		}

		if e.useTrades || e.useLiquidations {
			if v, ok := miu.data.(common.Trade); ok {
				if v.Tag == common.TradeTagNormal {
					// 刷新当前价格、浮盈
					if e.pxbyTrades {
						e.onLatestPrice(instId, v.Price, v.Time)
					}

					// 驱动策略
					s.OnTrade(instId, v, e)
				} else if v.Tag == common.TradeTagLiquidation {

					// 驱动策略
					s.OnLiquidation(instId, v, e)
				}
			}
		}

		if e.useLiquidations {
			if v, ok := miu.data.(common.Trade); ok {
				// 驱动策略
				s.OnLiquidation(instId, v, e)
			}
		}

		if e.useKline {
			if v, ok := miu.data.(common.KlineUnit); ok {
				// 刷新当前价格、浮盈
				if e.pxbyKline {
					e.onLatestPrice(instId, v.ClosePrice, v.Time)
				}

				// 驱动策略
				s.OnKlineUnit(instId, v, e)
			}
		}

		// 可视化数据刷新
		e.refreshVisualData(s)
	}

	// 可视化数据保存
	e.saveVisualData(s)

	tracker.MarkAsDone()
	time.Sleep(time.Millisecond * 100)
}

// 计算当前单位净值
// 目前仅能计算单一初始币种的情况
// 将当前所有资产，折算成目标资产数量，然后计算单位净值
func (e *Executor) nav() decimal.Decimal {
	if len(e.initBalance) >= 1 {
		// 初始资产类型和数量
		baseCcy := ""
		baseBal := decimal.Zero
		for k, d := range e.initBalance {
			baseCcy = k
			baseBal = d
			break
		}

		if baseBal.IsZero() {
			return util.DecimalOne
		}

		// 将所有资产折算成初始资产数量
		currBal := decimal.Zero
		for ccy, amount := range e.balance {
			currBal = currBal.Add(e.exchangeToCcy(ccy, baseCcy, amount))
		}

		for ccy, amount := range e.unrealizedPnl {
			currBal = currBal.Add(e.exchangeToCcy(ccy, baseCcy, amount))
		}

		return currBal.Div(baseBal)
	} else {
		return util.DecimalOne
	}
}

func (e *Executor) exchangeToCcy(srcCcy, dstCcy string, amount decimal.Decimal) decimal.Decimal {
	if srcCcy == dstCcy {
		return amount
	}

	if px, ok := e.priceOfInsts[fmt.Sprintf("%s_%s", srcCcy, dstCcy)]; ok {
		return amount.Mul(px)
	} else if px, ok := e.priceOfInsts[fmt.Sprintf("%s_%s", dstCcy, srcCcy)]; ok {
		return amount.Div(px)
	}

	return decimal.Zero
}

// 模拟现货买入
func (e *Executor) spotBuy(baseCcy, quoteCcy string, price, amount decimal.Decimal, taker bool) {
	// 修改资产数量
	quoteAmount := price.Mul(amount)
	fee := amount.Mul(util.ValueIf(taker, e.cfg.FeeSpotTaker, e.cfg.FeeSpotMaker))
	amount = amount.Sub(fee)
	e.balance[baseCcy] = e.balance[baseCcy].Add(amount)
	e.balance[quoteCcy] = e.balance[quoteCcy].Sub(quoteAmount)

	// 记录成交
	instId := fmt.Sprintf("%s_%s", baseCcy, quoteCcy)
	e.dgDefault.RecordPoint(instId, datavisual.Point{Time: e.Time, Value: price.InexactFloat64(), Tag: datavisual.PointTag_Buy})
}

// 模拟现货卖出
func (e *Executor) spotSell(baseCcy, quoteCcy string, price, amount decimal.Decimal, taker bool) {
	// 修改资产数量
	quoteAmount := price.Mul(amount)
	fee := quoteAmount.Mul(util.ValueIf(taker, e.cfg.FeeSpotTaker, e.cfg.FeeSpotMaker))
	quoteAmount = quoteAmount.Sub(fee)
	e.balance[baseCcy] = e.balance[baseCcy].Sub(amount)
	e.balance[quoteCcy] = e.balance[quoteCcy].Add(quoteAmount)

	// 记录成交
	instId := fmt.Sprintf("%s_%s", baseCcy, quoteCcy)
	e.dgDefault.RecordPoint(instId, datavisual.Point{Time: e.Time, Value: price.InexactFloat64(), Tag: datavisual.PointTag_Sell})
}

// 模拟合约交易。amount正数表示买入，负数表示卖出
func (e *Executor) contractDeal(instId string, price, amount decimal.Decimal, taker bool) {
	// 找出持仓对象
	if _, ok := e.positions[instId]; !ok {
		ct := common.NewContractPosition(
			e.cfg.FeeContractMaker,
			e.cfg.FeeContractTaker,
			common.IsUsdtContract(instId),
			false,
			common.InstId2MarginCcy(instId))
		e.positions[instId] = ct
	}

	// 模拟交易
	ct := e.positions[instId]
	fee, profit := ct.Deal(price, amount, taker, e.Time, nil)

	// 修改余额（手续费）
	e.balance[ct.MarginCcy] = e.balance[ct.MarginCcy].Add(profit.Sub(fee))

	// 记录成交
	e.dgDefault.RecordPoint(
		instId,
		datavisual.Point{
			Time:  e.Time,
			Value: price.InexactFloat64(),
			Tag: util.ValueIf(
				amount.IsPositive(),
				datavisual.PointTag_Buy,
				datavisual.PointTag_Sell)})
}

// 刷新最新价格
func (e *Executor) onLatestPrice(instId string, price decimal.Decimal, time time.Time) {
	// 刷新价格
	e.priceOfInsts[instId] = price

	// 刷新浮盈
	if pos, ok := e.positions[instId]; ok {
		pos.Update(price)
		e.unrealizedPnl[pos.MarginCcy] = pos.UnRealizedProfit
	}
}

// 可视化数据初始化
func (e *Executor) initVisualData(s strategy) {
	s.OnVisualDataInit(e.cfg.ChartsIntervalMs, e)
}

// 可视化数据刷新
func (e *Executor) refreshVisualData(s strategy) {
	if e.Time.After(e.dgNextRefreshTime) {
		// strategy层面处理
		s.OnVisualDataRefeshing(e.dgDefault, e)

		// 下次采样时间，对齐到下一个时间片的开始
		e.dgNextRefreshTime = util.AlignTime(e.Time, e.cfg.ChartsIntervalMs).Add(e.cfg.chartsInterval)
	}
}

// 生成可视化数据
func (e *Executor) saveVisualData(s strategy) {
	var lcDefault *datavisual.LayoutConfig

	// 生成extraInfo
	t0 := e.marketInfoSeq[0].time
	t1 := e.marketInfoSeq[len(e.marketInfoSeq)-1].time
	e.dgDefault.SaveExtraInfo(fmt.Sprintf("起始时间：%s\r\n", t0.Format(time.DateTime)))
	e.dgDefault.SaveExtraInfo(fmt.Sprintf("结束时间：%s\r\n", t1.Format(time.DateTime)))
	e.dgDefault.SaveExtraInfo(fmt.Sprintf("总时长：%s\r\n", util.Duration2Str(t1.Sub(t0))))
	e.dgDefault.SaveExtraInfo(fmt.Sprintf("单位净值：%.4f\r\n", e.nav().InexactFloat64()))

	// 数据保存目录
	rootDir := fmt.Sprintf("./visual/%s/%s", s.Class(), time.Now().Format("2006-01-02.15-04-05"))

	// strategy层面处理
	s.OnVisualDataSaving(rootDir, &lcDefault, e)

	// 存储可视化数据，并展示
	defaultDgDir := fmt.Sprintf("%s/default", rootDir)
	e.dgDefault.SaveToDir(defaultDgDir)
	lcDefault.SaveToDir(defaultDgDir)
	datavisual.GenerateLayoutGroupConfig(rootDir)
	cmd := exec.Command("CommonDataViewer.exe", rootDir)
	go cmd.Run()
}

// 生成颜色表（ccy-color + instId-color)
func (e *Executor) generateColorTable() map[string]datavisual.Color {
	colors := map[string]datavisual.Color{}

	for _, instId := range e.instIds {
		baseCcy, quoteCcy := common.InstId2Ccys(instId)
		colors[baseCcy] = datavisual.Color{}
		colors[quoteCcy] = datavisual.Color{}
	}

	colorGroup := datavisual.NewColorGroup(len(colors), 0.8, 0.8)
	i := 0
	for k := range colors {
		colors[k] = colorGroup.Colors[i]
		i++
	}

	for _, instId := range e.instIds {
		baseCcy, _ := common.InstId2Ccys(instId)
		colors[instId] = colors[baseCcy]
	}

	return colors
}
