我们把整个回测逻辑拆分成两部分
一部分叫executor，一部分叫strategy

executor负责很多事情：
1、记录仓位/资产/盈亏
2、根据strategy要求的品种的行情类型，加载并驱动行情（目前固定由local模块负责提供数据）
3、驱动strategy，提供其需要的数据
4、根据接受strategy输出的交易信号，执行交易，修改仓位和资产
这一部分为固定逻辑，不随策略变化而变化

strategy的职责比较单纯：
接受行情输入，提供交易信号输出
stragegy由executor驱动
也应该可以由实盘驱动（实盘也可以看作是一种executor）