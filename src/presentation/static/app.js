const state = {
  me: null,
  registration: {
    enabled: true,
    max_users: 200,
    registered_users: 0,
    remaining_slots: 200,
    limit_reached: false,
  },
  runtime: null,
  allSignals: [],
  signals: [],
  signalPage: {
    limit: Number(localStorage.getItem('signals.pageSize') || 100),
    offset: 0,
    total: 0,
    hasMore: false,
  },
  marketSnapshots: {},
  selectedSignalId: null,
  globalStocks: [],
  myStocks: [],
  defaultRules: {},
  myRules: {},
  forwardMetrics: {
    '1m_horizon_minutes': 20,
    '5m_horizon_minutes': 60,
  },
  notifications: {},
  users: [],
  templates: [],
  backtests: [],
  backtestDetail: null,
  backtestConfig: {
    timeframes: ['1m', '5m'],
    criteria: {
      '1m': { horizon_minutes: 15, min_move_pct: 0.5 },
      '5m': { horizon_minutes: 60, min_move_pct: 1.5 },
    },
  },
  analytics: {},
  chartBars: [],
  chartSignals: [],
  chartView: {
    mode: localStorage.getItem('chart.mode') || 'candles',
    showVwap: localStorage.getItem('chart.showVwap') !== '0',
    collapsed: localStorage.getItem('chart.collapsed') === '1',
    visibleCount: Number(localStorage.getItem('chart.visibleCount') || 80),
    startIndex: 0,
    dragOriginX: 0,
    dragOriginStart: 0,
    dragging: false,
    key: '',
  },
  language: localStorage.getItem('app.language') || 'zh',
};

const translations = {
  zh: {
    documentTitle: '日内趋势雷达',
    'brand.title': '日内趋势雷达',
    'session.logout': '退出登录',
    'nav.monitor': '监控面板',
    'nav.config': '配置中心',
    'nav.admin': '用户管理',
    'nav.labs': '策略实验室',
    'actions.refresh': '刷新',
    'actions.applyConfig': '应用配置',
    'actions.manual': '使用说明',
    'actions.github': 'GitHub 仓库',
    'login.title': '登录日内趋势雷达',
    'login.subtitle': '默认管理员账号由初始化配置生成。',
    'login.username': '用户名',
    'login.password': '密码',
    'login.submit': '登录',
    'register.title': '快速注册',
    'register.username': '用户名',
    'register.password': '密码',
    'register.submit': '注册',
    'register.meta': '开放注册中，请填写用户名和密码创建普通用户。',
    'register.limitReached': '已达到普通用户注册上限 {max}，请联系管理员。',
    'register.disabled': '当前未开放自助注册，请联系管理员开通账号。',
    'register.watchlistHint': '每位用户当前最多只能维护 1 支自选股票。',
    'register.success': '注册成功，请使用刚刚创建的账号登录。',
    'register.invalid': '请输入用户名和密码。',
    'monitor.title': '监控面板',
    'monitor.subtitle': '实时查看信号、K 线与运行状态。',
    'config.title': '配置中心',
    'config.subtitle': '管理股票池、规则覆盖和通知方式。',
    'admin.title': '用户管理',
    'admin.subtitle': '只有管理员可见，可创建账号、禁用账号并重置密码。',
    'labs.title': '策略实验室',
    'labs.subtitle': '保存模板、提交回测、查看分析。',
    'monitor.runtime': '运行状态',
    'monitor.refreshRuntime': '刷新状态',
    'monitor.chartParams': '图表参数',
    'monitor.refreshChart': '更新图表',
    'monitor.chartModeCandles': 'K线',
    'monitor.chartModeLine': '曲线',
    'monitor.showVwap': '显示 VWAP',
    'monitor.resetChart': '重置视图',
    'monitor.collapseChart': '收起图表',
    'monitor.expandChart': '展开图表',
    'monitor.mySignals': '我的信号',
    'monitor.signalFilter': '关键词过滤',
    'monitor.allStocks': '全部股票',
    'monitor.allTimeframes': '全部周期',
    'monitor.signalDetail': '信号详情',
    'monitor.clickSignal': '点击左侧信号查看详情',
    'monitor.chartHint': '当前图表：{symbol} · {tf} · {bars} 根 K 线',
    'monitor.chartHintEmpty': '请选择股票并刷新图表',
    'monitor.chartGuide': '滚轮缩放，拖动画布平移。',
    'monitor.indicatorsCount': '{count} 个同向指标',
    'monitor.currentPrice': '现价',
    'monitor.forwardMetrics': '{minutes}m后 最高 +{up}% · 最低 -{down}% · 收盘 {final}%',
    'monitor.signalPageSummary': '第 {page} / {pages} 页 · 共 {total} 条',
    'monitor.signalPageSize': '每页',
    'monitor.noSignals': '暂无信号',
    'monitor.noChartData': '暂无图表数据',
    'monitor.noRuntimeEvents': '暂无运行事件',
    'monitor.runtimeError': '最近错误: {error}',
    'monitor.runtimeHint': '股票 {symbols} / 策略档案 {profiles}',
    'monitor.scope.global': '全局',
    'monitor.scope.user': '个人',
    'runtime.running': '运行中',
    'runtime.idle': '空闲',
    'runtime.offline': '离线',
    'runtime.stats.activeSymbols': '活跃股票',
    'runtime.stats.subscriptions': '订阅数',
    'runtime.stats.profiles': '策略档案',
    'runtime.stats.warmupBars': '预热 K 线',
    'config.globalStocks': '全局股票池',
    'config.myStocks': '我的自选池',
    'config.stockNamePlaceholder': '股票名称',
    'config.addGlobalStock': '添加全局股票',
    'config.addMyStock': '添加自选股票',
    'config.myStockLimit': '每位普通用户当前最多只能维护 1 支自选股票。',
    'config.myStockLimitAdmin': '管理员账号可维护不限数量的自选股票。',
    'config.defaultRules': '默认规则',
    'config.myRuleOverrides': '我的规则覆盖',
    'config.saveRules': '保存规则覆盖',
    'config.forwardMetrics': '信号后评估窗口',
    'config.forwardMetricsHint': '新出现的信号会按这里的分钟数记录最大涨幅、最大跌幅和最终涨跌幅。',
    'config.forwardMetric1m': '1m 信号窗口(分钟)',
    'config.forwardMetric5m': '5m 信号窗口(分钟)',
    'config.saveForwardMetrics': '保存评估窗口',
    'config.notifications': '通知配置',
    'config.notifyNone': '关闭所有通知',
    'config.notifyLocal': '仅本地通知',
    'config.notifyTelegram': '仅 Telegram',
    'config.notifyBoth': '本地 + Telegram',
    'config.telegramAdminOnly': 'Telegram 仅管理员可配置',
    'config.bellOnAlert': '提示时响铃',
    'config.saveNotifications': '保存通知设置',
    'config.noGlobalStocks': '暂无全局股票',
    'config.noMyStocks': '暂无自选股票',
    'common.enabled': '启用',
    'common.disabled': '停用',
    'common.all': '全部',
    'common.createdAt': '创建于 {time}',
    'common.notAvailable': '暂无数据',
    'common.passwordUpdated': '密码已更新',
    'common.saved': '已保存',
    'common.usernameTaken': '用户名已存在，请换一个。',
    'common.registrationLimitReached': '已达到注册上限。',
    'common.watchlistLimitReached': '每位普通用户最多只能维护 1 支自选股票。',
    'common.accountEnabled': '账号已启用',
    'common.accountDisabled': '账号已禁用',
    'common.cannotDisableSelf': '不能禁用当前登录的管理员账号',
    'common.role': '角色',
    'common.status': '状态',
    'common.prev': '上一页',
    'common.next': '下一页',
    'admin.createUser': '创建用户',
    'admin.usernamePlaceholder': '用户名',
    'admin.passwordPlaceholder': '初始密码',
    'admin.createUserAction': '创建用户',
    'admin.userList': '用户列表',
    'admin.registrationSummary': '普通用户上限 {max} · 已注册 {registered} · 剩余 {remaining}',
    'admin.noUsers': '暂无用户',
    'admin.resetPassword': '修改密码',
    'admin.newPassword': '新密码',
    'admin.disableUser': '禁用账号',
    'admin.enableUser': '启用账号',
    'admin.selfTag': '当前账号',
    'labs.ruleTemplates': '策略模板',
    'labs.templateName': '模板名',
    'labs.templateDescription': '描述',
    'labs.saveTemplate': '保存模板',
    'labs.localBacktest': '本地回测',
    'labs.submitBacktest': '提交回测',
    'labs.analytics': '分析概览',
    'labs.noTemplates': '暂无模板',
    'labs.noBacktests': '暂无回测任务',
    'labs.passCriteria': '测试通过条件',
    'labs.useTimeframes': '回测周期',
    'labs.criteria1m': '1m 条件',
    'labs.criteria5m': '5m 条件',
    'labs.horizonMinutes': '观察窗口(分钟)',
    'labs.minMovePct': '最小波动(%)',
    'labs.latestBacktest': '最近一次回测',
    'labs.latestBacktestEmpty': '提交回测后，这里会展示信号表格和 summary。',
    'labs.backtestSignals': '触发信号',
    'labs.backtestSummary': '回测 Summary',
    'labs.filterTimeframe': '周期筛选',
    'labs.filterResult': '结果筛选',
    'labs.filterDirection': '方向筛选',
    'labs.backtestNoResults': '本次回测没有触发任何信号。',
    'labs.noFilteredSignals': '当前筛选条件下没有匹配的信号。',
    'labs.backtestPending': '回测任务已提交，正在等待结果。',
    'labs.backtestOverall': '整体结果',
    'labs.backtestByTimeframe': '分周期结果',
    'labs.table.time': '时间',
    'labs.table.timeframe': '周期',
    'labs.table.rule': '规则',
    'labs.table.signal': '信号内容',
    'labs.table.maxUp': '最大上涨%',
    'labs.table.maxDown': '最大回撤%',
    'labs.table.threshold': '通过阈值%',
    'labs.table.horizon': '观察窗口',
    'labs.table.result': '结果',
    'labs.result.pass': '通过',
    'labs.result.fail': '未通过',
    'labs.result.all': '全部结果',
    'labs.direction.all': '全部方向',
    'labs.backtestEvaluated': '信号数',
    'labs.backtestEffective': '通过数',
    'labs.backtestHitRate': '通过率',
    'labs.backtestAvgUp': '平均最大上涨%',
    'labs.backtestAvgDown': '平均最大回撤%',
    'labs.backtestSymbolPlaceholder': '请选择股票',
    'labs.noBacktestSymbols': '请先在配置中心添加股票，再进行本地回测',
    'labs.backtestSymbolRequired': '请选择要回测的股票',
    'labs.backtestDateRequired': '请选择回测日期',
    'labs.backtestTimeframeRequired': '请至少选择一个回测周期',
    'labs.backtestError': '失败原因: {error}',
    'labs.backtestStatus.queued': '排队中',
    'labs.backtestStatus.running': '运行中',
    'labs.backtestStatus.done': '已完成',
    'labs.backtestStatus.failed': '失败',
    'rule.open_range_breakout': '开盘区间突破',
    'rule.vwap_deviation': 'VWAP 偏离',
    'rule.squeeze_breakout': '波动压缩后放大',
    'rule.rsi_overbought': 'RSI 超买',
    'rule.rsi_oversold': 'RSI 超卖',
    'rule.break_retest': '突破回踩确认',
    'rule.volume_price_divergence': '量价背离',
    'rule.prev_day_break': '昨日高低点突破',
    'direction.up': '向上',
    'direction.down': '向下',
    'direction.neutral': '中性',
    'summary.timeframes': '周期',
    'events.level.info': '信息',
    'events.level.error': '错误',
    'events.runtime_started.title': '监控已启动',
    'events.runtime_started.body': '已订阅 {symbols} 支股票，监控 {timeframes} 个周期，等待新 K 线。',
    'events.subscriptions_updated.title': '订阅已刷新',
    'events.subscriptions_updated.body': '已重新应用配置，当前订阅 {symbols} 支股票。',
    'events.config_applied.title': '配置已应用',
    'events.config_applied.body': '股票 {symbols} 支 · 周期 {timeframes} 个 · 策略档案 {profiles} 份',
    'events.warmup_completed.title': '预热完成',
    'events.warmup_completed.body': '本次共预热 {bars} 根 K 线，用于初始化指标状态。',
    'events.warmup_symbol_failed.title': '个股预热已跳过',
    'events.warmup_symbol_failed.body': '{message}',
    'events.config_apply_failed.title': '配置应用失败',
    'events.config_apply_failed.opendLog': 'Futu SDK 无法写入本地日志目录，请检查 OpenD 日志目录权限。',
    'events.config_apply_failed.body': '{message}',
    'signal.level': '等级',
    'auth.invalid_credentials': '用户名或密码错误。剩余尝试次数: {remaining}',
    'auth.invalid_credentials_simple': '用户名或密码错误。',
    'auth.account_locked': '密码错误达到 5 次，账号已锁定至 {until}',
  },
  en: {
    documentTitle: 'Intraday Trend Radar',
    'brand.title': 'Intraday Trend Radar',
    'session.logout': 'Log out',
    'nav.monitor': 'Monitor',
    'nav.config': 'Configuration',
    'nav.admin': 'User control',
    'nav.labs': 'Labs',
    'actions.refresh': 'Refresh',
    'actions.applyConfig': 'Apply Config',
    'actions.manual': 'User Guide',
    'actions.github': 'GitHub Repo',
    'login.title': 'Sign in to Intraday Trend Radar',
    'login.subtitle': 'The bootstrap admin account is created from the initial config.',
    'login.username': 'Username',
    'login.password': 'Password',
    'login.submit': 'Sign in',
    'register.title': 'Quick Sign Up',
    'register.username': 'Username',
    'register.password': 'Password',
    'register.submit': 'Create Account',
    'register.meta': 'Self-registration is open. Fill in a username and password to create a user account.',
    'register.limitReached': 'The user registration limit of {max} has been reached. Please contact the admin.',
    'register.disabled': 'Self-registration is currently disabled. Please contact the admin.',
    'register.watchlistHint': 'Each user can keep only one personal watchlist symbol for now.',
    'register.success': 'Account created. Please sign in with the new credentials.',
    'register.invalid': 'Please enter both username and password.',
    'monitor.title': 'Monitor',
    'monitor.subtitle': 'Inspect signals, charts, and runtime status in real time.',
    'config.title': 'Configuration',
    'config.subtitle': 'Manage watchlists, rule overrides, and notification settings.',
    'admin.title': 'User control',
    'admin.subtitle': 'Admin-only area for creating accounts, disabling users, and resetting passwords.',
    'labs.title': 'Strategy Lab',
    'labs.subtitle': 'Save templates, run backtests, and review analytics.',
    'monitor.runtime': 'Runtime Status',
    'monitor.refreshRuntime': 'Refresh Status',
    'monitor.chartParams': 'Chart Controls',
    'monitor.refreshChart': 'Refresh Chart',
    'monitor.chartModeCandles': 'Candles',
    'monitor.chartModeLine': 'Line',
    'monitor.showVwap': 'Show VWAP',
    'monitor.resetChart': 'Reset View',
    'monitor.collapseChart': 'Hide Chart',
    'monitor.expandChart': 'Show Chart',
    'monitor.mySignals': 'My Signals',
    'monitor.signalFilter': 'Filter by keyword',
    'monitor.allStocks': 'All symbols',
    'monitor.allTimeframes': 'All timeframes',
    'monitor.signalDetail': 'Signal Detail',
    'monitor.clickSignal': 'Click a signal on the left to inspect details',
    'monitor.chartHint': 'Chart: {symbol} · {tf} · {bars} bars',
    'monitor.chartHintEmpty': 'Select a symbol and refresh the chart',
    'monitor.chartGuide': 'Scroll to zoom and drag to pan.',
    'monitor.indicatorsCount': '{count} aligned indicators',
    'monitor.currentPrice': 'Now',
    'monitor.forwardMetrics': 'After {minutes}m high +{up}% · low -{down}% · close {final}%',
    'monitor.signalPageSummary': 'Page {page} / {pages} · {total} total',
    'monitor.signalPageSize': 'Per page',
    'monitor.noSignals': 'No signals yet',
    'monitor.noChartData': 'No chart data yet',
    'monitor.noRuntimeEvents': 'No runtime events yet',
    'monitor.runtimeError': 'Latest error: {error}',
    'monitor.runtimeHint': 'symbols {symbols} / profiles {profiles}',
    'monitor.scope.global': 'Global',
    'monitor.scope.user': 'Personal',
    'runtime.running': 'running',
    'runtime.idle': 'idle',
    'runtime.offline': 'offline',
    'runtime.stats.activeSymbols': 'Active Symbols',
    'runtime.stats.subscriptions': 'Subscriptions',
    'runtime.stats.profiles': 'Profiles',
    'runtime.stats.warmupBars': 'Warmup bars',
    'config.globalStocks': 'Global Watchlist',
    'config.myStocks': 'My Watchlist',
    'config.stockNamePlaceholder': 'Symbol name',
    'config.addGlobalStock': 'Add global stock',
    'config.addMyStock': 'Add my stock',
    'config.myStockLimit': 'Each regular user can keep only one personal watchlist symbol for now.',
    'config.myStockLimitAdmin': 'Admin accounts can keep an unlimited number of personal watchlist symbols.',
    'config.defaultRules': 'Default Rules',
    'config.myRuleOverrides': 'My Rule Overrides',
    'config.saveRules': 'Save Rule Overrides',
    'config.forwardMetrics': 'Signal Evaluation Window',
    'config.forwardMetricsHint': 'New signals will record max up, max down, and final change using these minute windows.',
    'config.forwardMetric1m': '1m signal window (minutes)',
    'config.forwardMetric5m': '5m signal window (minutes)',
    'config.saveForwardMetrics': 'Save Evaluation Window',
    'config.notifications': 'Notifications',
    'config.notifyNone': 'Disable All Notifications',
    'config.notifyLocal': 'Local Only',
    'config.notifyTelegram': 'Telegram Only',
    'config.notifyBoth': 'Local + Telegram',
    'config.telegramAdminOnly': 'Telegram is available to admin accounts only.',
    'config.bellOnAlert': 'Ring bell on alert',
    'config.saveNotifications': 'Save Notification Settings',
    'config.noGlobalStocks': 'No global stocks yet',
    'config.noMyStocks': 'No personal stocks yet',
    'common.enabled': 'enabled',
    'common.disabled': 'disabled',
    'common.all': 'All',
    'common.createdAt': 'Created at {time}',
    'common.notAvailable': 'No data',
    'common.passwordUpdated': 'Password updated',
    'common.saved': 'Saved',
    'common.usernameTaken': 'That username already exists. Please choose another one.',
    'common.registrationLimitReached': 'The registration limit has been reached.',
    'common.watchlistLimitReached': 'Each regular user can keep only one personal watchlist symbol.',
    'common.accountEnabled': 'Account enabled',
    'common.accountDisabled': 'Account disabled',
    'common.cannotDisableSelf': 'You cannot disable the current admin account',
    'common.role': 'Role',
    'common.status': 'Status',
    'common.prev': 'Previous',
    'common.next': 'Next',
    'admin.createUser': 'Create User',
    'admin.usernamePlaceholder': 'Username',
    'admin.passwordPlaceholder': 'Initial password',
    'admin.createUserAction': 'Create User',
    'admin.userList': 'User List',
    'admin.registrationSummary': 'User limit {max} · registered {registered} · remaining {remaining}',
    'admin.noUsers': 'No users yet',
    'admin.resetPassword': 'Reset Password',
    'admin.newPassword': 'New password',
    'admin.disableUser': 'Disable Account',
    'admin.enableUser': 'Enable Account',
    'admin.selfTag': 'Current session',
    'labs.ruleTemplates': 'Rule Templates',
    'labs.templateName': 'Template name',
    'labs.templateDescription': 'Description',
    'labs.saveTemplate': 'Save Template',
    'labs.localBacktest': 'Local Backtest',
    'labs.submitBacktest': 'Submit Backtest',
    'labs.analytics': 'Analytics',
    'labs.noTemplates': 'No templates yet',
    'labs.noBacktests': 'No backtests yet',
    'labs.passCriteria': 'Pass Criteria',
    'labs.useTimeframes': 'Timeframes',
    'labs.criteria1m': '1m criteria',
    'labs.criteria5m': '5m criteria',
    'labs.horizonMinutes': 'Horizon (minutes)',
    'labs.minMovePct': 'Min move (%)',
    'labs.latestBacktest': 'Latest Backtest',
    'labs.latestBacktestEmpty': 'Run a backtest to see the signal table and summary here.',
    'labs.backtestSignals': 'Triggered Signals',
    'labs.backtestSummary': 'Backtest Summary',
    'labs.filterTimeframe': 'Timeframe Filter',
    'labs.filterResult': 'Result Filter',
    'labs.filterDirection': 'Direction Filter',
    'labs.backtestNoResults': 'No signals were triggered in this backtest.',
    'labs.noFilteredSignals': 'No signals match the current filters.',
    'labs.backtestPending': 'Backtest submitted and waiting for results.',
    'labs.backtestOverall': 'Overall',
    'labs.backtestByTimeframe': 'By Timeframe',
    'labs.table.time': 'Time',
    'labs.table.timeframe': 'TF',
    'labs.table.rule': 'Rule',
    'labs.table.signal': 'Signal',
    'labs.table.maxUp': 'Max Up%',
    'labs.table.maxDown': 'Max Drawdown%',
    'labs.table.threshold': 'Threshold%',
    'labs.table.horizon': 'Horizon',
    'labs.table.result': 'Result',
    'labs.result.pass': 'Pass',
    'labs.result.fail': 'Fail',
    'labs.result.all': 'All Results',
    'labs.direction.all': 'All Directions',
    'labs.backtestEvaluated': 'Signals',
    'labs.backtestEffective': 'Passed',
    'labs.backtestHitRate': 'Hit Rate',
    'labs.backtestAvgUp': 'Avg Max Up%',
    'labs.backtestAvgDown': 'Avg Max Drawdown%',
    'labs.backtestSymbolPlaceholder': 'Select a symbol',
    'labs.noBacktestSymbols': 'Add symbols in Configuration before running a local backtest',
    'labs.backtestSymbolRequired': 'Please select a symbol to backtest',
    'labs.backtestDateRequired': 'Please choose a trade date',
    'labs.backtestTimeframeRequired': 'Please select at least one timeframe',
    'labs.backtestError': 'Failure reason: {error}',
    'labs.backtestStatus.queued': 'queued',
    'labs.backtestStatus.running': 'running',
    'labs.backtestStatus.done': 'done',
    'labs.backtestStatus.failed': 'failed',
    'rule.open_range_breakout': 'Open Range Breakout',
    'rule.vwap_deviation': 'VWAP Deviation',
    'rule.squeeze_breakout': 'Squeeze Breakout',
    'rule.rsi_overbought': 'RSI Overbought',
    'rule.rsi_oversold': 'RSI Oversold',
    'rule.break_retest': 'Break Retest',
    'rule.volume_price_divergence': 'Volume Price Divergence',
    'rule.prev_day_break': 'Previous Day Break',
    'direction.up': 'Up',
    'direction.down': 'Down',
    'direction.neutral': 'Neutral',
    'summary.timeframes': 'Timeframes',
    'events.level.info': 'info',
    'events.level.error': 'error',
    'events.runtime_started.title': 'Runtime Started',
    'events.runtime_started.body': 'Subscribed to {symbols} symbols across {timeframes} timeframes and waiting for new bars.',
    'events.subscriptions_updated.title': 'Subscriptions Updated',
    'events.subscriptions_updated.body': 'Configuration reapplied. {symbols} symbols are now active.',
    'events.config_applied.title': 'Configuration Applied',
    'events.config_applied.body': '{symbols} symbols · {timeframes} timeframes · {profiles} profiles',
    'events.warmup_completed.title': 'Warmup Completed',
    'events.warmup_completed.body': '{bars} bars were replayed to initialize indicator state.',
    'events.warmup_symbol_failed.title': 'Symbol Warmup Skipped',
    'events.warmup_symbol_failed.body': '{message}',
    'events.config_apply_failed.title': 'Configuration Apply Failed',
    'events.config_apply_failed.opendLog': 'The Futu SDK could not write its local log file. Please check the OpenD log directory permissions.',
    'events.config_apply_failed.body': '{message}',
    'signal.level': 'Level',
    'auth.invalid_credentials': 'Invalid username or password. Remaining attempts: {remaining}',
    'auth.invalid_credentials_simple': 'Invalid username or password.',
    'auth.account_locked': 'Too many password failures. Account locked until {until}',
  },
};

const pageMeta = {
  monitor: ['monitor.title', 'monitor.subtitle'],
  config: ['config.title', 'config.subtitle'],
  admin: ['admin.title', 'admin.subtitle'],
  labs: ['labs.title', 'labs.subtitle'],
};

function t(key, vars = {}) {
  const dict = translations[state.language] || translations.zh;
  const template = dict[key] || translations.zh[key] || key;
  return template.replace(/\{(\w+)\}/g, (_, name) => String(vars[name] ?? ''));
}

function setLanguage(language) {
  state.language = language;
  localStorage.setItem('app.language', language);
  document.documentElement.lang = language === 'zh' ? 'zh' : 'en';
  document.title = t('documentTitle');
  document.querySelectorAll('[data-i18n]').forEach(node => {
    node.textContent = t(node.dataset.i18n);
  });
  document.querySelectorAll('[data-i18n-placeholder]').forEach(node => {
    node.placeholder = t(node.dataset.i18nPlaceholder);
  });
  document.getElementById('langZhBtn').classList.toggle('active', language === 'zh');
  document.getElementById('langEnBtn').classList.toggle('active', language === 'en');
  const runtimeBadge = document.getElementById('runtimeBadge');
  if (runtimeBadge && runtimeBadge.textContent === 'offline') {
    runtimeBadge.textContent = t('runtime.offline');
  }
  const activeBtn = document.querySelector('.nav-btn.active');
  setActiveView(activeBtn ? activeBtn.dataset.view : 'monitor');
  renderSession();
  renderRegistrationCard();
  renderRuntime();
  renderStocks();
  renderRules();
  renderForwardMetricsConfig();
  renderNotifications();
  renderUsers();
  renderSignals();
  renderTemplates();
  renderBacktests();
  renderAnalytics();
  renderChartPanel();
  drawChart();
}

function formatBeijingDateTime(value) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return new Intl.DateTimeFormat(state.language === 'zh' ? 'zh-CN' : 'en-US', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(date);
}

function formatBeijingTimeShort(value) {
  if (!value) return '--:--';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value).slice(11, 16) || String(value);
  return new Intl.DateTimeFormat(state.language === 'zh' ? 'zh-CN' : 'en-US', {
    timeZone: 'Asia/Shanghai',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(date);
}

function normalizeTf(value) {
  return String(value || '').trim().toLowerCase();
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function translatedRuleLabel(ruleName) {
  if (!ruleName) return '-';
  const key = `rule.${ruleName}`;
  const label = t(key);
  return label === key ? ruleName : label;
}

function signalTriggerLabels(signal) {
  return Array.from(new Set((signal.triggers || []).map(trigger => translatedRuleLabel(trigger.name))));
}

function buildRuntimeEventView(item) {
  const payload = item.payload || {};
  const titleKey = `events.${item.event_type}.title`;
  const bodyKey = `events.${item.event_type}.body`;
  let body = item.message || '';
  if (item.event_type === 'runtime_started') {
    body = t(bodyKey, { symbols: payload.symbol_count ?? payload.symbols?.length ?? 0, timeframes: payload.timeframe_count ?? payload.timeframes?.length ?? 0 });
  } else if (item.event_type === 'subscriptions_updated') {
    body = t(bodyKey, { symbols: payload.symbol_count ?? payload.symbols?.length ?? 0 });
  } else if (item.event_type === 'config_applied') {
    body = t(bodyKey, {
      symbols: payload.symbol_count ?? payload.symbols?.length ?? 0,
      timeframes: payload.timeframe_count ?? payload.timeframes?.length ?? 0,
      profiles: payload.profiles_count ?? 0,
    });
  } else if (item.event_type === 'warmup_completed') {
    body = t(bodyKey, { bars: payload.warmup_total_bars ?? 0 });
  } else if (item.event_type === 'config_apply_failed') {
    body = String(item.message || '').includes('.com.futunn.FutuOpenD/Log')
      ? t('events.config_apply_failed.opendLog')
      : t(bodyKey, { message: item.message || '' });
  }
  return {
    title: t(titleKey),
    body,
    level: t(`events.level.${item.level}`),
    createdAt: formatBeijingDateTime(item.created_at),
  };
}

function friendlyRuntimeError(message) {
  const text = String(message || '');
  if (text.includes('.com.futunn.FutuOpenD/Log')) {
    return t('events.config_apply_failed.opendLog');
  }
  return text;
}

async function fetchJSON(url, options = {}) {
  const res = await fetch(url, {
    headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
    ...options,
  });
  const text = await res.text();
  const data = text ? JSON.parse(text) : {};
  if (!res.ok) {
    const error = new Error(data.error || `HTTP ${res.status}`);
    error.payload = data;
    throw error;
  }
  return data;
}

function setActiveView(view) {
  document.querySelectorAll('.nav-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.view === view));
  document.querySelectorAll('.view').forEach(section => section.classList.toggle('active', section.id === `view-${view}`));
  document.getElementById('pageTitle').textContent = t(pageMeta[view][0]);
  document.getElementById('pageSubtitle').textContent = t(pageMeta[view][1]);
  if (view === 'monitor') {
    requestAnimationFrame(() => {
      renderChartPanel();
      drawChart();
    });
  }
}

function renderSession() {
  const me = state.me;
  const loginView = document.getElementById('loginView');
  const appView = document.getElementById('appView');
  const sessionCard = document.getElementById('sessionCard');
  const adminNavBtn = document.getElementById('adminNavBtn');
  const adminView = document.getElementById('view-admin');
  const forwardMetricsPanel = document.getElementById('forwardMetricsPanel');
  const adminVisible = me?.user.role === 'admin';
  if (!me) {
    loginView.classList.remove('hidden');
    appView.classList.add('hidden');
    sessionCard.classList.add('hidden');
    adminNavBtn.classList.add('hidden');
    adminView.classList.add('hidden');
    forwardMetricsPanel?.classList.add('hidden');
    renderRegistrationCard();
    return;
  }
  loginView.classList.add('hidden');
  appView.classList.remove('hidden');
  sessionCard.classList.remove('hidden');
  document.getElementById('sessionUser').textContent = me.user.username;
  document.getElementById('sessionRole').textContent = me.user.role;
  adminView.classList.toggle('hidden', !adminVisible);
  adminNavBtn.classList.toggle('hidden', !adminVisible);
  forwardMetricsPanel?.classList.toggle('hidden', !adminVisible);
  document.getElementById('globalStockForm').style.display = adminVisible ? 'grid' : 'none';
  const myStockLimitHint = document.getElementById('myStockLimitHint');
  if (myStockLimitHint) {
    myStockLimitHint.textContent = adminVisible ? t('config.myStockLimitAdmin') : t('config.myStockLimit');
  }
  syncNotificationForm();
  if (!adminVisible && document.querySelector('.nav-btn.active')?.dataset.view === 'admin') {
    setActiveView('monitor');
  }
}

function renderRegistrationCard() {
  const meta = document.getElementById('registerMeta');
  const submitBtn = document.getElementById('registerSubmitBtn');
  const form = document.getElementById('registerForm');
  if (!meta || !submitBtn || !form) return;
  const registration = state.registration || {};
  const enabled = Boolean(registration.enabled);
  const limitReached = Boolean(registration.limit_reached);
  if (!enabled) {
    meta.textContent = t('register.disabled');
  } else if (limitReached) {
    meta.textContent = t('register.limitReached', { max: registration.max_users ?? 0 });
  } else {
    meta.textContent = t('register.meta');
  }
  form.querySelectorAll('input, button').forEach(node => {
    if (node.id === 'langZhBtn' || node.id === 'langEnBtn') return;
    node.disabled = !enabled || limitReached;
  });
}

function renderRuntime() {
  const runtime = state.runtime || {};
  const status = document.getElementById('runtimeStatus');
  const badge = document.getElementById('runtimeBadge');
  const hint = document.getElementById('statusHint');
  const filteredEvents = [];
  let hasConfigApplied = false;
  for (const item of runtime.recent_events || []) {
    if (item.event_type === 'config_applied') {
      if (hasConfigApplied) continue;
      hasConfigApplied = true;
    }
    filteredEvents.push(item);
  }
  badge.textContent = runtime.started ? t('runtime.running') : (runtime.active_symbols ? t('runtime.idle') : t('runtime.offline'));
  hint.textContent = runtime.last_error
    ? t('monitor.runtimeError', { error: friendlyRuntimeError(runtime.last_error) })
    : t('monitor.runtimeHint', { symbols: runtime.active_symbols?.length || 0, profiles: runtime.profiles_count || 0 });
  const cards = [
    [t('runtime.stats.activeSymbols'), runtime.active_symbols?.length || 0],
    [t('runtime.stats.subscriptions'), runtime.subscriptions_count || 0],
    [t('runtime.stats.profiles'), runtime.profiles_count || 0],
    [t('runtime.stats.warmupBars'), runtime.warmup_total_bars || 0],
  ];
  status.innerHTML = cards.map(([label, value]) => `<div class="stat-card"><span>${label}</span><strong>${value}</strong></div>`).join('');
  document.getElementById('runtimeEvents').innerHTML = filteredEvents.map(item => {
    const view = buildRuntimeEventView(item);
    return `
    <div class="event-item">
      <div><strong>${view.title}</strong></div>
      <div class="meta-line">${view.createdAt} · ${view.level}</div>
      <div>${view.body}</div>
    </div>`;
  }).join('') || `<div class="meta-line">${t('monitor.noRuntimeEvents')}</div>`;
}

function renderStocks() {
  const displayLabel = item => item?.symbol_name ? `${item.symbol} ${item.symbol_name}` : item.symbol;
  const toHtml = (items, emptyKey) => items.length
    ? items.map(item => `
      <div class="list-item">
        <div><strong>${item.symbol}</strong> ${item.symbol_name || ''}</div>
        <div class="meta-line">${item.enabled ? t('common.enabled') : t('common.disabled')}</div>
      </div>`).join('')
    : `<div class="meta-line">${t(emptyKey)}</div>`;
  document.getElementById('globalStockList').innerHTML = toHtml(state.globalStocks, 'config.noGlobalStocks');
  document.getElementById('myStockList').innerHTML = toHtml(state.myStocks, 'config.noMyStocks');
  const stockOptions = [];
  [...state.globalStocks, ...state.myStocks].forEach(item => {
    if (!stockOptions.some(entry => entry.symbol === item.symbol)) {
      stockOptions.push(item);
    }
  });
  const symbols = stockOptions.map(item => item.symbol);
  const signalSymbolFilter = document.getElementById('signalSymbolFilter');
  const previousSignalSymbol = signalSymbolFilter.value;
  signalSymbolFilter.innerHTML = `
    <option value="">${t('monitor.allStocks')}</option>
    ${symbols.map(symbol => `<option value="${symbol}">${symbol}</option>`).join('')}
  `;
  signalSymbolFilter.value = symbols.includes(previousSignalSymbol) ? previousSignalSymbol : '';
  const chartSelect = document.getElementById('chartSymbol');
  const previousChart = chartSelect.value;
  chartSelect.innerHTML = symbols.map(symbol => `<option value="${symbol}">${symbol}</option>`).join('');
  if (symbols.includes(previousChart)) chartSelect.value = previousChart;
  else if (symbols[0]) chartSelect.value = symbols[0];

  const chartMode = document.getElementById('chartMode');
  if (chartMode) {
    chartMode.value = state.chartView.mode;
  }
  const chartShowVwap = document.getElementById('chartShowVwap');
  if (chartShowVwap) {
    chartShowVwap.checked = Boolean(state.chartView.showVwap);
  }

  const backtestSelect = document.getElementById('backtestSymbol');
  const previousBacktest = backtestSelect.value;
  backtestSelect.innerHTML = stockOptions.length
    ? stockOptions.map(item => `<option value="${item.symbol}">${displayLabel(item)}</option>`).join('')
    : `<option value="">${t('labs.backtestSymbolPlaceholder')}</option>`;
  if (symbols.includes(previousBacktest)) backtestSelect.value = previousBacktest;
  else if (symbols[0]) backtestSelect.value = symbols[0];
  else backtestSelect.value = '';

  const backtestDate = document.getElementById('backtestDate');
  if (backtestDate && !backtestDate.value) {
    const now = new Date();
    const beijing = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Asia/Shanghai',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).format(now);
    backtestDate.value = beijing;
  }

}

function renderRules() {
  document.getElementById('defaultRulesView').textContent = JSON.stringify(state.defaultRules || {}, null, 2);
  document.getElementById('myRulesEditor').value = JSON.stringify(state.myRules || {}, null, 2);
  document.getElementById('templateConfigEditor').value = JSON.stringify(mergeDeep(JSON.parse(JSON.stringify(state.defaultRules || {})), state.myRules || {}), null, 2);
}

function renderChartPanel() {
  const grid = document.getElementById('monitorMainGrid');
  const panel = document.getElementById('chartPanel');
  const body = document.getElementById('chartPanelBody');
  const btn = document.getElementById('toggleChartPanelBtn');
  if (!grid || !panel || !body || !btn) return;
  const collapsed = Boolean(state.chartView.collapsed);
  grid.classList.toggle('chart-collapsed', collapsed);
  panel.classList.toggle('panel-collapsed', collapsed);
  panel.classList.toggle('hidden', collapsed);
  body.classList.toggle('hidden', collapsed);
  btn.textContent = collapsed ? t('monitor.expandChart') : t('monitor.collapseChart');
  btn.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
}

function renderForwardMetricsConfig() {
  const input1m = document.getElementById('forwardMetric1m');
  const input5m = document.getElementById('forwardMetric5m');
  if (!input1m || !input5m) return;
  input1m.value = state.forwardMetrics['1m_horizon_minutes'] ?? 20;
  input5m.value = state.forwardMetrics['5m_horizon_minutes'] ?? 60;
}

function renderNotifications() {
  const form = document.getElementById('notificationForm');
  if (!form) return;
  form.mode.value = state.notifications.mode || 'local';
  form.telegram_token.value = state.notifications.telegram_token || '';
  form.telegram_chat_id.value = state.notifications.telegram_chat_id || '';
  form.bell_on_alert.checked = Boolean(state.notifications.bell_on_alert);
  syncNotificationForm();
}

function syncNotificationForm() {
  const form = document.getElementById('notificationForm');
  if (!form) return;
  const isAdmin = state.me?.user.role === 'admin';
  const mode = form.mode.value || 'local';
  const telegramFields = document.getElementById('telegramSettingsFields');
  const telegramHint = document.getElementById('telegramAdminOnlyHint');
  form.querySelectorAll('[data-notify-admin-only="1"]').forEach(option => {
    option.disabled = !isAdmin;
    option.hidden = !isAdmin;
  });
  if (!isAdmin && (mode === 'telegram' || mode === 'both')) {
    form.mode.value = 'local';
  }
  const showTelegramFields = isAdmin && (form.mode.value === 'telegram' || form.mode.value === 'both');
  telegramFields?.classList.toggle('hidden', !showTelegramFields);
  telegramHint?.classList.toggle('hidden', isAdmin);
}

function renderUsers() {
  const box = document.getElementById('userList');
  const registrationSummary = document.getElementById('adminRegistrationSummary');
  if (!box) return;
  const users = state.users || [];
  const registration = state.registration || {};
  if (registrationSummary) {
    registrationSummary.textContent = t('admin.registrationSummary', {
      max: registration.max_users ?? 0,
      registered: registration.registered_users ?? 0,
      remaining: registration.remaining_slots ?? 0,
    });
  }
  box.innerHTML = users.length ? users.map(user => `
    <div class="list-item">
      <div>
        <strong>${user.username}</strong>
        <span class="pill">${user.role}</span>
        ${state.me?.user.id === user.id ? `<span class="pill">${t('admin.selfTag')}</span>` : ''}
      </div>
      <div class="meta-line">${t('common.status')}: ${user.is_active ? t('common.enabled') : t('common.disabled')}</div>
      <div class="meta-line">${t('common.createdAt', { time: formatBeijingDateTime(user.created_at) })}</div>
      <form class="reset-password-form stack-form" data-user-id="${user.id}">
        <input name="password" placeholder="${t('admin.newPassword')}" />
        <button class="mini-btn" type="submit">${t('admin.resetPassword')}</button>
      </form>
      <button class="mini-btn toggle-user-btn" type="button" data-user-id="${user.id}" data-next-active="${user.is_active ? '0' : '1'}" ${state.me?.user.id === user.id ? 'disabled' : ''}>
        ${user.is_active ? t('admin.disableUser') : t('admin.enableUser')}
      </button>
    </div>`).join('') : `<div class="meta-line">${t('admin.noUsers')}</div>`;

  box.querySelectorAll('.reset-password-form').forEach(form => {
    form.addEventListener('submit', async event => {
      event.preventDefault();
      const password = form.password.value.trim();
      if (!password) return;
      await fetchJSON(`/api/users/${form.dataset.userId}/password`, {
        method: 'PUT',
        body: JSON.stringify({ password }),
      });
      form.reset();
      alert(t('common.passwordUpdated'));
    });
  });

  box.querySelectorAll('.toggle-user-btn').forEach(button => {
    button.addEventListener('click', async () => {
      const userId = Number(button.dataset.userId);
      const isActive = button.dataset.nextActive === '1';
      if (state.me?.user.id === userId && !isActive) {
        alert(t('common.cannotDisableSelf'));
        return;
      }
      await fetchJSON(`/api/users/${userId}`, {
        method: 'PUT',
        body: JSON.stringify({ is_active: isActive }),
      });
      await refreshAdmin();
      alert(isActive ? t('common.accountEnabled') : t('common.accountDisabled'));
    });
  });
}

function renderSignals() {
  const list = document.getElementById('signalList');
  const signals = state.signals || [];
  list.innerHTML = signals.length ? signals.map(signal => `
    <div class="signal-card ${signal.direction || ''} ${state.selectedSignalId === signal.id ? 'active' : ''}" data-signal-id="${signal.id}">
      <div class="signal-meta-row">
        <strong>${escapeHtml(signal.symbol_name || signal.symbol)}</strong>
        <span>${escapeHtml(signal.timeframe || '-')}</span>
        <span class="pill">${escapeHtml(signal.level || 'Lv0')}</span>
        <span class="signal-pill ${signal.direction || ''}">${t(`direction.${signal.direction || 'neutral'}`)}</span>
      </div>
      <div class="meta-line">${formatBeijingDateTime(signal.ts)} · ${t('monitor.indicatorsCount', { count: signal.trigger_count || (signal.triggers || []).length || 1 })}</div>
      ${renderSignalPrice(signal)}
      <div>${escapeHtml(signalTriggerLabels(signal).join(' · '))}</div>
      <div class="meta-line">${escapeHtml(signal.message || '')}</div>
      ${renderForwardMetrics(signal)}
    </div>`).join('') : `<div class="meta-line">${t('monitor.noSignals')}</div>`;
  list.querySelectorAll('.signal-card').forEach(card => {
    card.addEventListener('click', () => selectSignal(card.dataset.signalId));
  });
  renderSignalPager();
}

function renderSignalPager() {
  const pager = document.getElementById('signalPager');
  if (!pager) return;
  const total = Number(state.signalPage?.total || 0);
  const limit = Math.max(1, Number(state.signalPage?.limit || 100));
  const offset = Math.max(0, Number(state.signalPage?.offset || 0));
  const currentPage = total ? Math.floor(offset / limit) + 1 : 1;
  const totalPages = Math.max(1, Math.ceil(total / limit));
  pager.innerHTML = `
    <div class="pager-meta">
      <label class="pager-size-control">
        <span class="meta-line">${t('monitor.signalPageSize')}</span>
        <select id="signalPageSizeSelect">
          ${[20, 50, 100].map(size => `<option value="${size}" ${size === limit ? 'selected' : ''}>${size}</option>`).join('')}
        </select>
      </label>
      <span class="meta-line">${t('monitor.signalPageSummary', { page: currentPage, pages: totalPages, total, limit })}</span>
    </div>
    <div class="pager-actions">
      <button id="signalPrevPageBtn" class="mini-btn" type="button" ${offset <= 0 ? 'disabled' : ''}>${t('common.prev')}</button>
      <button id="signalNextPageBtn" class="mini-btn" type="button" ${!state.signalPage?.hasMore ? 'disabled' : ''}>${t('common.next')}</button>
    </div>
  `;
  document.getElementById('signalPageSizeSelect')?.addEventListener('change', event => {
    const nextLimit = Number(event.currentTarget.value || 100);
    if (!Number.isFinite(nextLimit) || nextLimit <= 0) return;
    state.signalPage.limit = nextLimit;
    state.signalPage.offset = 0;
    localStorage.setItem('signals.pageSize', String(nextLimit));
    refreshSignals();
  });
  document.getElementById('signalPrevPageBtn')?.addEventListener('click', () => changeSignalPage(-1));
  document.getElementById('signalNextPageBtn')?.addEventListener('click', () => changeSignalPage(1));
}

function renderSignalPrice(signal) {
  const snapshot = state.marketSnapshots?.[signal.symbol];
  if (!snapshot || !Number.isFinite(Number(snapshot.last_price))) return '';
  const change = Number(snapshot.change_pct);
  const changeClass = !Number.isFinite(change)
    ? 'flat'
    : (change > 0 ? 'up' : (change < 0 ? 'down' : 'flat'));
  const changeText = Number.isFinite(change) ? `${change >= 0 ? '+' : ''}${change.toFixed(2)}%` : '--';
  return `<div class="meta-line signal-price-line"><span>${t('monitor.currentPrice')}</span> <strong>${formatNumber(snapshot.last_price, 2)}</strong> <span class="signal-metric-value ${changeClass}">${escapeHtml(changeText)}</span></div>`;
}

function renderForwardMetrics(signal) {
  const metrics = signal?.evaluation;
  if (
    !metrics
    || !metrics.completed
    || !Number.isFinite(Number(metrics.max_up))
    || !Number.isFinite(Number(metrics.max_down))
    || !Number.isFinite(Number(metrics.final_change))
  ) {
    return '';
  }
  const prefix = state.language === 'zh' ? `${metrics.horizon_minutes ?? '-'}m后` : `After ${metrics.horizon_minutes ?? '-'}m`;
  const finalClass = Number(metrics.final_change) > 0 ? 'up' : (Number(metrics.final_change) < 0 ? 'down' : 'flat');
  return `<div class="meta-line signal-evaluation-line">
    <span>${escapeHtml(prefix)}</span>
    <span> ${state.language === 'zh' ? '最高' : 'high'} </span><span class="signal-metric-value up">+${formatNumber(metrics.max_up, 2)}%</span>
    <span> · ${state.language === 'zh' ? '最低' : 'low'} </span><span class="signal-metric-value down">-${formatNumber(metrics.max_down, 2)}%</span>
    <span> · ${state.language === 'zh' ? '收盘' : 'close'} </span><span class="signal-metric-value ${finalClass}">${formatSignedNumber(metrics.final_change, 2)}%</span>
  </div>`;
}

function renderTemplates() {
  const templates = state.templates || [];
  document.getElementById('templateList').innerHTML = templates.length ? templates.map(item => `
    <div class="list-item">
      <div><strong>${item.name}</strong></div>
      <div class="meta-line">${item.description}</div>
      <pre class="code-block">${escapeHtml(JSON.stringify(item.config, null, 2))}</pre>
    </div>`).join('') : `<div class="meta-line">${t('labs.noTemplates')}</div>`;
}

function renderBacktests() {
  const summaryBox = document.getElementById('backtestSummaryView');
  const signalsBox = document.getElementById('backtestSignalsView');
  const detail = state.backtestDetail;
  const previousTfFilter = document.getElementById('backtestTimeframeFilter')?.value || '';
  const previousResultFilter = document.getElementById('backtestResultFilter')?.value || '';
  const previousDirectionFilter = document.getElementById('backtestDirectionFilter')?.value || '';
  if (!detail) {
    summaryBox.innerHTML = `<div class="meta-line">${t('labs.latestBacktestEmpty')}</div>`;
    signalsBox.innerHTML = `<div class="meta-line">${t('labs.latestBacktestEmpty')}</div>`;
    return;
  }

  const params = detail.params || {};
  const selectedTimeframes = params.timeframes || [];
  const results = detail.results || [];
  const summary = detail.summary || {};
  const overall = summary.overall || {};
  const byTimeframe = summary.by_timeframe || {};
  const statusLabel = t(`labs.backtestStatus.${detail.status}`);
  const criteriaHtml = selectedTimeframes.map(tf => {
    const tfCriteria = byTimeframe[tf]?.criteria || params.criteria?.[tf] || state.backtestConfig.criteria?.[tf] || {};
    return `
      <div class="backtest-meta-card">
        <span>${tf}</span>
        <div class="meta-line">${t('labs.horizonMinutes')}: ${tfCriteria.horizon_minutes ?? '-'}</div>
        <div class="meta-line">${t('labs.minMovePct')}: ${formatNumber(tfCriteria.min_move_pct, 2)}%</div>
      </div>`;
  }).join('');

  const filteredResults = results.filter(item => {
    const result = item.result || {};
    const matchesTf = !previousTfFilter || item.timeframe === previousTfFilter;
    const matchesResult = !previousResultFilter
      || (previousResultFilter === 'pass' && Boolean(result.passed))
      || (previousResultFilter === 'fail' && !result.passed);
    const matchesDirection = !previousDirectionFilter || (result.direction || 'neutral') === previousDirectionFilter;
    return matchesTf && matchesResult && matchesDirection;
  });

  const tableHtml = results.length ? `
    <div>
      <div class="panel-head compact-head">
        <div class="toolbar backtest-filters">
          <span class="meta-text">${t('labs.filterTimeframe')}</span>
          <select id="backtestTimeframeFilter">
            <option value="">${t('common.all')}</option>
            ${selectedTimeframes.map(tf => `<option value="${tf}" ${previousTfFilter === tf ? 'selected' : ''}>${tf}</option>`).join('')}
          </select>
          <span class="meta-text">${t('labs.filterResult')}</span>
          <select id="backtestResultFilter">
            <option value="">${t('labs.result.all')}</option>
            <option value="pass" ${previousResultFilter === 'pass' ? 'selected' : ''}>${t('labs.result.pass')}</option>
            <option value="fail" ${previousResultFilter === 'fail' ? 'selected' : ''}>${t('labs.result.fail')}</option>
          </select>
          <span class="meta-text">${t('labs.filterDirection')}</span>
          <select id="backtestDirectionFilter">
            <option value="">${t('labs.direction.all')}</option>
            <option value="up" ${previousDirectionFilter === 'up' ? 'selected' : ''}>${t('direction.up')}</option>
            <option value="down" ${previousDirectionFilter === 'down' ? 'selected' : ''}>${t('direction.down')}</option>
          </select>
        </div>
      </div>
      <div class="backtest-table-wrap">
        <table class="backtest-table">
          <thead>
            <tr>
              <th>${t('labs.table.time')}</th>
              <th>${t('labs.table.timeframe')}</th>
              <th>${t('labs.table.rule')}</th>
              <th>${t('labs.table.signal')}</th>
              <th>${t('labs.table.maxUp')}</th>
              <th>${t('labs.table.maxDown')}</th>
              <th>${t('labs.table.threshold')}</th>
              <th>${t('labs.table.horizon')}</th>
              <th>${t('labs.table.result')}</th>
            </tr>
          </thead>
          <tbody>
            ${filteredResults.map(item => {
              const result = item.result || {};
              const passed = Boolean(result.passed);
              return `
                <tr>
                  <td>${formatBeijingDateTime(item.signal_ts)}</td>
                  <td>${item.timeframe}</td>
                  <td>${backtestRuleLabel(result.rule_key)}</td>
                  <td>${escapeHtml(backtestSignalText(result))}</td>
                  <td>${formatNumber(result.max_up, 2)}%</td>
                  <td>${formatNumber(result.max_down, 2)}%</td>
                  <td>${formatNumber(result.min_move_pct, 2)}%</td>
                  <td>${result.horizon_minutes ?? '-'}m</td>
                  <td><span class="${passed ? 'backtest-pass' : 'backtest-fail'}">${passed ? t('labs.result.pass') : t('labs.result.fail')}</span></td>
                </tr>`;
            }).join('')}
          </tbody>
        </table>
      </div>
      ${filteredResults.length ? '' : `<div class="meta-line">${t('labs.noFilteredSignals')}</div>`}
    </div>` : `<div class="meta-line">${detail.status === 'done' ? t('labs.backtestNoResults') : t('labs.backtestPending')}</div>`;

  const summaryCards = `
    <div>
      <h4 class="backtest-section-title">${t('labs.backtestSummary')}</h4>
      <div class="backtest-summary-grid">
        <div class="backtest-summary-card">
          <span>${t('labs.backtestEvaluated')}</span>
          <strong>${overall.evaluated ?? 0}</strong>
        </div>
        <div class="backtest-summary-card">
          <span>${t('labs.backtestEffective')}</span>
          <strong>${overall.effective ?? 0}</strong>
        </div>
        <div class="backtest-summary-card">
          <span>${t('labs.backtestHitRate')}</span>
          <strong>${formatNumber(overall.hit_rate, 2)}%</strong>
        </div>
        <div class="backtest-summary-card">
          <span>${t('summary.timeframes')}</span>
          <strong>${selectedTimeframes.join(', ') || '-'}</strong>
        </div>
      </div>
      <div class="list-block">
        ${selectedTimeframes.map(tf => {
          const item = byTimeframe[tf] || {};
          return `
            <div class="list-item">
              <div><strong>${t('labs.backtestByTimeframe')} ${tf}</strong></div>
              <div class="meta-line">${t('labs.backtestEvaluated')}: ${item.evaluated ?? 0} · ${t('labs.backtestEffective')}: ${item.effective ?? 0} · ${t('labs.backtestHitRate')}: ${formatNumber(item.hit_rate, 2)}%</div>
              <div class="meta-line">${t('labs.backtestAvgUp')}: ${formatNumber(item.avg_max_up, 2)}% · ${t('labs.backtestAvgDown')}: ${formatNumber(item.avg_max_down, 2)}%</div>
            </div>`;
        }).join('')}
      </div>
    </div>`;

  summaryBox.innerHTML = `
    <div class="backtest-meta-grid">
      <div class="backtest-meta-card">
        <span>${t('labs.latestBacktest')}</span>
        <strong>${detail.symbol}</strong>
        <div class="meta-line">${detail.trade_date}</div>
      </div>
      <div class="backtest-meta-card">
        <span>${t('labs.table.result')}</span>
        <strong>${statusLabel}</strong>
        <div class="meta-line">${formatBeijingDateTime(detail.created_at)}</div>
      </div>
      ${criteriaHtml}
    </div>
    ${detail.error_text ? `<div class="backtest-error">${t('labs.backtestError', { error: detail.error_text })}</div>` : ''}
    ${summaryCards}
  `;
  signalsBox.innerHTML = tableHtml;

  document.getElementById('backtestTimeframeFilter')?.addEventListener('change', renderBacktests);
  document.getElementById('backtestResultFilter')?.addEventListener('change', renderBacktests);
  document.getElementById('backtestDirectionFilter')?.addEventListener('change', renderBacktests);
}

function renderAnalytics() {
  document.getElementById('analyticsView').textContent = JSON.stringify(state.analytics || {}, null, 2);
}

function resetChartView() {
  const bars = state.chartBars || [];
  const maxVisible = Math.max(24, Math.min(160, bars.length || 80));
  state.chartView.visibleCount = clamp(state.chartView.visibleCount || 80, 24, maxVisible);
  state.chartView.startIndex = Math.max(0, bars.length - state.chartView.visibleCount);
}

function visibleChartBars() {
  const bars = state.chartBars || [];
  if (!bars.length) {
    return { bars: [], start: 0, end: 0 };
  }
  const visibleCount = clamp(state.chartView.visibleCount || 80, 24, Math.max(24, bars.length));
  const maxStart = Math.max(0, bars.length - visibleCount);
  state.chartView.startIndex = clamp(state.chartView.startIndex || 0, 0, maxStart);
  const start = state.chartView.startIndex;
  const end = Math.min(bars.length, start + visibleCount);
  return { bars: bars.slice(start, end), start, end };
}

function priceToY(price, top, bottom, plotTop, plotHeight) {
  const range = top - bottom || 1;
  return plotTop + ((top - price) / range) * plotHeight;
}

function chartSignalsForCurrentView() {
  const symbol = document.getElementById('chartSymbol')?.value;
  const timeframe = normalizeTf(document.getElementById('chartTf')?.value);
  return (state.signals || []).filter(item => item.symbol === symbol && normalizeTf(item.timeframe) === timeframe);
}

function drawChart() {
  const canvas = document.getElementById('chartCanvas');
  if (!canvas) return;
  if (state.chartView.collapsed) {
    const meta = document.getElementById('chartMeta');
    if (meta) meta.textContent = '';
    return;
  }
  const ctx = canvas.getContext('2d');
  const bars = state.chartBars || [];
  const ratio = window.devicePixelRatio || 1;
  const width = Math.max(canvas.clientWidth || canvas.parentElement?.clientWidth || 0, 320);
  const height = canvas.clientHeight || Number(canvas.getAttribute('height')) || 260;
  const meta = document.getElementById('chartMeta');
  canvas.width = width * ratio;
  canvas.height = height * ratio;
  ctx.setTransform(1, 0, 0, 1, 0, 0);
  ctx.scale(ratio, ratio);
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = '#fffef9';
  ctx.fillRect(0, 0, width, height);
  if (!bars.length) {
    ctx.fillStyle = '#617080';
    ctx.font = '13px "IBM Plex Sans", "Noto Sans SC", sans-serif';
    ctx.fillText(t('monitor.noChartData'), 16, 24);
    if (meta) meta.textContent = t('monitor.chartHintEmpty');
    return;
  }

  const symbol = document.getElementById('chartSymbol')?.value || '-';
  const tf = document.getElementById('chartTf')?.value || '-';
  const { bars: view } = visibleChartBars();
  if (!view.length) {
    if (meta) meta.textContent = t('monitor.chartHintEmpty');
    return;
  }
  if (meta) {
    meta.textContent = `${t('monitor.chartHint', { symbol, tf, bars: bars.length })} · ${t('monitor.chartGuide')}`;
  }

  const plot = {
    left: 68,
    top: 18,
    right: width - 20,
    bottom: height - 36,
  };
  const plotWidth = plot.right - plot.left;
  const plotHeight = plot.bottom - plot.top;
  const includeVwap = Boolean(state.chartView.showVwap);
  const priceValues = view.flatMap(bar => {
    const values = [bar.high, bar.low];
    if (includeVwap && Number.isFinite(Number(bar.vwap))) values.push(Number(bar.vwap));
    return values;
  });
  const rawTop = Math.max(...priceValues);
  const rawBottom = Math.min(...priceValues);
  const padding = Math.max((rawTop - rawBottom) * 0.08, rawTop * 0.0025, 0.3);
  const top = rawTop + padding;
  const bottom = rawBottom - padding;
  const stepX = plotWidth / Math.max(1, view.length);

  ctx.fillStyle = '#fffaf3';
  ctx.fillRect(plot.left, plot.top, plotWidth, plotHeight);

  ctx.strokeStyle = '#e2d7c7';
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i += 1) {
    const y = plot.top + i * (plotHeight / 4);
    ctx.beginPath();
    ctx.moveTo(plot.left, y);
    ctx.lineTo(plot.right, y);
    ctx.stroke();
    const price = top - (i * (top - bottom) / 4);
    ctx.fillStyle = '#617080';
    ctx.font = '12px "IBM Plex Sans", "Noto Sans SC", sans-serif';
    ctx.textAlign = 'left';
    ctx.fillText(price.toFixed(2), 10, y + 4);
  }

  const xStep = Math.max(1, Math.floor(view.length / 6));
  for (let i = 0; i < view.length; i += xStep) {
    const bar = view[i];
    const x = plot.left + i * stepX + stepX / 2;
    ctx.strokeStyle = 'rgba(22, 32, 42, 0.08)';
    ctx.beginPath();
    ctx.moveTo(x, plot.top);
    ctx.lineTo(x, plot.bottom);
    ctx.stroke();
    ctx.fillStyle = '#617080';
    ctx.textAlign = 'center';
    ctx.fillText(formatBeijingTimeShort(bar.ts), x, height - 12);
  }

  if (state.chartView.mode === 'line') {
    ctx.strokeStyle = '#cc5a2b';
    ctx.lineWidth = 2.4;
    ctx.beginPath();
    view.forEach((bar, index) => {
      const x = plot.left + index * stepX + stepX / 2;
      const y = priceToY(bar.close, top, bottom, plot.top, plotHeight);
      if (index === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();
  } else {
    const candleWidth = Math.max(4, Math.min(14, stepX * 0.68));
    view.forEach((bar, index) => {
      const x = plot.left + index * stepX + stepX / 2;
      const openY = priceToY(bar.open, top, bottom, plot.top, plotHeight);
      const closeY = priceToY(bar.close, top, bottom, plot.top, plotHeight);
      const highY = priceToY(bar.high, top, bottom, plot.top, plotHeight);
      const lowY = priceToY(bar.low, top, bottom, plot.top, plotHeight);
      const up = bar.close >= bar.open;
      ctx.strokeStyle = up ? '#0f766e' : '#b91c1c';
      ctx.fillStyle = up ? 'rgba(15, 118, 110, 0.8)' : 'rgba(185, 28, 28, 0.78)';
      ctx.lineWidth = 1.2;
      ctx.beginPath();
      ctx.moveTo(x, highY);
      ctx.lineTo(x, lowY);
      ctx.stroke();
      const bodyTop = Math.min(openY, closeY);
      const bodyHeight = Math.max(2, Math.abs(openY - closeY));
      ctx.fillRect(x - candleWidth / 2, bodyTop, candleWidth, bodyHeight);
    });
  }

  if (includeVwap) {
    ctx.strokeStyle = '#2563eb';
    ctx.lineWidth = 1.8;
    ctx.beginPath();
    let started = false;
    view.forEach((bar, index) => {
      if (!Number.isFinite(Number(bar.vwap))) return;
      const x = plot.left + index * stepX + stepX / 2;
      const y = priceToY(Number(bar.vwap), top, bottom, plot.top, plotHeight);
      if (!started) {
        ctx.moveTo(x, y);
        started = true;
      } else {
        ctx.lineTo(x, y);
      }
    });
    ctx.stroke();
  }

  const signalMap = new Map();
  chartSignalsForCurrentView().forEach(signal => {
    const list = signalMap.get(signal.ts) || [];
    list.push(signal);
    signalMap.set(signal.ts, list);
  });
  const selected = state.signals.find(item => item.id === state.selectedSignalId);
  view.forEach((bar, index) => {
    const signalsAtBar = signalMap.get(bar.ts) || [];
    if (!signalsAtBar.length) return;
    const x = plot.left + index * stepX + stepX / 2;
    signalsAtBar.forEach((signal, offsetIndex) => {
      const anchor = signal.direction === 'down' ? bar.high : bar.low;
      const offset = signal.direction === 'down' ? -12 - (offsetIndex * 12) : 12 + (offsetIndex * 12);
      const y = priceToY(anchor, top, bottom, plot.top, plotHeight) + offset;
      ctx.fillStyle = signal.direction === 'down' ? '#b91c1c' : '#0f766e';
      ctx.font = '14px "IBM Plex Sans", "Noto Sans SC", sans-serif';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      const glyph = signal.direction === 'down' ? '▼' : '▲';
      ctx.fillText(glyph, x, y);
      if (selected && selected.id === signal.id) {
        ctx.strokeStyle = '#f59e0b';
        ctx.lineWidth = 2;
        ctx.beginPath();
        ctx.arc(x, y, 8, 0, Math.PI * 2);
        ctx.stroke();
      }
    });
  });

  ctx.strokeStyle = '#d1c4b0';
  ctx.lineWidth = 1;
  ctx.strokeRect(plot.left, plot.top, plotWidth, plotHeight);
}

async function loadSession() {
  try {
    state.me = await fetchJSON('/api/me');
  } catch {
    state.me = null;
  }
  renderSession();
}

async function refreshRuntime() {
  state.runtime = await fetchJSON('/api/runtime/status');
  renderRuntime();
}

async function refreshConfig() {
  const requests = [
    fetchJSON('/api/stocks/global'),
    fetchJSON('/api/stocks/mine'),
    fetchJSON('/api/rules/default'),
    fetchJSON('/api/rules/mine'),
    fetchJSON('/api/notifications/mine'),
  ];
  if (state.me?.user.role === 'admin') {
    requests.push(fetchJSON('/api/forward-metrics'));
  }
  const [globalStocks, myStocks, defaultRules, myRules, notifications, forwardMetrics] = await Promise.all(requests);
  state.globalStocks = globalStocks;
  state.myStocks = myStocks;
  state.defaultRules = defaultRules;
  state.myRules = myRules;
  state.notifications = notifications;
  if (forwardMetrics) {
    state.forwardMetrics = forwardMetrics;
  }
  renderStocks();
  renderRules();
  renderForwardMetricsConfig();
  renderNotifications();
}

function buildSignalQuery() {
  const params = new URLSearchParams();
  const text = document.getElementById('signalTextFilter')?.value.trim() || '';
  const symbol = document.getElementById('signalSymbolFilter')?.value || '';
  const tf = document.getElementById('signalTfFilter')?.value || '';
  params.set('limit', String(state.signalPage.limit || 100));
  params.set('offset', String(state.signalPage.offset || 0));
  if (text) params.set('text', text);
  if (symbol) params.set('symbol', symbol);
  if (tf) params.set('tf', tf);
  return params.toString();
}

async function refreshSignals(options = {}) {
  if (options.resetOffset) {
    state.signalPage.offset = 0;
  }
  const query = buildSignalQuery();
  const [signalsPage, marketSnapshots] = await Promise.all([
    fetchJSON(`/api/signals?${query}`),
    fetchJSON('/api/market-snapshots'),
  ]);
  state.allSignals = signalsPage?.items || [];
  state.signals = state.allSignals;
  state.signalPage.total = Number(signalsPage?.total || 0);
  state.signalPage.limit = Number(signalsPage?.limit || state.signalPage.limit || 100);
  state.signalPage.offset = Number(signalsPage?.offset || 0);
  state.signalPage.hasMore = Boolean(signalsPage?.has_more);
  state.marketSnapshots = marketSnapshots || {};
  if (state.selectedSignalId && !state.signals.some(item => item.id === state.selectedSignalId)) {
    state.selectedSignalId = null;
  }
  renderSignals();
  if (!state.selectedSignalId && state.signals[0]) {
    selectSignal(state.signals[0].id);
    return;
  }
  drawChart();
}

function applySignalFilters() {
  refreshSignals({ resetOffset: true });
}

function changeSignalPage(step) {
  const limit = Math.max(1, Number(state.signalPage.limit || 100));
  const nextOffset = Math.max(0, Number(state.signalPage.offset || 0) + (step * limit));
  if (nextOffset === state.signalPage.offset) return;
  state.signalPage.offset = nextOffset;
  refreshSignals();
}

async function refreshChart(resetView = false) {
  const symbol = document.getElementById('chartSymbol').value;
  const tf = normalizeTf(document.getElementById('chartTf').value);
  if (!symbol || !tf) {
    state.chartBars = [];
    drawChart();
    return;
  }
  const nextKey = `${symbol}:${tf}`;
  state.chartBars = await fetchJSON(`/api/chart/day-bars?symbol=${encodeURIComponent(symbol)}&tf=${encodeURIComponent(tf)}`);
  if (resetView || state.chartView.key !== nextKey) {
    state.chartView.key = nextKey;
    resetChartView();
  } else {
    const maxStart = Math.max(0, state.chartBars.length - clamp(state.chartView.visibleCount || 80, 24, Math.max(24, state.chartBars.length || 24)));
    state.chartView.startIndex = clamp(state.chartView.startIndex || 0, 0, maxStart);
  }
  requestAnimationFrame(() => drawChart());
}

async function refreshAdmin() {
  if (state.me?.user.role !== 'admin') {
    state.users = [];
    renderUsers();
    return;
  }
  const [users, registration] = await Promise.all([
    fetchJSON('/api/users'),
    fetchJSON('/api/auth/registration'),
  ]);
  state.users = users;
  state.registration = registration;
  renderUsers();
}

async function refreshLabs() {
  const [templates, backtests, analytics, backtestConfig] = await Promise.all([
    fetchJSON('/api/rule-templates'),
    fetchJSON('/api/backtests'),
    fetchJSON('/api/analytics/summary'),
    fetchJSON('/api/backtests/config'),
  ]);
  state.templates = templates;
  state.backtests = backtests;
  state.analytics = analytics;
  state.backtestConfig = backtestConfig;
  state.backtestDetail = backtests[0] ? await fetchJSON(`/api/backtests/${backtests[0].id}`) : null;
  renderBacktestConfig();
  renderTemplates();
  renderBacktests();
  renderAnalytics();
}

async function refreshAll() {
  if (!state.me) return;
  await Promise.all([refreshRuntime(), refreshConfig(), refreshSignals(), refreshLabs(), refreshAdmin()]);
  await refreshChart();
}

async function selectSignal(signalId) {
  state.selectedSignalId = signalId;
  renderSignals();
  const detail = state.signals.find(item => item.id === signalId);
  if (!detail) {
    drawChart();
    return;
  }
  document.getElementById('chartSymbol').value = detail.symbol;
  document.getElementById('chartTf').value = detail.timeframe;
  await refreshChart(true);
}

function mergeDeep(base, override) {
  const output = { ...base };
  Object.keys(override || {}).forEach(key => {
    const source = base ? base[key] : undefined;
    const value = override[key];
    if (source && typeof source === 'object' && !Array.isArray(source) && value && typeof value === 'object' && !Array.isArray(value)) {
      output[key] = mergeDeep(source, value);
    } else {
      output[key] = value;
    }
  });
  return output;
}

function escapeHtml(text) {
  return String(text)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;');
}

function formatNumber(value, digits = 2) {
  const num = Number(value);
  if (!Number.isFinite(num)) return '-';
  return num.toFixed(digits);
}

function formatSignedNumber(value, digits = 2) {
  const num = Number(value);
  if (!Number.isFinite(num)) return '-';
  return `${num >= 0 ? '+' : ''}${num.toFixed(digits)}`;
}

function backtestRuleLabel(ruleKey) {
  return t(`rule.${ruleKey}`) || ruleKey || '-';
}

function backtestSignalText(result) {
  const rule = backtestRuleLabel(result.rule_key);
  const direction = t(`direction.${result.direction || 'neutral'}`);
  if (state.language === 'zh') {
    return result.signal_message || `${rule}触发`;
  }
  return `${rule} triggered (${direction})`;
}

function renderBacktestConfig() {
  const cfg = state.backtestConfig || {};
  const criteria = cfg.criteria || {};
  const fields = [
    ['criteria1mHorizon', criteria['1m']?.horizon_minutes ?? 15],
    ['criteria1mMove', criteria['1m']?.min_move_pct ?? 0.5],
    ['criteria5mHorizon', criteria['5m']?.horizon_minutes ?? 60],
    ['criteria5mMove', criteria['5m']?.min_move_pct ?? 1.5],
  ];
  fields.forEach(([id, value]) => {
    const input = document.getElementById(id);
    if (input && !input.value) input.value = value;
  });
}

function formatLoginError(error) {
  const payload = error?.payload || {};
  if (payload.error_code === 'account_locked') {
    const until = payload.locked_until ? new Date(payload.locked_until).toLocaleString() : '-';
    return t('auth.account_locked', { until });
  }
  if (payload.error_code === 'invalid_credentials') {
    if (typeof payload.remaining_attempts === 'number') {
      return t('auth.invalid_credentials', { remaining: payload.remaining_attempts });
    }
    return t('auth.invalid_credentials_simple');
  }
  return error?.message || t('auth.invalid_credentials_simple');
}

function formatRegistrationError(error) {
  const payload = error?.payload || {};
  if (payload.error_code === 'registration_disabled') {
    return t('register.disabled');
  }
  if (payload.error_code === 'registration_limit_reached') {
    return t('register.limitReached', { max: payload.max_users ?? state.registration.max_users ?? 0 });
  }
  if (payload.error_code === 'username_taken') {
    return t('common.usernameTaken');
  }
  if (payload.error_code === 'invalid_registration_data') {
    return t('register.invalid');
  }
  return error?.message || t('register.invalid');
}

function formatCommonActionError(error) {
  const payload = error?.payload || {};
  if (payload.error_code === 'watchlist_limit_reached') return t('common.watchlistLimitReached');
  if (payload.error_code === 'registration_limit_reached') return t('common.registrationLimitReached');
  if (payload.error_code === 'username_taken') return t('common.usernameTaken');
  return error?.message || t('common.notAvailable');
}

async function loadRegistration() {
  try {
    state.registration = await fetchJSON('/api/auth/registration');
  } catch {
    state.registration = {
      enabled: false,
      max_users: 0,
      registered_users: 0,
      remaining_slots: 0,
      limit_reached: true,
    };
  }
  renderRegistrationCard();
}

function attachChartInteractions() {
  const canvas = document.getElementById('chartCanvas');
  if (!canvas || canvas.dataset.bound === '1') return;
  canvas.dataset.bound = '1';

  canvas.addEventListener('mousedown', event => {
    state.chartView.dragging = true;
    state.chartView.dragOriginX = event.clientX;
    state.chartView.dragOriginStart = state.chartView.startIndex || 0;
    canvas.classList.add('dragging');
  });

  window.addEventListener('mouseup', () => {
    state.chartView.dragging = false;
    canvas.classList.remove('dragging');
  });

  window.addEventListener('mousemove', event => {
    if (!state.chartView.dragging || !state.chartBars.length) return;
    const rect = canvas.getBoundingClientRect();
    const plotWidth = Math.max(1, rect.width - 88);
    const visibleCount = clamp(state.chartView.visibleCount || 80, 24, Math.max(24, state.chartBars.length));
    const pixelsPerBar = plotWidth / Math.max(1, visibleCount);
    const deltaBars = Math.round((state.chartView.dragOriginX - event.clientX) / Math.max(4, pixelsPerBar));
    const maxStart = Math.max(0, state.chartBars.length - visibleCount);
    state.chartView.startIndex = clamp(state.chartView.dragOriginStart + deltaBars, 0, maxStart);
    drawChart();
  });

  canvas.addEventListener('mouseleave', () => {
    state.chartView.dragging = false;
    canvas.classList.remove('dragging');
  });

  canvas.addEventListener('wheel', event => {
    if (!state.chartBars.length) return;
    event.preventDefault();
    const previousCount = clamp(state.chartView.visibleCount || 80, 24, Math.max(24, state.chartBars.length));
    const nextCount = clamp(previousCount + (event.deltaY < 0 ? -12 : 12), 24, Math.max(24, state.chartBars.length));
    if (nextCount === previousCount) return;
    const rect = canvas.getBoundingClientRect();
    const pointerRatio = rect.width ? clamp((event.clientX - rect.left - 68) / Math.max(1, rect.width - 88), 0, 1) : 0.5;
    const focusIndex = (state.chartView.startIndex || 0) + Math.round(previousCount * pointerRatio);
    state.chartView.visibleCount = nextCount;
    const maxStart = Math.max(0, state.chartBars.length - nextCount);
    state.chartView.startIndex = clamp(focusIndex - Math.round(nextCount * pointerRatio), 0, maxStart);
    localStorage.setItem('chart.visibleCount', String(nextCount));
    drawChart();
  }, { passive: false });

  window.addEventListener('resize', () => {
    if (document.getElementById('view-monitor')?.classList.contains('active')) {
      requestAnimationFrame(() => drawChart());
    }
  });
}

function bindEvents() {
  document.getElementById('langZhBtn').addEventListener('click', () => setLanguage('zh'));
  document.getElementById('langEnBtn').addEventListener('click', () => setLanguage('en'));
  document.getElementById('manualLink').addEventListener('click', event => {
    event.preventDefault();
    window.open('/manual', '_blank', 'noopener');
  });

  document.getElementById('loginForm').addEventListener('submit', async event => {
    event.preventDefault();
    const payload = {
      username: document.getElementById('loginUsername').value.trim(),
      password: document.getElementById('loginPassword').value,
    };
    try {
      await fetchJSON('/api/auth/login', { method: 'POST', body: JSON.stringify(payload) });
      document.getElementById('loginError').textContent = '';
      await loadSession();
      await refreshAll();
    } catch (error) {
      document.getElementById('loginError').textContent = formatLoginError(error);
    }
  });

  document.getElementById('registerForm').addEventListener('submit', async event => {
    event.preventDefault();
    const username = document.getElementById('registerUsername').value.trim();
    const password = document.getElementById('registerPassword').value;
    const errorBox = document.getElementById('registerError');
    const successBox = document.getElementById('registerSuccess');
    if (!username || !password) {
      errorBox.textContent = t('register.invalid');
      successBox.textContent = '';
      return;
    }
    try {
      await fetchJSON('/api/auth/register', {
        method: 'POST',
        body: JSON.stringify({ username, password }),
      });
      errorBox.textContent = '';
      successBox.textContent = t('register.success');
      document.getElementById('loginUsername').value = username;
      document.getElementById('loginPassword').value = password;
      document.getElementById('registerForm').reset();
      await loadRegistration();
    } catch (error) {
      successBox.textContent = '';
      errorBox.textContent = formatRegistrationError(error);
      await loadRegistration();
    }
  });

  document.getElementById('logoutBtn').addEventListener('click', async () => {
    await fetchJSON('/api/auth/logout', { method: 'POST' });
    state.me = null;
    state.selectedSignalId = null;
    await loadRegistration();
    renderSession();
    state.chartBars = [];
    drawChart();
  });

  document.querySelectorAll('.nav-btn').forEach(btn => btn.addEventListener('click', () => setActiveView(btn.dataset.view)));
  document.getElementById('refreshAllBtn').addEventListener('click', refreshAll);
  document.getElementById('refreshRuntimeBtn').addEventListener('click', refreshRuntime);
  document.getElementById('refreshChartBtn').addEventListener('click', () => refreshChart(false));
  document.getElementById('toggleChartPanelBtn').addEventListener('click', () => {
    state.chartView.collapsed = !state.chartView.collapsed;
    localStorage.setItem('chart.collapsed', state.chartView.collapsed ? '1' : '0');
    renderChartPanel();
    if (!state.chartView.collapsed) {
      requestAnimationFrame(() => drawChart());
    }
  });
  document.getElementById('signalTextFilter').addEventListener('input', applySignalFilters);
  document.getElementById('signalSymbolFilter').addEventListener('change', applySignalFilters);
  document.getElementById('signalTfFilter').addEventListener('change', applySignalFilters);
  document.getElementById('chartSymbol').addEventListener('change', () => refreshChart(true));
  document.getElementById('chartTf').addEventListener('change', () => refreshChart(true));
  document.getElementById('chartMode').addEventListener('change', event => {
    state.chartView.mode = event.currentTarget.value;
    localStorage.setItem('chart.mode', state.chartView.mode);
    drawChart();
  });
  document.getElementById('chartShowVwap').addEventListener('change', event => {
    state.chartView.showVwap = event.currentTarget.checked;
    localStorage.setItem('chart.showVwap', state.chartView.showVwap ? '1' : '0');
    drawChart();
  });
  document.getElementById('resetChartViewBtn').addEventListener('click', () => {
    resetChartView();
    drawChart();
  });

  document.getElementById('globalStockForm').addEventListener('submit', async event => {
    event.preventDefault();
    const form = event.currentTarget;
    await fetchJSON('/api/stocks/global', {
      method: 'POST',
      body: JSON.stringify({
        symbol: form.symbol.value.trim(),
        symbol_name: form.symbol_name.value.trim(),
        enabled: true,
      }),
    });
    form.reset();
    await refreshConfig();
  });

  document.getElementById('myStockForm').addEventListener('submit', async event => {
    event.preventDefault();
    const form = event.currentTarget;
    const errorBox = document.getElementById('myStockError');
    try {
      await fetchJSON('/api/stocks/mine', {
        method: 'POST',
        body: JSON.stringify({
          symbol: form.symbol.value.trim(),
          symbol_name: form.symbol_name.value.trim(),
          enabled: true,
        }),
      });
      errorBox.textContent = '';
      form.reset();
      await refreshConfig();
    } catch (error) {
      errorBox.textContent = formatCommonActionError(error);
    }
  });

  document.getElementById('saveRulesBtn').addEventListener('click', async () => {
    const parsed = JSON.parse(document.getElementById('myRulesEditor').value || '{}');
    await fetchJSON('/api/rules/mine', {
      method: 'PUT',
      body: JSON.stringify(parsed),
    });
    state.myRules = parsed;
    renderRules();
  });

  document.getElementById('forwardMetricsForm').addEventListener('submit', async event => {
    event.preventDefault();
    const form = event.currentTarget;
    const payload = {
      '1m_horizon_minutes': Number(form.forward_metric_1m.value),
      '5m_horizon_minutes': Number(form.forward_metric_5m.value),
    };
    state.forwardMetrics = await fetchJSON('/api/forward-metrics', {
      method: 'PUT',
      body: JSON.stringify(payload),
    });
    renderForwardMetricsConfig();
    alert(t('common.saved'));
  });

  document.getElementById('notificationForm').mode.addEventListener('change', () => {
    syncNotificationForm();
  });

  document.getElementById('notificationForm').addEventListener('submit', async event => {
    event.preventDefault();
    const form = event.currentTarget;
    const isAdmin = state.me?.user.role === 'admin';
    await fetchJSON('/api/notifications/mine', {
      method: 'PUT',
      body: JSON.stringify({
        mode: form.mode.value,
        telegram_token: isAdmin ? form.telegram_token.value.trim() : '',
        telegram_chat_id: isAdmin ? form.telegram_chat_id.value.trim() : '',
        bell_on_alert: form.bell_on_alert.checked,
      }),
    });
    await refreshConfig();
  });

  document.getElementById('applyConfigBtn').addEventListener('click', async () => {
    await fetchJSON('/api/runtime/apply-config', { method: 'POST' });
    await refreshAll();
  });

  document.getElementById('createUserForm').addEventListener('submit', async event => {
    event.preventDefault();
    const form = event.currentTarget;
    const errorBox = document.getElementById('createUserError');
    try {
      await fetchJSON('/api/users', {
        method: 'POST',
        body: JSON.stringify({
          username: form.username.value.trim(),
          password: form.password.value,
          role: form.role.value,
        }),
      });
      errorBox.textContent = '';
      form.reset();
      await refreshAdmin();
      await loadRegistration();
      alert(t('common.saved'));
    } catch (error) {
      errorBox.textContent = formatCommonActionError(error);
    }
  });

  document.getElementById('templateForm').addEventListener('submit', async event => {
    event.preventDefault();
    const form = event.currentTarget;
    await fetchJSON('/api/rule-templates', {
      method: 'POST',
      body: JSON.stringify({
        name: form.name.value.trim(),
        description: form.description.value.trim(),
        config: JSON.parse(document.getElementById('templateConfigEditor').value || '{}'),
      }),
    });
    form.reset();
    await refreshLabs();
  });

  document.getElementById('backtestForm').addEventListener('submit', async event => {
    event.preventDefault();
    const form = event.currentTarget;
    const errorBox = document.getElementById('backtestError');
    const symbol = form.symbol.value.trim();
    const date = form.date.value.trim();
    const timeframes = Array.from(form.querySelectorAll('input[name="timeframes"]:checked')).map(node => node.value);
    if (!symbol) {
      errorBox.textContent = t('labs.backtestSymbolRequired');
      return;
    }
    if (!date) {
      errorBox.textContent = t('labs.backtestDateRequired');
      return;
    }
    if (!timeframes.length) {
      errorBox.textContent = t('labs.backtestTimeframeRequired');
      return;
    }
    const criteria = {
      '1m': {
        horizon_minutes: Number(document.getElementById('criteria1mHorizon').value),
        min_move_pct: Number(document.getElementById('criteria1mMove').value),
      },
      '5m': {
        horizon_minutes: Number(document.getElementById('criteria5mHorizon').value),
        min_move_pct: Number(document.getElementById('criteria5mMove').value),
      },
    };
    const rules = mergeDeep(JSON.parse(JSON.stringify(state.defaultRules || {})), state.myRules || {});
    try {
      errorBox.textContent = '';
      await fetchJSON('/api/backtests', {
        method: 'POST',
        body: JSON.stringify({
          symbol,
          date,
          timeframes,
          criteria,
          rules,
        }),
      });
      await refreshLabs();
    } catch (error) {
      errorBox.textContent = error?.message || t('labs.backtestError', { error: '' });
    }
  });
}

async function boot() {
  bindEvents();
  attachChartInteractions();
  setLanguage(state.language);
  setActiveView('monitor');
  await loadRegistration();
  await loadSession();
  if (state.me) {
    await refreshAll();
  }
  setInterval(() => {
    if (state.me) {
      refreshRuntime();
      refreshSignals();
      refreshLabs();
    }
  }, 20000);
}

boot();
