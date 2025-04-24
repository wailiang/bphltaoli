#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
套利引擎模块

实现资金费率套利策略的核心逻辑
"""

import asyncio
import logging
import time
import os
import json
import sys  # 添加sys模块导入
from typing import Dict, List, Optional, Any
from datetime import datetime

# 尝试使用包内相对导入（当作为包导入时）
try:
    from funding_arbitrage_bot.exchanges.backpack_api import BackpackAPI
    from funding_arbitrage_bot.exchanges.hyperliquid_api import HyperliquidAPI
    from funding_arbitrage_bot.exchanges.coinex_api import CoinExAPI
    from funding_arbitrage_bot.core.data_manager import DataManager
    from funding_arbitrage_bot.utils.display_manager import DisplayManager
    from funding_arbitrage_bot.utils.webhook_alerter import WebhookAlerter
    from funding_arbitrage_bot.utils.helpers import (
        calculate_funding_diff,
        get_backpack_symbol,
        get_hyperliquid_symbol,
        get_coinex_symbol
    )
# 当直接运行时尝试使用直接导入
except ImportError:
    try:
        from exchanges.backpack_api import BackpackAPI
        from exchanges.hyperliquid_api import HyperliquidAPI
        from exchanges.coinex_api import CoinExAPI
        from core.data_manager import DataManager
        from utils.display_manager import DisplayManager
        from utils.webhook_alerter import WebhookAlerter
        from utils.helpers import (
            calculate_funding_diff,
            get_backpack_symbol,
            get_hyperliquid_symbol,
            get_coinex_symbol
        )
    # 如果以上都失败，尝试相对导入（当在包内运行时）
    except ImportError:
        from ..exchanges.backpack_api import BackpackAPI
        from ..exchanges.hyperliquid_api import HyperliquidAPI
        from ..exchanges.coinex_api import CoinExAPI
        from ..core.data_manager import DataManager
        from ..utils.display_manager import DisplayManager
        from ..utils.webhook_alerter import WebhookAlerter
        from ..utils.helpers import (
            calculate_funding_diff,
            get_backpack_symbol,
            get_hyperliquid_symbol,
            get_coinex_symbol
        )


class ArbitrageEngine:
    """套利引擎类，负责执行套利策略"""

    def __init__(
        self,
        config: Dict[str, Any],
        backpack_api: BackpackAPI,
        hyperliquid_api: HyperliquidAPI,
        coinex_api: CoinExAPI,
        logger: Optional[logging.Logger] = None
    ):
        """
        初始化套利引擎

        Args:
            config: 配置字典
            backpack_api: Backpack API实例
            hyperliquid_api: Hyperliquid API实例
            logger: 日志记录器
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)

        # 初始化API实例
        self.backpack_api = backpack_api
        self.hyperliquid_api = hyperliquid_api
        self.coinex_api = coinex_api

        # 初始化数据管理器
        self.data_manager = DataManager(
            backpack_api=backpack_api,
            hyperliquid_api=hyperliquid_api,
            coinex_api=coinex_api,
            symbols=config["strategy"]["symbols"],
            funding_update_interval=config["strategy"]["funding_update_interval"],
            logger=self.logger)

        # 先不初始化显示管理器，等start方法中再初始化
        self.display_manager = None

        # 设置策略参数
        strategy_config = config["strategy"]
        open_conditions = strategy_config.get("open_conditions", {})
        close_conditions = strategy_config.get("close_conditions", {})

        # 从open_conditions中获取min_funding_diff（新配置结构）
        self.arb_threshold = open_conditions.get("min_funding_diff", 0.00001)

        # 如果open_conditions不存在或min_funding_diff不在其中，尝试从旧的配置结构获取
        if self.arb_threshold == 0.00001 and "min_funding_diff" in strategy_config:
            self.arb_threshold = strategy_config["min_funding_diff"]
            self.logger.warning("使用旧配置结构中的min_funding_diff参数")

        self.position_sizes = strategy_config.get("position_sizes", {})
        self.max_position_time = close_conditions.get(
            "max_position_time", 28800)  # 默认8小时
        self.trading_pairs = strategy_config.get("trading_pairs", [])

        # 价差参数 - 从新配置结构获取
        self.min_price_diff_percent = open_conditions.get(
            "min_price_diff_percent", 0.2)
        self.max_price_diff_percent = open_conditions.get(
            "max_price_diff_percent", 1.0)

        # 获取开仓和平仓条件类型
        self.open_condition_type = open_conditions.get(
            "condition_type", "funding_only")
        self.close_condition_type = close_conditions.get(
            "condition_type", "any")

        # 平仓条件参数
        self.funding_diff_sign_change = close_conditions.get(
            "funding_diff_sign_change", True)
        self.min_profit_percent = close_conditions.get(
            "min_profit_percent", 0.1)
        self.max_loss_percent = close_conditions.get("max_loss_percent", 0.3)
        self.close_min_funding_diff = close_conditions.get(
            "min_funding_diff", self.arb_threshold / 2)

        # 初始化价格和资金费率数据
        self.prices = {}
        self.funding_rates = {}
        self.positions = {}
        self.positions_lock = asyncio.Lock()

        # 初始化交易对映射
        self.symbol_mapping = {}

        # 资金费率符号记录文件路径
        self.funding_signs_file = os.path.join(
            os.path.dirname(
                os.path.dirname(
                    os.path.dirname(
                        os.path.abspath(__file__)))),
            'data',
            'funding_diff_signs.json'
        )

        # 确保data目录存在
        os.makedirs(os.path.dirname(self.funding_signs_file), exist_ok=True)

        # 添加开仓时的资金费率符号记录 - 从文件加载
        self.funding_diff_signs = self._load_funding_diff_signs()
        self.logger.info(f"从文件加载资金费率符号记录: {self.funding_diff_signs}")

        # 新增：持仓方向记录字典
        self.position_directions = {}

        # 新增：开仓时的资金费率和价格记录
        self.entry_funding_rates = {}
        self.entry_prices = {}

        # 新增：开仓时间记录
        self.position_open_times = {}

        # 新增：交易快照文件路径
        self.snapshots_dir = os.path.join(
            os.path.dirname(
                os.path.dirname(
                    os.path.dirname(
                        os.path.abspath(__file__)))),
            'data',
            'positions'
        )
        os.makedirs(self.snapshots_dir, exist_ok=True)

        # 初始化事件循环和任务列表
        self.loop = asyncio.get_event_loop()
        self.tasks = []

        # 获取更新间隔
        update_intervals = strategy_config.get("update_intervals", {})
        self.price_update_interval = update_intervals.get("price", 1)
        self.funding_update_interval = update_intervals.get("funding", 60)
        self.position_update_interval = update_intervals.get("position", 10)
        self.check_interval = update_intervals.get("check", 5)

        # 初始化统计数据
        self.stats = {
            "total_trades": 0,
            "successful_trades": 0,
            "failed_trades": 0,
            "total_profit": 0,
            "start_time": None,
            "last_trade_time": None
        }

        # 初始化停止事件
        self.stop_event = asyncio.Event()

        # 打印配置摘要
        self.logger.info(f"套利引擎初始化完成，套利阈值: {self.arb_threshold}")
        self.logger.info(f"交易对: {self.trading_pairs}")
        self.logger.info(
            f"价差参数 - 最小: {self.min_price_diff_percent}%, 最大: {self.max_price_diff_percent}%")
        self.logger.info(
            f"开仓条件类型: {self.open_condition_type}, 平仓条件类型: {self.close_condition_type}")

        # 持仓同步参数
        self.position_sync_interval = config.get(
            "strategy", {}).get(
            "position_sync_interval", 300)  # 默认5分钟同步一次
        self.last_sync_time = 0

        # 运行标志
        self.is_running = False

        # 初始化报警管理器
        order_hook_url = config.get(
            "notification", {}).get("order_webhook_url")
        if order_hook_url:
            self.alerter = WebhookAlerter(order_hook_url)
            self.logger.info(f"已配置订单通知Webhook: {order_hook_url}")
        else:
            self.alerter = None
            self.logger.info("未配置订单通知")

    def _load_funding_diff_signs(self) -> Dict[str, int]:
        """
        从文件加载资金费率符号记录

        Returns:
            Dict[str, int]: 资金费率符号记录字典
        """
        try:
            if os.path.exists(self.funding_signs_file):
                with open(self.funding_signs_file, 'r') as f:
                    # 从文件中读取的是字符串形式的字典，需要将键的字符串形式转换为整数
                    signs_data = json.load(f)
                    # 确保符号值是整数类型
                    return {symbol: int(sign)
                            for symbol, sign in signs_data.items()}
            return {}
        except Exception as e:
            self.logger.error(f"加载资金费率符号记录文件失败: {e}")
            return {}

    def _save_funding_diff_signs(self) -> None:
        """
        将资金费率符号记录保存到文件
        """
        try:
            with open(self.funding_signs_file, 'w') as f:
                json.dump(self.funding_diff_signs, f)
            self.logger.debug(f"资金费率符号记录已保存到文件: {self.funding_signs_file}")
        except Exception as e:
            self.logger.error(f"保存资金费率符号记录到文件失败: {e}")
    
    def _save_position_snapshot(self, symbol, action, bp_position, hl_position, bp_price, hl_price, bp_funding, hl_funding):
        """
        保存开仓或平仓快照到文件
        
        Args:
             symbol: 币种
            action: 操作类型，"open"或"close"
            bp_position: BP持仓信息
            hl_position: HL持仓信息
            cx_position: CoinEx持仓信息
            bp_price: BP价格
            hl_price: HL价格
            cx_price: CoinEx价格
            bp_funding: BP资金费率
            hl_funding: HL资金费率
            cx_funding: CoinEx资金费率
        """
        try:
            # 准备快照数据
            timestamp = time.time()
            formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))

             # 计算价格差异
            bp_hl_price_diff = bp_price - hl_price
            bp_cx_price_diff = bp_price - cx_price
            hl_cx_price_diff = hl_price - cx_price

            # 计算价格差异百分比
            bp_hl_price_diff_percent = (bp_hl_price_diff / hl_price) * 100 if hl_price != 0 else 0
            bp_cx_price_diff_percent = (bp_cx_price_diff / cx_price) * 100 if cx_price != 0 else 0
            hl_cx_price_diff_percent = (hl_cx_price_diff / cx_price) * 100 if cx_price != 0 else 0

             # 计算资金费率差异
            bp_hl_funding_diff = bp_funding - hl_funding
            bp_cx_funding_diff = bp_funding - cx_funding
            hl_cx_funding_diff = hl_funding - cx_funding
            
            # 获取持仓方向
            bp_side = bp_position.get("side", "UNKNOWN") if bp_position else "UNKNOWN"
            hl_side = hl_position.get("side", "UNKNOWN") if hl_position else "UNKNOWN"
            cx_side = cx_position.get("side", "UNKNOWN") if cx_position else "UNKNOWN"
            
            # 获取持仓数量
            bp_size = bp_position.get("size", 0) if bp_position else 0
            if "quantity" in bp_position and not bp_size:
                bp_size = bp_position.get("quantity", 0)
            hl_size = hl_position.get("size", 0) if hl_position else 0
            cx_size = cx_position.get("size", 0) if cx_position else 0
            if "quantity" in cx_position and not cx_size:
                cx_size = cx_position.get("quantity", 0)
            
            snapshot = {
                "symbol": symbol,
                "action": action,
                "timestamp": timestamp,
                "formatted_time": formatted_time,
                "bp_side": bp_side,
                "hl_side": hl_side,
                "cx_side": cx_side,
                "bp_size": bp_size,
                "hl_size": hl_size,
                "cx_size": cx_size,
                "bp_price": bp_price,
                "hl_price": hl_price,
                "cx_price": cx_price,
                "bp_hl_price_diff": bp_hl_price_diff,
                "bp_cx_price_diff": bp_cx_price_diff,
                "hl_cx_price_diff": hl_cx_price_diff,
                "bp_hl_price_diff_percent": bp_hl_price_diff_percent,
                "bp_cx_price_diff_percent": bp_cx_price_diff_percent,
                "hl_cx_price_diff_percent": hl_cx_price_diff_percent,
                "bp_funding": bp_funding,
                "hl_funding": hl_funding,
                "cx_funding": cx_funding,
                "bp_hl_funding_diff": bp_hl_funding_diff,
                "bp_cx_funding_diff": bp_cx_funding_diff,
                "hl_cx_funding_diff": hl_cx_funding_diff
            }
            
            # 保存到文件
            filename = f"{self.snapshots_dir}/{symbol}_{action}_{int(timestamp)}.json"
            with open(filename, "w") as f:
                json.dump(snapshot, f, indent=2)
                
            self.logger.info(f"{symbol} - 已保存{action}快照到文件: {filename}")
                
        except Exception as e:
            self.logger.error(f"{symbol} - 保存{action}快照失败: {str(e)}")

    async def start(self):
        """启动套利引擎"""
        try:
            self.logger.info("正在启动套利引擎...")

            # 添加调试输出
            print("==== 套利引擎启动 ====", file=sys.__stdout__)
            print(
                f"策略配置: {self.config.get('strategy', {})}",
                file=sys.__stdout__)
            print(
                f"交易对: {self.config.get('strategy', {}).get('symbols', [])}",
                file=sys.__stdout__)

            # 初始化显示管理器
            print("正在初始化显示管理器...", file=sys.__stdout__)

            # 确保导入了DisplayManager
            try:
                from funding_arbitrage_bot.utils.display_manager import DisplayManager
            except ImportError:
                try:
                    from utils.display_manager import DisplayManager
                except ImportError:
                    print("无法导入DisplayManager", file=sys.__stdout__)
                    raise

            # 创建并启动显示管理器
            self.display_manager = DisplayManager(logger=self.logger)
            print("正在启动显示...", file=sys.__stdout__)
            self.display_manager.start()
            print("显示已启动", file=sys.__stdout__)

            # 启动数据流
            await self.data_manager.start_price_feeds()

            # 设置运行标志
            self.is_running = True

            # 开始主循环
            while self.is_running:
                try:
                    # 更新市场数据显示
                    market_data = self.data_manager.get_all_data()

                    # 获取当前持仓信息
                    bp_positions = await self.backpack_api.get_positions()
                    hl_positions = await self.hyperliquid_api.get_positions()

                    # 获取CoinEx持仓信息（如果已配置）
                    cx_positions = {}
                    if self.coinex_api:
                        try:
                            cx_positions = await self.coinex_api.get_positions()
                            self.logger.info(f"获取到CoinEx持仓信息: {cx_positions}")
                        except Exception as e:
                            self.logger.error(f"获取CoinEx持仓信息失败: {e}")

                    # 添加持仓信息到市场数据中，以便在表格中显示
                    for symbol in market_data:
                        bp_symbol = get_backpack_symbol(symbol)
                        cx_symbol = symbol
                        has_position = (
                            bp_symbol in bp_positions) or (
                            symbol in hl_positions) or (
                            cx_symbol in cx_positions)
                        market_data[symbol]["position"] = has_position

                    # 更新持仓方向信息
                    market_data = self._update_position_direction_info(
                        market_data, bp_positions, hl_positions, cx_positions)

                    # 使用直接的系统输出检查数据
                    print(f"更新市场数据: {len(market_data)}项", file=sys.__stdout__)

                    # 更新显示
                    if self.display_manager:
                        self.display_manager.update_market_data(market_data)
                    else:
                        print("显示管理器未初始化", file=sys.__stdout__)

                    # ===== 批量处理模式 =====
                    # 收集需要开仓和平仓的币种
                    open_candidates = []   # 存储满足开仓条件的币种信息
                    close_candidates = []  # 存储满足平仓条件的币种信息

                    # 检查每个交易对的套利机会，但不立即执行开仓/平仓
                    for symbol in self.config["strategy"]["symbols"]:
                        await self._collect_arbitrage_opportunity(
                            symbol,
                            open_candidates,
                            close_candidates,
                            bp_positions,
                            hl_positions,
                            cx_positions
                        )

                    # 批量执行开仓操作
                    if open_candidates:
                        self.logger.info(
                            f"批量开仓: 共{len(open_candidates)}个币种符合开仓条件")
                        for candidate in open_candidates:
                            try:
                                await self._open_position(
                                    candidate["symbol"],
                                    candidate["funding_diff"],
                                    candidate["bp_funding"],
                                    candidate["hl_funding"],
                                    candidate["available_size"],
                                    candidate["cx_funding"]
                                )
                                # 每次开仓后添加短暂延迟，避免API限制但又不至于等待太久
                                await asyncio.sleep(0.5)
                            except Exception as e:
                                self.logger.error(
                                    f"执行{candidate['symbol']}批量开仓时出错: {e}")

                    # 批量执行平仓操作
                    if close_candidates:
                        self.logger.info(
                            f"批量平仓: 共{len(close_candidates)}个币种符合平仓条件")
                        for candidate in close_candidates:
                            try:
                                await self._close_position(
                                    candidate["symbol"],
                                    candidate["position"]
                                )
                                # 每次平仓后添加短暂延迟，避免API限制但又不至于等待太久
                                await asyncio.sleep(0.5)
                            except Exception as e:
                                self.logger.error(
                                    f"执行{candidate['symbol']}批量平仓时出错: {e}")

                    # 等待下一次检查
                    await asyncio.sleep(self.config["strategy"]["check_interval"])

                except Exception as e:
                    self.logger.error(f"主循环发生错误: {e}")
                    print(f"主循环错误: {e}", file=sys.__stdout__)
                    await asyncio.sleep(5)  # 发生错误时等待5秒

        except Exception as e:
            self.logger.error(f"启动套利引擎时发生错误: {e}")
            print(f"启动引擎错误: {str(e)}", file=sys.__stdout__)
        finally:
            # 停止显示
            if self.display_manager:
                print("停止显示...", file=sys.__stdout__)
                self.display_manager.stop()
                print("显示已停止", file=sys.__stdout__)

    def _analyze_orderbook(self, orderbook, side, amount_usd, price):
        """
        分析订单簿计算滑点

        Args:
            orderbook: 订单簿数据
            side: 'bids'表示买单(做多)，'asks'表示卖单(做空)
            amount_usd: 欲交易的美元金额
            price: 当前市场价格

        Returns:
            float: 百分比形式的滑点
        """
        try:
            # 添加调试日志
            self.logger.debug(
                f"分析订单簿: side={side}, amount_usd={amount_usd}, price={price}")

            # 检查订单簿数据有效性
            if not orderbook:
                self.logger.warning("订单簿数据为空")
                return 0.1  # 默认较高滑点

            if side not in orderbook:
                self.logger.warning(f"订单簿中不存在{side}列表")
                return 0.1  # 默认较高滑点

            if not orderbook[side]:
                self.logger.warning(f"订单簿{side}列表为空")
                return 0.1  # 默认较高滑点

            # 查看数据结构
            sample_item = orderbook[side][0] if orderbook[side] else None
            self.logger.debug(f"订单簿数据样例: {sample_item}")

            # 处理不同交易所可能的数据格式差异
            book_side = []
            for level in orderbook[side]:
                # 对Hyperliquid格式 [{"px": price, "sz": size}, ...] 的处理
                if isinstance(level, dict) and "px" in level and "sz" in level:
                    book_side.append([float(level["px"]), float(level["sz"])])
                # 对Backpack格式 [{"px": price, "sz": size}, ...] 的处理 (已在API中统一)
                elif isinstance(level, dict) and "price" in level and "size" in level:
                    book_side.append(
                        [float(level["price"]), float(level["size"])])
                # 对价格和数量已经是列表 [price, size] 的处理
                elif isinstance(level, list) and len(level) >= 2:
                    book_side.append([float(level[0]), float(level[1])])
                else:
                    self.logger.warning(f"无法识别的订单簿数据格式: {level}")

            # 如果数据转换后为空，返回默认值
            if not book_side:
                self.logger.warning("数据格式转换后订单簿为空")
                return 0.1  # 默认较高滑点

            # 确保订单按价格排序
            if side == 'bids':
                # 买单从高到低
                book_side = sorted(
                    book_side, key=lambda x: float(
                        x[0]), reverse=True)
            else:
                # 卖单从低到高
                book_side = sorted(book_side, key=lambda x: float(x[0]))

            # 记录排序后的前几个价格
            self.logger.debug(
                f"排序后的{side}前5个价格: {[item[0] for item in book_side[:5]]}")

            # 计算滑点
            amount_filled = 0.0
            weighted_price = 0.0

            levels_checked = 0
            for level in book_side:
                level_price = float(level[0])
                level_size = float(level[1])

                # 计算此价格级别的美元价值
                level_value = level_price * level_size
                level_contribution = min(
                    level_value, amount_usd - amount_filled)

                self.logger.debug(
                    f"  深度{levels_checked}: 价格={level_price}, 数量={level_size}, 美元价值={level_value}, 贡献={level_contribution}")

                if amount_filled + level_value >= amount_usd:
                    # 计算需要从此级别填充的部分
                    remaining = amount_usd - amount_filled
                    size_needed = remaining / level_price

                    # 添加到加权平均价格计算
                    weighted_price += level_price * size_needed
                    amount_filled = amount_usd  # 已完全填充

                    self.logger.debug(
                        f"  已填满订单: 剩余={remaining}, 所需数量={size_needed}, 总填充量={amount_filled}")
                    break
                else:
                    # 添加整个级别到加权平均价格
                    weighted_price += level_price * level_size
                    amount_filled += level_value

                    self.logger.debug(f"  部分填充: 累计填充量={amount_filled}")

                levels_checked += 1
                # 只检查前10个深度级别
                if levels_checked >= 10:
                    self.logger.debug("已检查10个深度级别，中断检查")
                    break

            # 如果未能完全填充订单，但已填充超过80%，使用已填充部分计算
            if amount_filled < amount_usd:
                fill_percentage = (amount_filled / amount_usd) * 100
                self.logger.warning(
                    f"未能完全填充订单: 已填充{fill_percentage:.2f}% (${amount_filled:.2f}/${amount_usd:.2f})")

                if fill_percentage >= 80:
                    self.logger.info(
                        f"已填充{fill_percentage:.2f}%，继续使用已填充部分计算滑点")
                else:
                    # 流动性不足，但不要直接返回固定值，而是基于填充比例计算滑点
                    slippage = (100 - fill_percentage) / 100
                    # 限制最大滑点为0.2%
                    slippage = min(0.2, slippage)
                    self.logger.info(f"流动性不足，基于填充比例计算滑点: {slippage:.4f}")
                    return slippage

            # 使用实际填充的金额计算平均价格
            if amount_filled > 0:
                # 原始计算公式有问题，导致加权平均价几乎总是等于市场价
                # average_price = weighted_price / (amount_filled / price)

                # 修正后的计算公式：使用最后处理的level_price作为基准
                average_price = weighted_price / (amount_filled / level_price)
            else:
                self.logger.warning("填充金额为0，无法计算滑点")
                return 0.1  # 默认较高滑点

            # 计算滑点百分比
            if side == 'bids':
                # 买单，滑点 = (市场价 - 加权平均价) / 市场价
                slippage = (price - average_price) / price * 100
            else:
                # 卖单，滑点 = (加权平均价 - 市场价) / 市场价
                slippage = (average_price - price) / price * 100

            # 确保滑点为正值
            slippage = abs(slippage)

            self.logger.info(
                f"计算得到的滑点: {slippage:.4f}%, 市场价: {price}, 加权平均价: {average_price}")

            # 限制最小滑点为0.01%，最大滑点为0.5%
            slippage = max(0.01, min(0.5, slippage))

            return slippage

        except Exception as e:
            import traceback
            self.logger.error(f"分析订单簿计算滑点时出错: {e}")
            self.logger.error(traceback.format_exc())
            return 0.1  # 出错时返回默认滑点

    async def _collect_arbitrage_opportunity(
        self,
        symbol: str,
        open_candidates: list,
        close_candidates: list,
        bp_positions: dict,
        hl_positions: dict,
        cx_positions: dict = None
    ):
        """
        收集套利机会，但不立即执行，而是将满足条件的币种添加到候选列表中

        Args:
            symbol: 基础币种，如 "BTC"
            open_candidates: 存储满足开仓条件的币种信息的列表
            close_candidates: 存储满足平仓条件的币种信息的列表
            bp_positions: Backpack持仓信息
            hl_positions: Hyperliquid持仓信息
            cx_positions: CoinEx持仓信息（可选）
        """
        try:

            # 初始化CoinEx持仓信息（如果未提供）
            if cx_positions is None:
                cx_positions = {}

            # 获取最新数据
            data = await self.data_manager.get_data(symbol)

            # 检查数据有效性
            if not self.data_manager.is_data_valid(symbol):
                self.logger.warning(f"{symbol}数据无效，跳过检查")
                return

            # 提取价格和资金费率
            bp_data = data["backpack"]
            hl_data = data["hyperliquid"]
            cx_data = data["coinex"]

            bp_price = bp_data["price"]
            bp_funding = bp_data["funding_rate"]
            hl_price = hl_data["price"]
            hl_funding = hl_data["funding_rate"]
            cx_price = cx_data["price"]
            cx_funding = cx_data["funding_rate"]

            # 调整Hyperliquid资金费率以匹配Backpack的8小时周期
            adjusted_hl_funding = hl_funding * 8

            # 调整CoinEx资金费率（如果有）
            adjusted_cx_funding = None
            if cx_funding is not None:
                # 根据CoinEx的结算周期调整资金费率
                # 假设CoinEx是8小时结算，如果不是，需要相应调整
                adjusted_cx_funding = cx_funding

            # 计算价格差异（百分比）
            price_diff_percent = (bp_price - hl_price) / hl_price * 100

            # 计算CoinEx与其他交易所的价格差异（如果有CoinEx价格）
            cx_bp_price_diff_percent = None
            cx_hl_price_diff_percent = None
            if cx_price is not None:
                cx_bp_price_diff_percent = (cx_price - bp_price) / bp_price * 100
                cx_hl_price_diff_percent = (cx_price - hl_price) / hl_price * 100

            # 计算资金费率差异
            funding_diff, funding_diff_sign = calculate_funding_diff(
                bp_funding, hl_funding)
            funding_diff_percent = funding_diff * 100  # 转为百分比

            # 计算CoinEx与其他交易所的资金费率差异（如果有CoinEx资金费率）
            cx_bp_funding_diff = None
            cx_bp_funding_diff_sign = 0
            cx_hl_funding_diff = None
            cx_hl_funding_diff_sign = 0

            if adjusted_cx_funding is not None and bp_funding is not None:
                cx_bp_funding_diff, cx_bp_funding_diff_sign = calculate_funding_diff(
                    adjusted_cx_funding, bp_funding)
                
            if adjusted_cx_funding is not None and adjusted_hl_funding is not None:
                cx_hl_funding_diff, cx_hl_funding_diff_sign = calculate_funding_diff(
                    adjusted_cx_funding, adjusted_hl_funding)

            bp_symbol = get_backpack_symbol(symbol)
            cx_symbol = symbol

            has_position = (
                bp_symbol in bp_positions) or (
                symbol in hl_positions) or (
                cx_symbol in cx_positions)

            # 计算滑点信息
            # 确定做多和做空的交易所
            # 这里需要考虑三个交易所的情况，选择资金费率最低和最高的
            # long_exchange = "hyperliquid" if funding_diff_sign < 0 else "backpack"
            # short_exchange = "backpack" if funding_diff_sign < 0 else "hyperliquid"
            exchanges = []
            funding_rates = []

            # 分析订单深度获取滑点信息
            # try:
            #     # 获取交易金额
            #     trade_size_usd = self.config["strategy"].get(
            #         "trade_size_usd", {}).get(symbol, 100)

            #     # 获取Hyperliquid订单深度数据
            #     hl_orderbook = await self.hyperliquid_api.get_orderbook(symbol)
            #     # 获取Backpack订单深度数据
            #     bp_orderbook = await self.backpack_api.get_orderbook(symbol)

            #     # 分析订单簿计算精确滑点
            #     long_slippage = 0.05  # 默认值
            #     short_slippage = 0.05  # 默认值

            #     # 根据做多/做空交易所计算实际滑点
            #     if long_exchange == "hyperliquid":
            #         long_slippage = self._analyze_orderbook(
            #             hl_orderbook, "bids", trade_size_usd, hl_price)
            #     else:  # long_exchange == "backpack"
            #         long_slippage = self._analyze_orderbook(
            #             bp_orderbook, "bids", trade_size_usd, bp_price)

            #     if short_exchange == "hyperliquid":
            #         short_slippage = self._analyze_orderbook(
            #             hl_orderbook, "asks", trade_size_usd, hl_price)
            #     else:  # short_exchange == "backpack"
            #         short_slippage = self._analyze_orderbook(
            #             bp_orderbook, "asks", trade_size_usd, bp_price)

            #     # 计算总滑点
            #     total_slippage = long_slippage + short_slippage

            #     # 将滑点信息添加到市场数据中
            #     market_data = self.data_manager.get_all_data()
            #     if symbol in market_data:
            #         market_data[symbol]["total_slippage"] = total_slippage
            #         market_data[symbol]["long_slippage"] = long_slippage
            #         market_data[symbol]["short_slippage"] = short_slippage
            #         market_data[symbol]["long_exchange"] = long_exchange
            #         market_data[symbol]["short_exchange"] = short_exchange

            #         # 调试输出滑点信息
            #         self.logger.debug(
            #             f"{symbol}滑点分析: 总滑点={total_slippage:.4f}%, 做多({long_exchange})={long_slippage:.4f}%, 做空({short_exchange})={short_slippage:.4f}%")

            #         # 更新显示（仅在计算滑点后）
            #         if self.display_manager:
            #             self.display_manager.update_market_data(market_data)
            # except Exception as e:
            #     self.logger.error(f"计算{symbol}滑点信息时出错: {e}")

            if bp_funding is not None:
                exchanges.append("backpack")
                funding_rates.append(bp_funding)
                
            if adjusted_hl_funding is not None:
                exchanges.append("hyperliquid")
                funding_rates.append(adjusted_hl_funding)
                
            if adjusted_cx_funding is not None:
                exchanges.append("coinex")
                funding_rates.append(adjusted_cx_funding)
            
            # 如果至少有两个交易所有数据，才能进行套利
            if len(exchanges) >= 2:
                # 找出资金费率最低和最高的交易所
                min_idx = funding_rates.index(min(funding_rates))
                max_idx = funding_rates.index(max(funding_rates))
                
                long_exchange = exchanges[min_idx]  # 资金费率低的做多
                short_exchange = exchanges[max_idx]  # 资金费率高的做空
                
                # 分析订单深度获取滑点信息
                try:
                    # 获取交易金额
                    trade_size_usd = self.config["strategy"].get(
                        "trade_size_usd", {}).get(symbol, 100)

                    # 获取各交易所订单深度数据
                    hl_orderbook = await self.hyperliquid_api.get_orderbook(symbol)
                    bp_orderbook = await self.backpack_api.get_orderbook(symbol)
                    
                    # 获取CoinEx订单深度数据（如果API可用）
                    cx_orderbook = None
                    if self.coinex_api:
                        try:
                            cx_orderbook = await self.coinex_api.get_orderbook(symbol)
                        except Exception as e:
                            self.logger.error(f"获取CoinEx {symbol}订单深度失败: {e}")

                    # 分析订单簿计算精确滑点
                    long_slippage = 0.05  # 默认值
                    short_slippage = 0.05  # 默认值

                    # 根据做多/做空交易所计算实际滑点
                    if long_exchange == "hyperliquid":
                        long_slippage = self._analyze_orderbook(
                            hl_orderbook, "bids", trade_size_usd, hl_price)
                    elif long_exchange == "backpack":
                        long_slippage = self._analyze_orderbook(
                            bp_orderbook, "bids", trade_size_usd, bp_price)
                    elif long_exchange == "coinex" and cx_orderbook and cx_price:
                        long_slippage = self._analyze_orderbook(
                            cx_orderbook, "bids", trade_size_usd, cx_price)

                    if short_exchange == "hyperliquid":
                        short_slippage = self._analyze_orderbook(
                            hl_orderbook, "asks", trade_size_usd, hl_price)
                    elif short_exchange == "backpack":
                        short_slippage = self._analyze_orderbook(
                            bp_orderbook, "asks", trade_size_usd, bp_price)
                    elif short_exchange == "coinex" and cx_orderbook and cx_price:
                        short_slippage = self._analyze_orderbook(
                            cx_orderbook, "asks", trade_size_usd, cx_price)

                    # 计算总滑点
                    total_slippage = long_slippage + short_slippage

                    # 将滑点信息添加到市场数据中
                    market_data = self.data_manager.get_all_data()
                    if symbol in market_data:
                        market_data[symbol]["total_slippage"] = total_slippage
                        market_data[symbol]["long_slippage"] = long_slippage
                        market_data[symbol]["short_slippage"] = short_slippage
                        market_data[symbol]["long_exchange"] = long_exchange
                        market_data[symbol]["short_exchange"] = short_exchange

                        # 调试输出滑点信息
                        self.logger.debug(
                            f"{symbol}滑点分析: 总滑点={total_slippage:.4f}%, 做多({long_exchange})={long_slippage:.4f}%, 做空({short_exchange})={short_slippage:.4f}%")

                        # 更新显示（仅在计算滑点后）
                        if self.display_manager:
                            self.display_manager.update_market_data(market_data)
                except Exception as e:
                    self.logger.error(f"计算{symbol}滑点信息时出错: {e}")

            # 记录当前状态和调整后的资金费率
            # self.logger.info(
            #     f"{symbol} - 价格差: {price_diff_percent:.4f}%, "
            #     f"资金费率差: {funding_diff_percent:.6f}%, "
            #     f"BP: {bp_funding:.6f}(8h), HL原始: {hl_funding:.6f}(1h), HL调整后: {adjusted_hl_funding:.6f}(8h), "
            #     f"持仓: {'是' if has_position else '否'}")
                funding_info = (
                    f"{symbol} - 价格差: {price_diff_percent:.4f}%, "
                    f"资金费率差: {funding_diff_percent:.6f}%, "
                    f"BP: {bp_funding:.6f}(8h), HL原始: {hl_funding:.6f}(1h), HL调整后: {adjusted_hl_funding:.6f}(8h)"
                )

                # 添加CoinEx信息（如果有）
                if cx_funding is not None:
                    funding_info += f", CX: {cx_funding:.6f}(8h)"

                funding_info += f", 持仓: {'是' if has_position else '否'}"
                self.logger.info(funding_info)

                if not has_position:
                    # 没有仓位，检查是否满足开仓条件
                    should_open, reason, available_size = self._check_open_conditions_without_execution(
                        symbol,
                        bp_price,
                        hl_price,
                        bp_funding,
                        adjusted_hl_funding,
                        price_diff_percent,
                        funding_diff,
                        bp_positions,
                        hl_positions,
                        cx_price,
                        adjusted_cx_funding,
                        cx_positions
                    )

                    if should_open:
                        self.logger.info(f"{symbol} - 决定纳入批量开仓候选，原因: {reason}")
                        # 将满足开仓条件的币种信息添加到候选列表
                        open_candidates.append({
                            "symbol": symbol,
                            "funding_diff": funding_diff,
                            "bp_funding": bp_funding,
                            "hl_funding": adjusted_hl_funding,
                            "cx_funding": adjusted_cx_funding,
                            "available_size": available_size,
                            "reason": reason
                        })
                    else:
                        self.logger.debug(f"{symbol} - 不满足开仓条件，跳过")
                else:
                    # 有仓位，检查是否满足平仓条件
                    bp_position = bp_positions.get(bp_symbol)
                    hl_position = hl_positions.get(symbol)
                    cx_position = cx_positions.get(cx_symbol)

                    # 检查是否有任何两个交易所之间的仓位
                    if (bp_position and hl_position) or (bp_position and cx_position) or (hl_position and cx_position):
                        # 确定哪两个交易所有仓位
                        position = None
                        if bp_position and hl_position:
                            should_close, reason, position = self._check_close_conditions_without_execution(
                                symbol,
                                bp_position,
                                hl_position,
                                bp_price,
                                hl_price,
                                bp_funding,
                                adjusted_hl_funding,
                                price_diff_percent,
                                funding_diff,
                                funding_diff_sign
                            )
                        elif bp_position and cx_position:
                            # 需要实现BP和CX之间的平仓检查
                            should_close, reason, position = self._check_close_conditions_without_execution(
                                symbol,
                                bp_position,
                                cx_position,
                                bp_price,
                                cx_price,
                                bp_funding,
                                adjusted_cx_funding,
                                cx_bp_price_diff_percent,
                                cx_bp_funding_diff,
                                cx_bp_funding_diff_sign,
                                is_coinex=True
                            )
                        elif hl_position and cx_position:
                            # 需要实现HL和CX之间的平仓检查
                            should_close, reason, position = self._check_close_conditions_without_execution(
                                symbol,
                                hl_position,
                                cx_position,
                                hl_price,
                                cx_price,
                                adjusted_hl_funding,
                                adjusted_cx_funding,
                                cx_hl_price_diff_percent,
                                cx_hl_funding_diff,
                                cx_hl_funding_diff_sign,
                                is_coinex=True
                            )

                        if should_close and position:
                            self.logger.info(f"{symbol} - 决定纳入批量平仓候选，原因: {reason}")
                            # 将满足平仓条件的币种信息添加到候选列表
                            close_candidates.append({
                                "symbol": symbol,
                                "position": position,
                                "reason": reason
                            })
                        else:
                            self.logger.debug(f"{symbol} - 不满足平仓条件，保持持仓")

            # if not has_position:
            #     # 没有仓位，检查是否满足开仓条件
            #     should_open, reason, available_size = self._check_open_conditions_without_execution(
            #         symbol,
            #         bp_price,
            #         hl_price,
            #         bp_funding,
            #         adjusted_hl_funding,
            #         price_diff_percent,
            #         funding_diff,
            #         bp_positions,
            #         hl_positions
            #     )

            #     if should_open:
            #         self.logger.info(f"{symbol} - 决定纳入批量开仓候选，原因: {reason}")
            #         # 将满足开仓条件的币种信息添加到候选列表
            #         open_candidates.append({
            #             "symbol": symbol,
            #             "funding_diff": funding_diff,
            #             "bp_funding": bp_funding,
            #             "hl_funding": adjusted_hl_funding,
            #             "available_size": available_size,
            #             "reason": reason
            #         })
            #     else:
            #         self.logger.debug(f"{symbol} - 不满足开仓条件，跳过")
            # else:
                # 有仓位，检查是否满足平仓条件
                # bp_position = bp_positions.get(bp_symbol)
                # hl_position = hl_positions.get(symbol)

                # if bp_position and hl_position:
                #     should_close, reason, position = self._check_close_conditions_without_execution(
                #         symbol,
                #         bp_position,
                #         hl_position,
                #         bp_price,
                #         hl_price,
                #         bp_funding,
                #         adjusted_hl_funding,
                #         price_diff_percent,
                #         funding_diff,
                #         funding_diff_sign
                #     )

                #     if should_close:
                #         self.logger.info(f"{symbol} - 决定纳入批量平仓候选，原因: {reason}")
                #         # 将满足平仓条件的币种信息添加到候选列表
                #         close_candidates.append({
                #             "symbol": symbol,
                #             "position": position,
                #             "reason": reason
                #         })
                #     else:
                #         self.logger.debug(f"{symbol} - 不满足平仓条件，保持持仓")
        except Exception as e:
           self.logger.error(f"收集{symbol}套利机会异常: {e}", exc_info=True)

    def _check_open_conditions_without_execution(
        self,
        symbol: str,
        bp_price: float,
        hl_price: float,
        bp_funding: float,
        adjusted_hl_funding: float,
        price_diff_percent: float,
        funding_diff: float,
        bp_positions: dict,
        hl_positions: dict,
        cx_price: float = None,
        cx_funding: float = None,
        cx_positions: dict = None
    ):
        """
        检查是否满足开仓条件，但不执行开仓

        Args:
            symbol: 基础币种，如 "BTC"
            bp_price: Backpack价格
            hl_price: Hyperliquid价格
            bp_funding: Backpack资金费率
            adjusted_hl_funding: 调整后的Hyperliquid资金费率
            price_diff_percent: 价格差异（百分比）
            funding_diff: 资金费率差异
            bp_positions: Backpack持仓信息
            hl_positions: Hyperliquid持仓信息
            cx_price: CoinEx价格（可选）
            cx_funding: CoinEx资金费率（可选）
            cx_positions: CoinEx持仓信息（可选）

        Returns:
            tuple: (should_open, reason, available_size)
        """
        # 检查是否已有该币种的持仓
        bp_symbol = get_backpack_symbol(symbol)
        cx_symbol = symbol  # CoinEx使用基础币种作为符号
        has_position = (bp_symbol in bp_positions) or (symbol in hl_positions)

        if cx_positions and cx_symbol in cx_positions:
            has_position = True

        if has_position:
            return False, "已有持仓", 0

        # 检查滑点条件
        open_conditions = self.config.get(
            "strategy", {}).get(
            "open_conditions", {})
        max_slippage = open_conditions.get("max_slippage_percent", 0.15)
        ignore_slippage = open_conditions.get("ignore_high_slippage", False)

        # 获取市场数据中的滑点信息
        market_data = self.data_manager.get_all_data()
        total_slippage = market_data.get(
            symbol, {}).get(
            "total_slippage", 0.5)  # 默认为0.5%，较高值

        # 检查滑点是否超过限制
        slippage_condition_met = ignore_slippage or total_slippage <= max_slippage

        if not slippage_condition_met:
            self.logger.debug(
                f"{symbol} - 预估滑点({total_slippage:.4f}%)超过最大允许值({max_slippage:.4f}%)，暂不纳入开仓候选")
            return False, f"滑点过高({total_slippage:.4f}%)", 0

        # 检查是否达到最大持仓数量限制
        max_positions_count = self.config.get(
            "strategy", {}).get(
            "max_positions_count", 5)

        # 统计不同币种的持仓数量
        position_symbols = set()

        # 统计Backpack持仓币种
        for pos_symbol in bp_positions:
            base_symbol = pos_symbol.split('_')[0]  # 从"BTC_USDC_PERP"中提取"BTC"
            position_symbols.add(base_symbol)

        # 统计Hyperliquid持仓币种
        for pos_symbol in hl_positions:
            position_symbols.add(pos_symbol)

        # 统计CoinEx持仓币种
        if cx_positions:
            for pos_symbol in cx_positions:
                position_symbols.add(pos_symbol)

        current_positions_count = len(position_symbols)

        # 检查是否达到最大持仓数量限制
        if current_positions_count >= max_positions_count:
            self.logger.warning(
                f"已达到全局最大持仓数量限制({max_positions_count})，当前持仓币种: {position_symbols}，跳过{symbol}开仓")
            if self.display_manager:
                self.display_manager.add_order_message(
                    f"已达全局持仓限制({max_positions_count}币种)，跳过{symbol}开仓")
            return False, f"已达到全局最大持仓数量限制({max_positions_count})", None

        # 计算当前持仓的总量，用于检查是否超过最大持仓
        current_size = 0

        # 获取交易对配置，检查最大持仓限制
        trading_pair_config = None
        for pair in self.config.get("trading_pairs", []):
            if pair["symbol"] == symbol:
                trading_pair_config = pair
                break

        if not trading_pair_config:
            return False, f"未找到{symbol}的交易对配置", None

        # 获取最大持仓数量
        max_position_size = trading_pair_config.get("max_position_size", 0)

        # 检查是否超过最大持仓限制
        if current_size >= max_position_size:
            self.logger.warning(
                f"{symbol}当前持仓({current_size})已达到或超过最大持仓限制({max_position_size})，跳过开仓")
            if self.display_manager:
                self.display_manager.add_order_message(
                    f"{symbol}已达最大持仓限制({max_position_size})，跳过开仓")
            return False, f"{symbol}当前持仓({current_size})已达到或超过最大持仓限制({max_position_size})", None

        # 计算可用的剩余开仓量
        available_size = max_position_size - current_size

        # 获取开仓条件配置
        open_conditions = self.config.get(
            "strategy", {}).get(
            "open_conditions", {})
        condition_type = open_conditions.get("condition_type", "funding_only")

        # 检查价格差异条件
        min_price_diff = open_conditions.get("min_price_diff_percent", 0.2)
        max_price_diff = open_conditions.get("max_price_diff_percent", 1.0)
        price_condition_met = min_price_diff <= abs(
            price_diff_percent) <= max_price_diff

        # 检查资金费率差异条件
        min_funding_diff = open_conditions.get("min_funding_diff", 0.00001)
        funding_condition_met = abs(funding_diff) >= min_funding_diff

        # 检查滑点条件
        if not slippage_condition_met:
            self.logger.warning(
                f"{symbol} - 预估滑点({total_slippage:.4f}%)超过最大允许值({max_slippage:.4f}%)，跳过开仓")
            if self.display_manager:
                self.display_manager.add_order_message(
                    f"{symbol}滑点过高({total_slippage:.4f}%)，跳过开仓")
            return False, f"滑点过高({total_slippage:.4f}%)", 0

        # 检查方向一致性
        check_direction_consistency = open_conditions.get(
            "check_direction_consistency", False)
        direction_consistent = True  # 默认方向一致，如果不检查方向一致性，则此条件始终为True
        preferred_bp_side = None
        preferred_hl_side = None

        if check_direction_consistency and price_condition_met and funding_condition_met:
            if cx_price is not None and cx_funding is not None:
                # 如果有CoinEx数据，使用三个交易所的方向一致性检查
                direction_consistent, preferred_sides = self.check_direction_consistency_multi(
                    symbol, bp_price, hl_price, cx_price, bp_funding, adjusted_hl_funding, cx_funding)

                if direction_consistent:
                    preferred_bp_side = preferred_sides.get("bp_side")
                    preferred_hl_side = preferred_sides.get("hl_side")
                    preferred_cx_side = preferred_sides.get("cx_side")

            else:
                # 如果没有CoinEx数据，使用原有的两个交易所方向一致性检查
                direction_consistent, preferred_bp_side, preferred_hl_side = self.check_direction_consistency(
                    symbol, bp_price, hl_price, bp_funding, adjusted_hl_funding)

            # 如果方向一致，使用资金费率套利的方向作为最终开仓方向
            if direction_consistent:
                # 确保preferred_sides字典存在
                if not hasattr(self, 'preferred_sides'):
                    self.preferred_sides = {}

                self.preferred_sides[symbol] = {
                    "bp_side": preferred_bp_side,
                    "hl_side": preferred_hl_side
                }
                
                # 添加CoinEx方向（如果有）
                if preferred_cx_side:
                    self.preferred_sides[symbol]["cx_side"] = preferred_cx_side

        # 根据条件类型决定是否开仓
        should_open = False
        reason = ""

        if condition_type == "any":
            # 满足任一条件即可开仓
            should_open = (
                price_condition_met or funding_condition_met) and direction_consistent
            reason = "满足价格差异或资金费率差异条件"
        elif condition_type == "all":
            # 必须同时满足所有条件才能开仓
            should_open = price_condition_met and funding_condition_met and direction_consistent
            reason = "同时满足价格差异和资金费率差异条件"
        elif condition_type == "funding_only":
            # 仅考虑资金费率条件
            should_open = funding_condition_met and direction_consistent
            reason = "满足资金费率差异条件"
        elif condition_type == "price_only":
            # 仅考虑价格差异条件
            should_open = price_condition_met and direction_consistent
            reason = "满足价格差异条件"

        # 记录条件判断结果
        self.logger.info(
            f"{symbol} - 开仓条件检查: 价格条件{'' if price_condition_met else '未'}满足 "
            f"(差异: {abs(price_diff_percent):.4f}%, 阈值: {min_price_diff}%-{max_price_diff}%), "
            f"资金费率条件{'' if funding_condition_met else '未'}满足 "
            f"(差异: {abs(funding_diff):.6f}, 阈值: {min_funding_diff})"
            f"{', 方向一致性检查' + ('' if direction_consistent else '未') + '通过' if check_direction_consistency else ''}")

        return should_open, reason, available_size

    def check_direction_consistency_multi(
        self,
        symbol: str,
        bp_price: float,
        hl_price: float,
        cx_price: float,
        bp_funding: float,
        adjusted_hl_funding: float,
        cx_funding: float
    ):
        """
        检查三个交易所之间的方向一致性
        
        Args:
            symbol: 基础币种，如 "BTC"
            bp_price: Backpack价格
            hl_price: Hyperliquid价格
            cx_price: CoinEx价格
            bp_funding: Backpack资金费率
            adjusted_hl_funding: 调整后的Hyperliquid资金费率
            cx_funding: CoinEx资金费率
            
        Returns:
            tuple: (方向是否一致, 各交易所的建议方向字典)
        """
        # 获取资金费率配置
        funding_config = self.config.get("funding_rate", {})
        
        # 获取各交易所资金费率权重
        bp_weight = funding_config.get("bp_funding_rate_weight", 1.0)
        hl_weight = funding_config.get("hl_funding_rate_weight", 1.0)
        cx_weight = funding_config.get("cx_funding_rate_weight", 1.0)
        
        # 计算加权资金费率
        weighted_bp_funding = bp_funding * bp_weight
        weighted_hl_funding = adjusted_hl_funding * hl_weight
        weighted_cx_funding = cx_funding * cx_weight
        
        # 找出资金费率最低和最高的交易所
        funding_rates = [
            (weighted_bp_funding, "backpack"),
            (weighted_hl_funding, "hyperliquid"),
            (weighted_cx_funding, "coinex")
        ]
        
        # 按资金费率排序
        funding_rates.sort(key=lambda x: x[0])
        
        # 最低和最高资金费率的交易所
        lowest_funding_exchange = funding_rates[0][1]
        highest_funding_exchange = funding_rates[-1][1]
        
        # 确定各交易所的建议方向
        preferred_sides = {}
        
        # 资金费率低的交易所做多，资金费率高的交易所做空
        for rate, exchange in funding_rates:
            if exchange == lowest_funding_exchange:
                if exchange == "backpack":
                    preferred_sides["bp_side"] = "BUY"
                elif exchange == "hyperliquid":
                    preferred_sides["hl_side"] = "BUY"
                elif exchange == "coinex":
                    preferred_sides["cx_side"] = "BUY"
            elif exchange == highest_funding_exchange:
                if exchange == "backpack":
                    preferred_sides["bp_side"] = "SELL"
                elif exchange == "hyperliquid":
                    preferred_sides["hl_side"] = "SELL"
                elif exchange == "coinex":
                    preferred_sides["cx_side"] = "SELL"
            else:
                # 中间的交易所不参与交易
                if exchange == "backpack":
                    preferred_sides["bp_side"] = "NONE"
                elif exchange == "hyperliquid":
                    preferred_sides["hl_side"] = "NONE"
                elif exchange == "coinex":
                    preferred_sides["cx_side"] = "NONE"
        
        # 检查是否至少有一对交易所方向相反（一个做多一个做空）
        has_opposite_directions = False
        if ("bp_side" in preferred_sides and "hl_side" in preferred_sides and
            preferred_sides["bp_side"] != "NONE" and preferred_sides["hl_side"] != "NONE" and
            preferred_sides["bp_side"] != preferred_sides["hl_side"]):
            has_opposite_directions = True
        elif ("bp_side" in preferred_sides and "cx_side" in preferred_sides and
              preferred_sides["bp_side"] != "NONE" and preferred_sides["cx_side"] != "NONE" and
              preferred_sides["bp_side"] != preferred_sides["cx_side"]):
            has_opposite_directions = True
        elif ("hl_side" in preferred_sides and "cx_side" in preferred_sides and
              preferred_sides["hl_side"] != "NONE" and preferred_sides["cx_side"] != "NONE" and
              preferred_sides["hl_side"] != preferred_sides["cx_side"]):
            has_opposite_directions = True
        
        # 记录方向一致性检查结果
        self.logger.debug(
            f"{symbol} - 三交易所方向一致性检查: "
            f"BP资金费率={bp_funding:.6f}, HL资金费率={adjusted_hl_funding:.6f}, CX资金费率={cx_funding:.6f}, "
            f"建议方向: {preferred_sides}, 方向一致性: {has_opposite_directions}"
        )
        
        return has_opposite_directions, preferred_sides

    def check_direction_consistency(
            self,
            symbol,
            bp_price,
            hl_price,
            bp_funding,
            hl_funding):
        """
        检查基于价差和资金费率的开仓方向是否一致

        Args:
            symbol: 基础币种，如 "BTC"
            bp_price: Backpack价格
            hl_price: Hyperliquid价格
            bp_funding: Backpack资金费率
            hl_funding: Hyperliquid资金费率

        Returns:
            tuple: (is_consistent, funding_bp_side, funding_hl_side) 方向是否一致，以及基于资金费率的建议开仓方向
        """
        # 1. 计算基于价差的开仓方向
        price_diff = bp_price - hl_price
        if price_diff > 0:
            # BP价格高，价差套利应该BP做空，HL做多
            price_bp_side = "SELL"
            price_hl_side = "BUY"
        else:
            # HL价格高，价差套利应该BP做多，HL做空
            price_bp_side = "BUY"
            price_hl_side = "SELL"

        # 2. 计算基于资金费率的开仓方向
        if bp_funding < 0 and hl_funding < 0:
            # 两交易所资金费率都为负
            if abs(bp_funding) > abs(hl_funding):
                funding_bp_side = "BUY"    # BP绝对值大，做多
                funding_hl_side = "SELL"
            else:
                funding_bp_side = "SELL"
                funding_hl_side = "BUY"
        elif bp_funding > 0 and hl_funding > 0:
            # 两交易所资金费率都为正
            if bp_funding > hl_funding:
                funding_bp_side = "SELL"   # BP值大，做空
                funding_hl_side = "BUY"
            else:
                funding_bp_side = "BUY"
                funding_hl_side = "SELL"
        else:
            # 资金费率一正一负的情况
            # 经典资金费率套利：资金费率为正的交易所做空(支付资金费)，为负的交易所做多(收取资金费)
            funding_bp_side = "SELL" if bp_funding > 0 else "BUY"
            funding_hl_side = "SELL" if hl_funding > 0 else "BUY"

        # 3. 比较两种策略的开仓方向是否一致
        is_consistent = (
            price_bp_side == funding_bp_side and price_hl_side == funding_hl_side)

        self.logger.info(
            f"{symbol} - 方向一致性检查：价差套利方向(BP={price_bp_side}, HL={price_hl_side})，"
            f"资金费率套利方向(BP={funding_bp_side}, HL={funding_hl_side})，"
            f"{'一致' if is_consistent else '不一致'}")

        return is_consistent, funding_bp_side, funding_hl_side

    def _check_close_conditions_without_execution(
        self,
        symbol: str,
        bp_position: dict,
        hl_position: dict,
        bp_price: float,
        hl_price: float,
        bp_funding: float,
        adjusted_hl_funding: float,
        price_diff_percent: float,
        funding_diff: float,
        funding_diff_sign: int,
        cx_position: dict = None,
        cx_price: float = None,
        cx_funding: float = None,
        is_coinex: bool = False
    ):
        """
        检查是否满足平仓条件，但不执行平仓
        
        Args:
            symbol: 基础币种，如 "BTC"
            bp_position: Backpack持仓信息
            hl_position: Hyperliquid持仓信息
            bp_price: Backpack价格
            hl_price: Hyperliquid价格
            bp_funding: Backpack资金费率
            adjusted_hl_funding: 调整后的Hyperliquid资金费率
            price_diff_percent: 价格差异（百分比）
            funding_diff: 资金费率差异
            funding_diff_sign: 资金费率差异符号
            cx_position: CoinEx持仓信息（可选）
            cx_price: CoinEx价格（可选）
            cx_funding: CoinEx资金费率（可选）
            is_coinex: 是否涉及CoinEx交易所（可选）

        Returns:
            tuple: (should_close, reason, position)
        """
        # 获取平仓条件配置
        close_conditions = self.config.get(
            "strategy", {}).get(
            "close_conditions", {})
        condition_type = close_conditions.get("condition_type", "any")
        
        # 检查持仓时间是否足够长
        min_position_time = close_conditions.get("min_position_time", 600)  # 默认最小持仓10分钟
        current_time = time.time()
        open_time = self.position_open_times.get(symbol, 0)
        
        if open_time > 0:  # 如果有记录开仓时间
            position_duration = current_time - open_time
            if position_duration < min_position_time:
                self.logger.debug(f"{symbol} - 持仓时间过短({position_duration:.0f}秒<{min_position_time}秒)，暂不平仓")
                return False, "持仓时间过短", None
        
        # 资金费率差异符号变化条件已弃用，新增方向反转检查
        # 计算当前市场状况下的建议开仓方向
        is_consistent = False
        bp_current_side = None
        hl_current_side = None
        cx_current_side = None
        
        if cx_price and cx_funding:
            # 如果有CoinEx数据，使用三个交易所的方向一致性检查
            is_consistent, preferred_sides = self.check_direction_consistency_multi(
                symbol, bp_price, hl_price, cx_price, bp_funding, adjusted_hl_funding, cx_funding)
            
            if preferred_sides:
                bp_current_side = preferred_sides.get("bp_side")
                hl_current_side = preferred_sides.get("hl_side")
                cx_current_side = preferred_sides.get("cx_side")
        else:
            # 如果没有CoinEx数据，使用原有的两个交易所方向一致性检查
            is_consistent, bp_current_side, hl_current_side = self.check_direction_consistency(
                symbol, bp_price, hl_price, bp_funding, adjusted_hl_funding)
        
        # 从持仓获取实际开仓方向
        bp_entry_side = bp_position.get("side", "UNKNOWN") if bp_position else "UNKNOWN"
        hl_entry_side = hl_position.get("side", "UNKNOWN") if hl_position else "UNKNOWN"
        cx_entry_side = cx_position.get("side", "UNKNOWN") if cx_position else "UNKNOWN"
        
        # 判断方向是否反转（现在建议的方向与开仓方向相反）
        bp_direction_reversed = False
        if bp_entry_side != "UNKNOWN" and bp_current_side:
            bp_direction_reversed = (bp_entry_side == "BUY" and bp_current_side == "SELL") or \
                                   (bp_entry_side == "SELL" and bp_current_side == "BUY")
        
        hl_direction_reversed = False
        if hl_entry_side != "UNKNOWN" and hl_current_side:
            hl_direction_reversed = (hl_entry_side == "BUY" and hl_current_side == "SELL") or \
                                   (hl_entry_side == "SELL" and hl_current_side == "BUY")
        
        # 添加CoinEx方向反转检查
        cx_direction_reversed = False
        if cx_entry_side != "UNKNOWN" and cx_current_side:
            cx_direction_reversed = (cx_entry_side == "BUY" and cx_current_side == "SELL") or \
                                   (cx_entry_side == "SELL" and cx_current_side == "BUY")
        
        # 方向反转条件：根据持有的交易所组合判断
        direction_reversed = False
        
        # 如果同时持有三个交易所的仓位
        if bp_entry_side != "UNKNOWN" and hl_entry_side != "UNKNOWN" and cx_entry_side != "UNKNOWN":
            direction_reversed = bp_direction_reversed and hl_direction_reversed and cx_direction_reversed and is_consistent
        # 如果只持有Backpack和Hyperliquid的仓位
        elif bp_entry_side != "UNKNOWN" and hl_entry_side != "UNKNOWN":
            direction_reversed = bp_direction_reversed and hl_direction_reversed and is_consistent
        # 如果持有Backpack和CoinEx的仓位
        elif bp_entry_side != "UNKNOWN" and cx_entry_side != "UNKNOWN":
            direction_reversed = bp_direction_reversed and cx_direction_reversed and is_consistent
        # 如果持有Hyperliquid和CoinEx的仓位
        elif hl_entry_side != "UNKNOWN" and cx_entry_side != "UNKNOWN":
            direction_reversed = hl_direction_reversed and cx_direction_reversed and is_consistent
        
        self.logger.info(
            f"{symbol} - 方向反转检查: 持仓方向(BP={bp_entry_side}, HL={hl_entry_side}, CX={cx_entry_side})，"
            f"当前建议方向(BP={bp_current_side}, HL={hl_current_side}, CX={cx_current_side})，"
            f"方向反转={direction_reversed}，方向一致={is_consistent}"
        )
        
        # 如果方向未反转，说明还未到平仓时机
        if not direction_reversed:
            return False, "方向未反转，不满足平仓条件", None
        
        # 保留原有价格差异检查
        price_diff_sign = 1 if price_diff_percent > 0 else -1
        
        # 资金费率差异最小值条件（当差异过小，无套利空间时平仓）
        min_funding_diff = close_conditions.get("min_funding_diff", 0.000005)
        
        # 价格差异条件（获利/止损）
        min_profit_percent = close_conditions.get("min_profit_percent", 0.1)
        max_loss_percent = close_conditions.get("max_loss_percent", 0.3)
        
        # 检查滑点条件
        max_close_slippage = close_conditions.get(
            "max_close_slippage_percent", 0.25)
        ignore_close_slippage = close_conditions.get(
            "ignore_close_slippage", False)

        # 获取市场数据中的滑点信息
        market_data = self.data_manager.get_all_data()
        total_slippage = market_data.get(
            symbol, {}).get(
            "total_slippage", 0.5)  # 默认为0.5%，较高值

        # 检查平仓滑点是否超过限制
        slippage_condition_met = ignore_close_slippage or total_slippage <= max_close_slippage

        if not slippage_condition_met:
            self.logger.debug(
                f"{symbol} - 预估平仓滑点({total_slippage:.4f}%)超过最大允许值({max_close_slippage:.4f}%)，暂不纳入平仓候选")
            return False, f"滑点过高({total_slippage:.4f}%)", None
        
        # 检查资金费率和价格差异条件
        funding_condition_met = abs(funding_diff) >= min_funding_diff
        price_condition_met = abs(price_diff_percent) >= min_profit_percent
                
        # 根据条件类型决定是否平仓
        should_close = False
        reason = ""
        
        if condition_type == "any":
            # 满足任一条件即可平仓（但必须满足方向反转）
            should_close = (funding_condition_met or price_condition_met)
            if funding_condition_met:
                reason = f"资金费率差异({abs(funding_diff):.6f})满足条件且方向已反转，确认盈利"
            elif price_condition_met:
                reason = f"价格差异({abs(price_diff_percent):.4f}%)满足条件且方向已反转，确认盈利"
        elif condition_type == "all":
            # 必须同时满足所有条件才能平仓
            should_close = funding_condition_met and price_condition_met
            reason = "同时满足资金费率差异和价格差异条件且方向已反转，确认盈利"
        elif condition_type == "funding_only":
            # 仅考虑资金费率条件
            should_close = funding_condition_met
            reason = f"资金费率差异({abs(funding_diff):.6f})满足条件且方向已反转，确认盈利"
        elif condition_type == "price_only":
            # 仅考虑价格差异条件
            should_close = price_condition_met
            reason = f"价格差异({abs(price_diff_percent):.4f}%)满足条件且方向已反转，确认盈利"
        
        # 记录条件判断结果
        self.logger.info(
            f"{symbol} - 平仓条件检查: 资金费率条件{'' if funding_condition_met else '未'}满足 "
            f"(差异: {abs(funding_diff):.6f}, 阈值: {min_funding_diff}), "
            f"价格条件{'' if price_condition_met else '未'}满足 "
            f"(差异: {abs(price_diff_percent):.4f}%, 阈值: {min_profit_percent}%), "
            f"方向反转条件{'' if direction_reversed else '未'}满足"
        )
            
        # 创建持仓对象
        position = {
            "bp_symbol": get_backpack_symbol(symbol),
            "hl_symbol": symbol,
            "cx_symbol": symbol,  # CoinEx使用基础币种作为符号
            "bp_side": bp_position["side"] if bp_position else None,
            "hl_side": hl_position["side"] if hl_position else None,
            "bp_size": bp_position["size"] if bp_position else 0,
            "hl_size": hl_position["size"] if hl_position else 0
        }
        
        # 添加CoinEx持仓信息（如果有）
        if cx_position:
            position["cx_side"] = cx_position["side"]
            position["cx_size"] = cx_position["size"]

        return should_close, reason, position

    async def _open_position(
            self,
            symbol: str,
            funding_diff: float,
            bp_funding: float,
            hl_funding: float,
            available_size: float = None,
            cx_funding: float = None):
        """
        开仓

        Args:
            symbol: 基础币种，如 "BTC"
            funding_diff: 资金费率差
            bp_funding: Backpack资金费率
            hl_funding: Hyperliquid资金费率
            available_size: 可用的剩余开仓量，如果为None则使用配置中的开仓数量
            cx_funding: CoinEx资金费率（可选）
        """
        try:
            # 获取最新数据
            data = await self.data_manager.get_data(symbol)

            # 获取价格
            bp_price = data["backpack"]["price"]
            hl_price = data["hyperliquid"]["price"]
            cx_price = None
            if "coinex" in data:
                cx_price = data["coinex"]["price"]

            if bp_price is None or hl_price is None:
                self.logger.error(f"{symbol}价格数据无效，无法开仓")
                return

            # 获取交易对配置
            trading_pair_config = None
            for pair in self.config.get("trading_pairs", []):
                if pair["symbol"] == symbol:
                    trading_pair_config = pair
                    break

            if not trading_pair_config:
                self.logger.error(f"未找到{symbol}的交易对配置")
                return

            # 获取最大持仓数量和最小交易量
            max_position_size = trading_pair_config.get("max_position_size")
            min_volume = trading_pair_config.get("min_volume")

            # 计算开仓数量
            bp_size = available_size if available_size is not None else self.position_sizes[
                symbol]
            hl_size = bp_size  # 两个交易所使用相同的开仓数量
            cx_size = bp_size  # CoinEx也使用相同的开仓数量

            # 检查是否小于最小交易量
            if bp_size < min_volume:
                self.logger.warning(
                    f"{symbol}开仓数量({bp_size})小于最小交易量({min_volume})，已调整为最小交易量")
                bp_size = min_volume
                hl_size = min_volume
                cx_size = min_volume

            # 检查是否超过最大持仓数量
            if max_position_size is not None and (bp_size > max_position_size):
                self.logger.warning(
                    f"{symbol}开仓数量({bp_size})超过最大持仓数量({max_position_size})，已调整为最大持仓数量")
                bp_size = max_position_size
                hl_size = max_position_size
                cx_size = max_position_size

            # 计算资金费率差
            funding_diff, funding_diff_sign = calculate_funding_diff(
                bp_funding, hl_funding)

            # 记录当前的资金费率符号用于后续平仓判断
            self.funding_diff_signs[symbol] = funding_diff_sign
            self.logger.debug(f"记录{symbol}开仓时的资金费率符号: {funding_diff_sign}")
            # 保存资金费率符号记录到文件
            self._save_funding_diff_signs()

            # 计算价格差异符号并记录
            price_diff_percent = (bp_price - hl_price) / hl_price * 100
            price_diff_sign = 1 if price_diff_percent > 0 else -1

            # 确保price_diff_signs字典存在
            if not hasattr(self, 'price_diff_signs'):
                self.price_diff_signs = {}

            # 记录当前的价格差符号用于后续平仓判断
            self.price_diff_signs[symbol] = price_diff_sign
            self.logger.debug(f"记录{symbol}开仓时的价格差符号: {price_diff_sign}")

            # 准备仓位数据
            bp_symbol = get_backpack_symbol(
                symbol)  # 使用正确的交易对格式，如 BTC_USDC_PERP
            hl_symbol = get_hyperliquid_symbol(symbol)
            cx_symbol = get_coinex_symbol(symbol)

            # 检查是否存在已经计算好的开仓方向
            if hasattr(
                    self,
                    'preferred_sides') and symbol in self.preferred_sides:
                # 使用方向一致性检查时预先计算的方向
                bp_side = self.preferred_sides[symbol]["bp_side"]
                hl_side = self.preferred_sides[symbol]["hl_side"]
                cx_side = self.preferred_sides[symbol].get("cx_side")
                self.logger.info(
                    f"{symbol} - 使用方向一致性检查确定的交易方向: BP={bp_side}, HL={hl_side}, CX={cx_side}")

                # 使用完后清除，避免影响下次开仓
                del self.preferred_sides[symbol]
            else:
                # 确定交易方向
                # 首先确定哪些交易所有可用的资金费率数据
                exchanges = []
                funding_rates = []
                
                if bp_funding is not None:
                    exchanges.append("backpack")
                    funding_rates.append(bp_funding)
                    
                if hl_funding is not None:
                    # 调整Hyperliquid资金费率以匹配Backpack的8小时周期
                    adjusted_hl_funding = hl_funding * 8
                    exchanges.append("hyperliquid")
                    funding_rates.append(adjusted_hl_funding)
                    
                if cx_funding is not None:
                    # 根据CoinEx的结算周期调整资金费率（如果需要）
                    exchanges.append("coinex")
                    funding_rates.append(cx_funding)
                
                # 初始化交易方向
                bp_side = None
                hl_side = None
                cx_side = None
                
                # 如果至少有两个交易所有数据，确定做多和做空的交易所
                if len(exchanges) >= 2:
                    # 找出资金费率最低和最高的交易所
                    min_idx = funding_rates.index(min(funding_rates))
                    max_idx = funding_rates.index(max(funding_rates))
                    
                    # 资金费率低的做多，高的做空
                    for i, exchange in enumerate(exchanges):
                        if i == min_idx:  # 资金费率最低的做多
                            if exchange == "backpack":
                                bp_side = "BUY"
                            elif exchange == "hyperliquid":
                                hl_side = "BUY"
                            elif exchange == "coinex":
                                cx_side = "BUY"
                        elif i == max_idx:  # 资金费率最高的做空
                            if exchange == "backpack":
                                bp_side = "SELL"
                            elif exchange == "hyperliquid":
                                hl_side = "SELL"
                            elif exchange == "coinex":
                                cx_side = "SELL"
                
                # 如果只有两个交易所有数据，为第三个交易所设置默认方向
                if "backpack" in exchanges and "hyperliquid" in exchanges and cx_funding is None:
                    # 不使用CoinEx
                    cx_side = None
                elif "backpack" in exchanges and "coinex" in exchanges and hl_funding is None:
                    # 不使用Hyperliquid
                    hl_side = None
                elif "hyperliquid" in exchanges and "coinex" in exchanges and bp_funding is None:
                    # 不使用Backpack
                    bp_side = None

                # 如果没有足够的交易所数据，使用原始逻辑
                # 修正资金费率套利逻辑
                # 1. 当两个交易所资金费率正负相反时：负的做多，正的做空
                # 2. 当两个交易所资金费率都为负时：绝对值大的做多，绝对值小的做空
                # 3. 当两个交易所资金费率都为正时：值大的做空，值小的做多
                if (bp_funding < 0 and hl_funding < 0):
                    # 两个交易所资金费率都为负
                    if abs(bp_funding) > abs(hl_funding):
                        # BP资金费率绝对值更大，BP做多，HL做空
                        bp_side = "BUY"
                        hl_side = "SELL"
                    else:
                        # HL资金费率绝对值更大，HL做多，BP做空
                        bp_side = "SELL"
                        hl_side = "BUY"
                elif (bp_funding > 0 and hl_funding > 0):
                    # 两个交易所资金费率都为正
                    if bp_funding > hl_funding:
                        # BP资金费率更大，BP做空，HL做多
                        bp_side = "SELL"
                        hl_side = "BUY"
                    else:
                        # HL资金费率更大，HL做空，BP做多
                        bp_side = "BUY"
                        hl_side = "SELL"
                else:
                    # 两个交易所资金费率正负相反，保持原有逻辑
                    bp_side = "SELL" if bp_funding > 0 else "BUY"
                    hl_side = "SELL" if hl_funding > 0 else "BUY"

            # 记录资金费率和交易方向
            self.logger.info(
                f"{symbol} - BP资金费率: {bp_funding:.6f}，方向: {bp_side}；HL资金费率: {hl_funding:.6f}，方向: {hl_side}")

            if cx_funding is not None:
                self.logger.info(f"{symbol} - CX资金费率: {cx_funding:.6f}，方向: {cx_side}")

            # 获取交易前的持仓状态
            self.logger.info(f"获取{symbol}开仓前的持仓状态")
            pre_bp_positions = await self.backpack_api.get_positions()
            pre_hl_positions = await self.hyperliquid_api.get_positions()
            pre_cx_positions = {}
            if self.coinex_api and cx_side is not None:
                try:
                    pre_cx_positions = await self.coinex_api.get_positions()
                except Exception as e:
                    self.logger.error(f"获取CoinEx持仓信息失败: {e}")

            # 记录开仓前的持仓状态
            pre_bp_position = None
            for pos in pre_bp_positions.values():
                if pos.get("symbol") == bp_symbol:
                    pre_bp_position = pos
                    break

            pre_hl_position = None
            for pos in pre_hl_positions.values():
                if pos.get("symbol") == hl_symbol:
                    pre_hl_position = pos
                    break

            pre_cx_position = None
            for pos in pre_cx_positions.values():
                if pos.get("symbol") == cx_symbol:
                    pre_cx_position = pos
                    break

            self.logger.info(
                f"开仓前持仓: BP {bp_symbol}={pre_bp_position}, HL {hl_symbol}={pre_hl_position}, CX {cx_symbol}={pre_cx_position}")

            # ===== 同时下单 =====
            self.logger.info(
                f"同时在交易所为{symbol}下单: BP {bp_side} {bp_size}, HL {hl_side} {hl_size}")
            if cx_side is not None:
                self.logger.info(f"CoinEx {cx_side} {cx_size}")

            # 获取价格精度和tick_size
            price_precision = trading_pair_config.get("price_precision", 3)
            tick_size = trading_pair_config.get("tick_size", 0.001)

            # 在Hyperliquid下限价单
            hl_price_adjuster = 1.005 if hl_side == "BUY" else 0.995
            hl_limit_price = hl_price * hl_price_adjuster

            # 使用正确的tick_size对限价单价格进行调整
            hl_limit_price = round(hl_limit_price / tick_size) * tick_size
            hl_limit_price = round(hl_limit_price, price_precision)

            # 在CoinEx下限价单（如果需要）
            cx_limit_price = None
            if cx_side is not None and cx_price is not None:
                cx_price_adjuster = 1.005 if cx_side == "BUY" else 0.995
                cx_limit_price = cx_price * cx_price_adjuster
                cx_limit_price = round(cx_limit_price / tick_size) * tick_size
                cx_limit_price = round(cx_limit_price, price_precision)
                self.logger.info(
                    f"使用限价单开仓CoinEx: 价格={cx_limit_price}, 精度={price_precision}, tick_size={tick_size}")

            self.logger.info(
                f"使用限价单开仓Hyperliquid: 价格={hl_limit_price}, 精度={price_precision}, tick_size={tick_size}")

            # 创建订单任务列表
            order_tasks = []

            # 同时发送两个交易所的订单
            # 添加Backpack订单任务（如果需要）
            if bp_side is not None:
                bp_order_task = asyncio.create_task(
                    self.backpack_api.place_order(
                        symbol=bp_symbol,  # 使用正确的交易对格式
                        side=bp_side,
                        size=bp_size,
                        price=None,  # 市价单不需要价格
                        order_type="MARKET"  # Backpack使用市价单
                    )
                )
                order_tasks.append(bp_order_task)
            
            # 添加Hyperliquid订单任务（如果需要）
            if hl_side is not None:
                hl_order_task = asyncio.create_task(
                    self.hyperliquid_api.place_order(
                        symbol=hl_symbol,
                        side=hl_side,
                        size=hl_size,
                        price=hl_limit_price,
                        order_type="LIMIT"
                    )
                )
                order_tasks.append(hl_order_task)
            
            # 添加CoinEx订单任务（如果需要）
            cx_order_task = None
            if self.coinex_api and cx_side is not None and cx_limit_price is not None:
                try:
                    cx_order_task = asyncio.create_task(
                        self.coinex_api.place_order(
                            symbol=cx_symbol,
                            side=cx_side,
                            size=cx_size,
                            price=cx_limit_price,
                            order_type="LIMIT"
                        )
                    )
                    order_tasks.append(cx_order_task)
                except Exception as e:
                    self.logger.error(f"创建CoinEx订单任务失败: {e}")

            # 等待订单结果
            # bp_result, hl_result = await asyncio.gather(
            #     bp_order_task,
            #     hl_order_task,
            #     return_exceptions=True
            # )
            order_results = await asyncio.gather(*order_tasks, return_exceptions=True)

            # 解析订单结果
            bp_result = None
            hl_result = None
            cx_result = None

            result_index = 0
            if bp_side is not None:
                bp_result = order_results[result_index]
                result_index += 1
            
            if hl_side is not None:
                hl_result = order_results[result_index]
                result_index += 1
                
            if cx_order_task is not None:
                cx_result = order_results[result_index]

            # 检查订单结果
            bp_success = bp_side is not None and not isinstance(
                bp_result, Exception) and bp_result is not None

            # 增强的Hyperliquid订单成功检查逻辑
            hl_success = False
            hl_order_id = None
            if hl_side is not None and not isinstance(hl_result, Exception):
                if isinstance(hl_result, dict):
                    # 检查直接的success标志
                    if hl_result.get("success", False):
                        hl_success = True
                        hl_order_id = hl_result.get("order_id", "未知")
                        self.logger.info(
                            f"Hyperliquid订单成功，订单ID: {hl_order_id}")

                        # 检查订单是否已立即成交
                        if hl_result.get("status") == "filled":
                            self.logger.info(
                                f"Hyperliquid订单已立即成交，均价: {hl_result.get('price', '未知')}")

                    # 检查是否包含filled状态
                    elif "raw_response" in hl_result:
                        raw_response = hl_result["raw_response"]
                        raw_str = json.dumps(raw_response)

                        if "filled" in raw_str:
                            self.logger.info("检测到订单可能已成交，尝试提取订单信息")
                            hl_success = True

                            # 尝试提取订单ID
                            try:
                                if isinstance(
                                        raw_response, dict) and "response" in raw_response:
                                    response_data = raw_response["response"]
                                    if "data" in response_data and "statuses" in response_data["data"]:
                                        statuses = response_data["data"]["statuses"]
                                        if statuses and "filled" in statuses[0]:
                                            hl_order_id = statuses[0]["filled"].get(
                                                "oid", "未知")
                                            self.logger.info(
                                                f"成功提取订单ID: {hl_order_id}")
                            except Exception as extract_error:
                                self.logger.error(
                                    f"提取订单ID时出错: {extract_error}")
                                hl_order_id = "未能提取"

            # 检查CoinEx订单结果
            cx_success = False
            cx_order_id = None
            if cx_side is not None and cx_result is not None and not isinstance(cx_result, Exception):
                if isinstance(cx_result, dict):
                    # 检查订单是否成功
                    if cx_result.get("success", False) or cx_result.get("id"):
                        cx_success = True
                        cx_order_id = cx_result.get("id") or cx_result.get("order_id", "未知")
                        self.logger.info(f"CoinEx订单成功，订单ID: {cx_order_id}")
                        
                        # 检查订单是否已立即成交
                        if cx_result.get("status") == "filled" or cx_result.get("filled", 0) > 0:
                            self.logger.info(
                                f"CoinEx订单已立即成交，均价: {cx_result.get('price', '未知')}")

            # 日志记录订单结果
            if bp_side is not None:
                if bp_success:
                    self.logger.info(f"Backpack下单成功: {bp_result}")
                else:
                    self.logger.error(f"Backpack下单失败: {bp_result}")

            if hl_side is not None:
                if hl_success:
                    self.logger.info(f"Hyperliquid下单成功: {hl_order_id}")
                else:
                    self.logger.error(f"Hyperliquid下单失败: {hl_result}")
                    
            if cx_side is not None:
                if cx_success:
                    self.logger.info(f"CoinEx下单成功: {cx_order_id}")
                else:
                    self.logger.error(f"CoinEx下单失败: {cx_result}")

            # ===== 验证持仓变化 =====
            # 等待3秒让交易所处理订单
            self.logger.info("等待3秒让交易所处理订单...")
            await asyncio.sleep(3)

            # 获取交易后的持仓状态
            self.logger.info(f"获取{symbol}开仓后的持仓状态")
            post_bp_positions = await self.backpack_api.get_positions()
            post_hl_positions = await self.hyperliquid_api.get_positions()
            post_cx_positions = {}
            if self.coinex_api and cx_side is not None:
                try:
                    post_cx_positions = await self.coinex_api.get_positions()
                except Exception as e:
                    self.logger.error(f"获取CoinEx持仓信息失败: {e}")

            # 记录开仓后的持仓状态
            post_bp_position = None
            for pos in post_bp_positions.values():
                if pos.get("symbol") == bp_symbol:
                    post_bp_position = pos
                    break

            post_hl_position = None
            for pos in post_hl_positions.values():
                if pos.get("symbol") == hl_symbol:
                    post_hl_position = pos
                    break
            
            post_cx_position = None
            for pos in post_cx_positions.values():
                if pos.get("symbol") == cx_symbol:
                    post_cx_position = pos
                    break

            self.logger.info(
                f"开仓后持仓: BP {bp_symbol}={post_bp_position}, HL {hl_symbol}={post_hl_position}, CX {cx_symbol}={post_cx_position}")

            # 验证持仓变化
            bp_position_changed = False
            hl_position_changed = False
            cx_position_changed = False

            # 检查Backpack持仓变化
            if bp_side is not None:
                if pre_bp_position is None and post_bp_position is not None:
                    # 新建立了持仓
                    bp_position_changed = True
                    self.logger.info(
                        f"Backpack成功建立{bp_symbol}新持仓: {post_bp_position}")
                elif pre_bp_position is not None and post_bp_position is not None:
                    # 检查持仓大小是否变化
                    pre_size = float(pre_bp_position.get("quantity", 0))
                    post_size = float(post_bp_position.get("quantity", 0))
                    if abs(post_size - pre_size) >= 0.8 * bp_size:  # 允许80%的差异容忍度
                        bp_position_changed = True
                        self.logger.info(
                            f"Backpack {bp_symbol}持仓量变化: {pre_size} -> {post_size}")

            # 检查Hyperliquid持仓变化
            if hl_side is not None:
                if pre_hl_position is None and post_hl_position is not None:
                    # 新建立了持仓
                    hl_position_changed = True
                    self.logger.info(
                        f"Hyperliquid成功建立{hl_symbol}新持仓: {post_hl_position}")
                elif pre_hl_position is not None and post_hl_position is not None:
                    # 检查持仓大小是否变化
                    pre_size = float(pre_hl_position.get("size", 0))
                    post_size = float(post_hl_position.get("size", 0))
                    if abs(post_size - pre_size) >= 0.8 * hl_size:  # 允许80%的差异容忍度
                        hl_position_changed = True
                        self.logger.info(
                            f"Hyperliquid {hl_symbol}持仓量变化: {pre_size} -> {post_size}")

            # 检查CoinEx持仓变化
            if cx_side is not None:
                if pre_cx_position is None and post_cx_position is not None:
                    # 新建立了持仓
                    cx_position_changed = True
                    self.logger.info(
                        f"CoinEx成功建立{cx_symbol}新持仓: {post_cx_position}")
                elif pre_cx_position is not None and post_cx_position is not None:
                    # 检查持仓大小是否变化
                    pre_size = float(pre_cx_position.get("size", 0))
                    post_size = float(post_cx_position.get("size", 0))
                    if abs(post_size - pre_size) >= 0.8 * cx_size:  # 允许80%的差异容忍度
                        cx_position_changed = True
                        self.logger.info(
                            f"CoinEx {cx_symbol}持仓量变化: {pre_size} -> {post_size}")

            # 根据持仓变化情况判断开仓成功与否
            # 计算需要成功的交易所数量
            required_exchanges = 0
            if bp_side is not None:
                required_exchanges += 1
            if hl_side is not None:
                required_exchanges += 1
            if cx_side is not None:
                required_exchanges += 1
                
            # 计算实际成功的交易所数量
            successful_exchanges = 0
            if bp_position_changed:
                successful_exchanges += 1
            if hl_position_changed:
                successful_exchanges += 1
            if cx_position_changed:
                successful_exchanges += 1

            # 判断是否所有需要的交易所都成功开仓
            if successful_exchanges == required_exchanges and required_exchanges >= 2:
                # 所有交易所都成功开仓
                message = f"开仓成功: \n"
                if bp_side is not None:
                    message += f"Backpack: {bp_side} {bp_size} {bp_symbol}\n"
                if hl_side is not None:
                    message += f"Hyperliquid: {hl_side} {hl_size} {hl_symbol}\n"
                if cx_side is not None:
                    message += f"CoinEx: {cx_side} {cx_size} {cx_symbol}\n"

                self.logger.info(message)
                self.display_manager.add_order_message(message)
                # 更新订单统计
                self.display_manager.update_order_stats("open", True)
                
                # 记录开仓方向和时间
                self.position_directions[symbol] = {
                    "bp_side": bp_side,
                    "hl_side": hl_side,
                    "cx_side": cx_side
                }
                self.position_open_times[symbol] = time.time()
                
                # 记录开仓价格和资金费率
                self.entry_prices[symbol] = {
                    "bp_price": bp_price,
                    "hl_price": hl_price,
                    "cx_price": cx_price
                }
                self.entry_funding_rates[symbol] = {
                    "bp_funding": bp_funding,
                    "hl_funding": hl_funding,
                    "cx_funding": cx_funding
                }
                
                # 保存开仓快照
                self._save_position_snapshot(
                    symbol=symbol,
                    action="open",
                    bp_position={"side": bp_side, "size": bp_size} if bp_side else None,
                    hl_position={"side": hl_side, "size": hl_size} if hl_side else None,
                    cx_position={"side": cx_side, "size": cx_size} if cx_side else None,
                    bp_price=bp_price,
                    hl_price=hl_price,
                    cx_price=cx_price,
                    bp_funding=bp_funding,
                    hl_funding=hl_funding,
                    cx_funding=cx_funding
                )
                    
                # 发送开仓通知
                if self.alerter:
                    exchanges = []
                    if bp_side:
                        exchanges.append(f"Backpack({bp_side})")
                    if hl_side:
                        exchanges.append(f"Hyperliquid({hl_side})")
                    if cx_side:
                        exchanges.append(f"CoinEx({cx_side})")
                    
                    message = (
                        f"🚀 开仓: {symbol}\n"
                        f"交易所: {', '.join(exchanges)}\n"
                        f"仓位: {position_size}\n"
                        f"资金费率差异: {funding_diff:.6f}\n"
                        f"BP价格: {bp_price}\n"
                        f"HL价格: {hl_price}\n"
                    )
                    
                    if cx_price:
                        message += f"CX价格: {cx_price}\n"
                        
                    message += (
                        f"BP-HL价差: {bp_hl_price_diff_percent:.2f}%\n"
                    )
                    
                    if cx_price:
                        message += (
                            f"BP-CX价差: {bp_cx_price_diff_percent:.2f}%\n"
                            f"HL-CX价差: {hl_cx_price_diff_percent:.2f}%\n"
                        )
                        
                    await self.alerter.send_alert(message)
                
                self.logger.info(
                    f"{symbol} - 开仓完成: BP={bp_side}@{bp_price}, HL={hl_side}@{hl_price}, "
                    f"CX={cx_side}@{cx_price if cx_price else 'N/A'}, "
                    f"资金费率差异={funding_diff:.6f}, 仓位={position_size}"
                )

        except Exception as e:
            self.logger.error(f"{symbol} - 开仓操作失败: {str(e)}")
            self.display_manager.add_order_message(f"{symbol}开仓过程发生异常: {e}")
            return False

    async def _close_position(self, symbol: str, position: Dict[str, Any]):
        """
        平仓
        
        Args:
            symbol: 基础币种，如 "BTC"
            position: 仓位数据字典
        """
        try:
            message = f"尝试为{symbol}平仓"
            self.logger.info(message)
            self.display_manager.add_order_message(message)
            
           # 获取仓位信息
            bp_symbol = position["bp_symbol"]
            hl_symbol = position["hl_symbol"]
            cx_symbol = position["cx_symbol"]
            
            bp_side = position["bp_side"]
            hl_side = position["hl_side"]
            cx_side = position["cx_side"]
            
            bp_size = float(position["bp_size"]) if position["bp_size"] is not None else 0  # 确保是浮点数
            hl_size = float(position["hl_size"]) if position["hl_size"] is not None else 0  # 确保是浮点数
            cx_size = float(position["cx_symbol"]) if position["cx_side"] is not None else 0  # 获取CoinEx仓位大小
            
            # 平仓方向与开仓方向相反
            bp_close_side = "SELL" if bp_side == "BUY" else "BUY" if bp_side else None
            hl_close_side = "SELL" if hl_side == "BUY" else "BUY" if hl_side else None
            cx_close_side = "SELL" if cx_side == "BUY" else "BUY" if cx_side else None  # CoinEx平仓方向
            
            message = (
                f"平仓方向: "
            )
            if bp_side:
                message += f"BP {bp_close_side} {bp_size} {bp_symbol}, "
            if hl_side:
                message += f"HL {hl_close_side} {hl_size} {hl_symbol}, "
            if cx_side:
                message += f"CX {cx_close_side} {cx_size} {cx_symbol}"

            self.logger.info(message)
            self.display_manager.add_order_message(message)
            
            # 获取当前价格和资金费率
            data = await self.data_manager.get_data(symbol)
            if not data:
                message = f"无法获取{symbol}的市场数据，平仓失败"
                self.logger.error(message)
                self.display_manager.add_order_message(message)
                return False
                
            bp_price = data["backpack"]["price"]
            hl_price = data["hyperliquid"]["price"]
            bp_funding = data["backpack"]["funding_rate"]
            hl_funding = data["hyperliquid"]["funding_rate"] * 8  # 调整为8小时周期

            # 获取CoinEx价格和资金费率
            cx_price = None
            cx_funding = None
            if "coinex" in data:
                cx_price = data["coinex"]["price"]
                cx_funding = data["coinex"]["funding_rate"]
            
            if (bp_side and not bp_price) or (hl_side and not hl_price) or (cx_side and not cx_price):
                missing_exchange = []
                if bp_side and not bp_price:
                    missing_exchange.append(f"Backpack({bp_symbol})")
                if hl_side and not hl_price:
                    missing_exchange.append(f"Hyperliquid({hl_symbol})")
                if cx_side and not cx_price:
                    missing_exchange.append(f"CoinEx({cx_symbol})")
                    
                message = f"无法获取{', '.join(missing_exchange)}的当前价格，平仓失败"
                self.logger.error(message)
                self.display_manager.add_order_message(message)
                return False

            # 获取交易前的持仓状态
            self.logger.info(f"获取{symbol}平仓前的持仓状态")
            pre_bp_positions = await self.backpack_api.get_positions() if bp_side else {}
            pre_hl_positions = await self.hyperliquid_api.get_positions() if hl_side else {}
            pre_cx_positions = await self.coinex_api.get_positions() if cx_side else {}

            # 记录平仓前的持仓状态
            pre_bp_position = None
            for pos in pre_bp_positions.values():
                if pos.get("symbol") == bp_symbol:
                    pre_bp_position = pos
                    break

            pre_hl_position = None
            for pos in pre_hl_positions.values():
                if pos.get("symbol") == hl_symbol:
                    pre_hl_position = pos
                    break

            pre_cx_position = None
            if cx_side:
                for pos in pre_cx_positions.values():
                    if pos.get("base_symbol") == symbol:
                        pre_cx_position = pos
                        break

            self.logger.info(
                f"平仓前持仓: "
                f"BP {bp_symbol}={pre_bp_position if bp_side else 'N/A'}, "
                f"HL {hl_symbol}={pre_hl_position if hl_side else 'N/A'}, "
                f"CX {cx_symbol}={pre_cx_position if cx_side else 'N/A'}"
            )

            # 检查是否有持仓需要平仓
            if bp_side and pre_bp_position is None:
                self.logger.warning(f"Backpack没有{bp_symbol}的持仓，无需平仓")
                bp_side = None
                bp_close_side = None

            if hl_side and pre_hl_position is None:
                self.logger.warning(f"Hyperliquid没有{hl_symbol}的持仓，无需平仓")
                hl_side = None
                hl_close_side = None
                
            if cx_side and pre_cx_position is None:
                self.logger.warning(f"CoinEx没有{cx_symbol}的持仓，无需平仓")
                cx_side = None
                cx_close_side = None

            # 如果所有交易所都没有持仓，则无需平仓
            if not bp_side and not hl_side and not cx_side:
                self.logger.warning(f"所有交易所都没有{symbol}的持仓，无需平仓")
                return False

            # 根据买卖方向调整价格确保快速成交
            bp_price_adjuster = 1.005 if bp_close_side == "BUY" else 0.995
            bp_limit_price = bp_price * bp_price_adjuster

            # 根据tick_size调整价格
            if bp_limit_price:
                bp_limit_price = round(bp_limit_price / tick_size) * tick_size
                # 控制小数位数，确保不超过配置的精度
                bp_limit_price = round(bp_limit_price, price_precision)
                self.logger.info(
                    f"平仓价格计算: 原始价格={bp_price}, 调整系数={bp_price_adjuster}, "
                    f"调整后价格={bp_limit_price}, 精度={price_precision}, tick_size={tick_size}"
                )

            # 创建平仓任务列表
            close_tasks = []

            # Backpack平仓任务
            if bp_side:
                bp_order_task = asyncio.create_task(
                    self.backpack_api.place_order(
                        symbol=bp_symbol,
                        side=bp_close_side,
                        size=float(bp_size),  # 确保size是浮点数
                        price=None,  # 使用市价单简化操作
                        order_type="MARKET"
                    )
                )
                close_tasks.append(("backpack", bp_order_task))

            # Hyperliquid平仓任务
            if hl_side:
                hl_order_task = asyncio.create_task(
                    self.hyperliquid_api.place_order(
                        symbol=hl_symbol,
                        side=hl_close_side,
                        size=float(hl_size),  # 确保size是浮点数
                        price=None,  # 价格会在API内部计算
                        order_type="MARKET"  # 使用市价单简化操作
                    )
                )
                close_tasks.append(("hyperliquid", hl_order_task))
                
            # CoinEx平仓任务
            if cx_side:
                cx_order_task = asyncio.create_task(
                    self.coinex_api.place_order(
                        symbol=cx_symbol,
                        side=cx_close_side,
                        size=float(cx_size),  # 确保size是浮点数
                        price=None,  # 使用市价单
                        order_type="MARKET"
                    )
                )
                close_tasks.append(("coinex", cx_order_task))

            # 等待所有平仓任务完成
            results = {}
            for exchange, task in close_tasks:
                try:
                    result = await task
                    results[exchange] = result
                except Exception as e:
                    results[exchange] = e
                    self.logger.error(f"{exchange}平仓订单异常: {e}")

            # 检查平仓结果
            success_status = {}
            
            # 检查Backpack平仓结果
            if "backpack" in results:
                bp_result = results["backpack"]
                bp_success = not isinstance(bp_result, Exception) and not (
                    isinstance(bp_result, dict) and bp_result.get("error"))
                success_status["backpack"] = bp_success
                
                if bp_success:
                    self.logger.info(f"Backpack平仓订单成功: {bp_result}")
                else:
                    self.logger.error(f"Backpack平仓订单失败: {bp_result}")

            # 检查Hyperliquid平仓结果
            if "hyperliquid" in results:
                hl_result = results["hyperliquid"]
                hl_success = False
                if not isinstance(hl_result, Exception):
                    if isinstance(hl_result, dict):
                        # 检查直接的success标志
                        if hl_result.get("success", False):
                            hl_success = True
                            hl_order_id = hl_result.get("order_id", "未知")
                            self.logger.info(
                                f"Hyperliquid平仓订单成功，订单ID: {hl_order_id}")

                            # 检查订单是否已立即成交
                            if hl_result.get("status") == "filled":
                                self.logger.info(
                                    f"Hyperliquid平仓订单已立即成交，均价: {hl_result.get('price', '未知')}")

                        # 检查是否包含filled状态
                        elif "raw_response" in hl_result:
                            raw_response = hl_result["raw_response"]
                            raw_str = json.dumps(raw_response)

                            if "filled" in raw_str:
                                self.logger.info("检测到平仓订单可能已成交")
                                hl_success = True
                                
                success_status["hyperliquid"] = hl_success
                
                if hl_success:
                    self.logger.info(f"Hyperliquid平仓订单成功")
                else:
                    self.logger.error(f"Hyperliquid平仓订单失败: {hl_result}")
                    
            # 检查CoinEx平仓结果
            if "coinex" in results:
                cx_result = results["coinex"]
                cx_success = not isinstance(cx_result, Exception) and not (
                    isinstance(cx_result, dict) and cx_result.get("error"))
                success_status["coinex"] = cx_success
                
                if cx_success:
                    self.logger.info(f"CoinEx平仓订单成功: {cx_result}")
                else:
                    self.logger.error(f"CoinEx平仓订单失败: {cx_result}")

            # ===== 验证持仓变化 =====
            # 等待3秒让交易所处理订单
            self.logger.info("等待3秒让交易所处理订单...")
            await asyncio.sleep(3)

            # 获取交易后的持仓状态
            self.logger.info(f"获取{symbol}平仓后的持仓状态")
            post_bp_positions = await self.backpack_api.get_positions() if bp_side else {}
            post_hl_positions = await self.hyperliquid_api.get_positions() if hl_side else {}
            post_cx_positions = await self.coinex_api.get_positions() if cx_side else {}

            # 记录平仓后的持仓状态
            post_bp_position = None
            if bp_side:
                for pos in post_bp_positions.values():
                    if pos.get("symbol") == bp_symbol:
                        post_bp_position = pos
                        break

            post_hl_position = None
            if hl_side:
                for pos in post_hl_positions.values():
                    if pos.get("symbol") == hl_symbol:
                        post_hl_position = pos
                        break
                        
            post_cx_position = None
            if cx_side:
                for pos in post_cx_positions.values():
                    if pos.get("base_symbol") == symbol:
                        post_cx_position = pos
                        break

            self.logger.info(
                f"平仓后持仓: "
                f"BP {bp_symbol}={post_bp_position if bp_side else 'N/A'}, "
                f"HL {hl_symbol}={post_hl_position if hl_side else 'N/A'}, "
                f"CX {cx_symbol}={post_cx_position if cx_side else 'N/A'}"
            )

            # 验证持仓变化
            position_closed = {}

            # 检查Backpack持仓变化
            # 检查Backpack持仓变化
            if bp_side:
                if pre_bp_position is not None and post_bp_position is None:
                    # 持仓已完全平掉
                    position_closed["backpack"] = True
                    self.logger.info(f"Backpack成功平掉{bp_symbol}全部持仓")
                elif pre_bp_position is not None and post_bp_position is not None:
                    # 检查持仓大小是否变化
                    pre_size = float(pre_bp_position.get("quantity", 0))
                    post_size = float(post_bp_position.get("quantity", 0))
                    if pre_size > 0 and (
                            pre_size - post_size) / pre_size >= 0.9:  # 平掉了90%以上的持仓
                        position_closed["backpack"] = True
                        self.logger.info(
                            f"Backpack {bp_symbol}持仓量显著减少: {pre_size} -> {post_size}")
                    else:
                        position_closed["backpack"] = False
                else:
                    position_closed["backpack"] = False

            # 检查Hyperliquid持仓变化
            if hl_side:
                if pre_hl_position is not None and post_hl_position is None:
                    # 持仓已完全平掉
                    position_closed["hyperliquid"] = True
                    self.logger.info(f"Hyperliquid成功平掉{hl_symbol}全部持仓")
                elif pre_hl_position is not None and post_hl_position is not None:
                    # 检查持仓大小是否变化
                    pre_size = float(pre_hl_position.get("size", 0))
                    post_size = float(post_hl_position.get("size", 0))
                    if pre_size > 0 and (
                            pre_size - post_size) / pre_size >= 0.9:  # 平掉了90%以上的持仓
                        position_closed["hyperliquid"] = True
                        self.logger.info(
                            f"Hyperliquid {hl_symbol}持仓量显著减少: {pre_size} -> {post_size}")
                    else:
                        position_closed["hyperliquid"] = False
                else:
                    position_closed["hyperliquid"] = False
                    
            # 检查CoinEx持仓变化
            if cx_side:
                if pre_cx_position is not None and post_cx_position is None:
                    # 持仓已完全平掉
                    position_closed["coinex"] = True
                    self.logger.info(f"CoinEx成功平掉{cx_symbol}全部持仓")
                elif pre_cx_position is not None and post_cx_position is not None:
                    # 检查持仓大小是否变化
                    pre_size = float(pre_cx_position.get("size", 0))
                    post_size = float(post_cx_position.get("size", 0))
                    if pre_size > 0 and (
                            pre_size - post_size) / pre_size >= 0.9:  # 平掉了90%以上的持仓
                        position_closed["coinex"] = True
                        self.logger.info(
                            f"CoinEx {cx_symbol}持仓量显著减少: {pre_size} -> {post_size}")
                    else:
                        position_closed["coinex"] = False
                else:
                    position_closed["coinex"] = False

            # 根据持仓变化情况判断平仓成功与否
            all_closed = True
            for exchange in position_closed:
                if not position_closed[exchange]:
                    all_closed = False
                    break
                    
            if all_closed and position_closed:
                # 所有交易所都成功平仓
                message = f"{symbol}平仓成功"
                self.logger.info(message)
                self.display_manager.add_order_message(message)
                # 更新订单统计
                self.display_manager.update_order_stats("close", True)
                
                # 保存平仓快照
                self._save_position_snapshot(
                    symbol=symbol,
                    action="close",
                    bp_position=pre_bp_position if bp_side else None,
                    hl_position=pre_hl_position if hl_side else None,
                    cx_position=pre_cx_position if cx_side else None,  # 添加CoinEx持仓
                    bp_price=bp_price,
                    hl_price=hl_price,
                    cx_price=cx_price,  # 添加CoinEx价格
                    bp_funding=bp_funding,
                    hl_funding=hl_funding,
                    cx_funding=cx_funding  # 添加CoinEx资金费率
                )
                
                # 清除方向记录
                if symbol in self.position_directions:
                    del self.position_directions[symbol]
                if symbol in self.position_open_times:
                    del self.position_open_times[symbol]
                if symbol in self.entry_prices:
                    del self.entry_prices[symbol]
                if symbol in self.entry_funding_rates:
                    del self.entry_funding_rates[symbol]
                
                # 清除资金费率符号记录
                if symbol in self.funding_diff_signs:
                    del self.funding_diff_signs[symbol]
                    self.logger.debug(f"已清除{symbol}的资金费率符号记录")
                    # 保存更新后的资金费率符号记录到文件
                    self._save_funding_diff_signs()
                
                # 发送通知
                if self.alerter:
                    # 为每个交易所发送通知
                    if bp_side:
                        self.alerter.send_order_notification(
                            symbol=symbol,
                            action="平仓",
                            quantity=bp_size,
                            price=bp_price,
                            side="多" if bp_close_side == "BUY" else "空",
                            exchange="Backpack"
                        )
                    if hl_side:
                        self.alerter.send_order_notification(
                            symbol=symbol,
                            action="平仓",
                            quantity=hl_size,
                            price=hl_price,
                            side="多" if hl_close_side == "BUY" else "空",
                            exchange="Hyperliquid"
                        )
                    if cx_side:
                        self.alerter.send_order_notification(
                            symbol=symbol,
                            action="平仓",
                            quantity=cx_size,
                            price=cx_price,
                            side="多" if cx_close_side == "BUY" else "空",
                            exchange="CoinEx"
                        )
                return True
            elif position_closed:
                # 部分交易所平仓成功，可能需要尝试再次平掉其他交易所
                failed_exchanges = []
                for exchange in position_closed:
                    if not position_closed[exchange]:
                        failed_exchanges.append(exchange)
                
                self.logger.warning(f"{symbol}部分平仓成功，{', '.join(failed_exchanges)}可能需要手动处理")
                # 更新订单统计
                self.display_manager.update_order_stats("close", False)
                # 这里可以添加重试逻辑
                return False
            else:
                # 所有交易所都未成功平仓
                self.logger.error(f"{symbol}在所有交易所均未成功平仓")
                # 更新订单统计
                self.display_manager.update_order_stats("close", False)
                return False

        except Exception as e:
            message = f"{symbol}平仓异常: {e}"
            self.logger.error(message)
            self.display_manager.add_order_message(message)
            return False

    def _update_position_direction_info(
            self, market_data, bp_positions, hl_positions,cx_positions=None):
        """
        更新市场数据中的持仓方向信息

        Args:
            market_data: 市场数据字典
            bp_positions: Backpack持仓信息
            hl_positions: Hyperliquid持仓信息
            cx_positions: CoinEx持仓信息（可选）

        Returns:
            更新后的市场数据字典
        """

        if cx_positions is None:
            cx_positions = {}

        for symbol in market_data:
            bp_symbol = get_backpack_symbol(symbol)
            hl_symbol = symbol
            cx_symbol = symbol

            # 初始化持仓方向信息
            bp_side = None
            hl_side = None
            cx_side = None

            # 检查Backpack持仓
            for pos_symbol, pos_data in bp_positions.items():
                if pos_symbol == bp_symbol:
                    bp_side = pos_data.get("side")
                    break
            
            # 检查Hyperliquid持仓
            for pos_symbol, pos_data in hl_positions.items():
                if pos_symbol == hl_symbol:
                    hl_side = pos_data.get("side")
                    break
                    
            # 检查CoinEx持仓
            for pos_symbol, pos_data in cx_positions.items():
                if pos_symbol == cx_symbol:
                    cx_side = pos_data.get("side")
                    break

            # 更新市场数据中的持仓方向信息
            market_data[symbol]["bp_side"] = bp_side
            market_data[symbol]["hl_side"] = hl_side
            market_data[symbol]["cx_side"] = cx_side  # 添加CoinEx持仓方向
            
            # 检查是否有持仓
            has_position = bp_side is not None or hl_side is not None or cx_side is not None
            market_data[symbol]["position"] = has_position
            
            # 如果有持仓，记录持仓方向
            if has_position:
                self.position_directions[symbol] = {
                    "bp_side": bp_side,
                    "hl_side": hl_side,
                    "cx_side": cx_side  # 添加CoinEx持仓方向
                }
            elif symbol in self.position_directions:
                # 如果没有持仓但之前有记录，则删除记录
                del self.position_directions[symbol]

        return market_data
