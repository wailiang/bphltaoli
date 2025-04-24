#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
显示管理模块

管理终端显示，包括价格和资金费率表格，日志信息输出到日志文件而不在终端显示
"""

import os
import sys
import time
import logging
from typing import Dict, List, Optional
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.text import Text
from rich import box

class DisplayManager:
    """显示管理类，负责管理终端显示"""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化显示管理器
        
        Args:
            logger: 日志记录器
        """
        self.logger = logger or logging.getLogger(__name__)
        # 使用系统输出文件
        self.console = Console(file=sys.__stdout__)
        self.current_table = None  # 保存当前表格的引用
        self.last_update_time = time.time()
        self.order_stats = {
            "total_orders": 0,
            "successful_orders": 0,
            "failed_orders": 0,
            "last_order_time": None,
            "last_order_message": None
        }
        
        # 测试直接输出
        print("初始化DisplayManager", file=sys.__stdout__)
        
        # 创建Live显示上下文
        self.live = Live(
            console=self.console,
            refresh_per_second=4,  # 增加刷新率以使表格更新更流畅
            auto_refresh=True,
            transient=False  # 确保表格保持在屏幕上
        )
        
    def start(self):
        """启动显示"""
        # 显示开始信息直接使用系统输出
        print("资金费率套利机器人已启动，日志信息输出到日志文件", file=sys.__stdout__)
        print("按 Ctrl+C 退出", file=sys.__stdout__)
        
        # 创建初始表格
        initial_table = Table(title="正在加载市场数据...", box=box.ROUNDED)
        initial_table.add_column("状态", style="bold")
        initial_table.add_row("等待数据更新...")
        self.current_table = initial_table
        
        # 启动Live显示
        try:
            print("开始启动Live显示...", file=sys.__stdout__)
            self.live.start(self.current_table)
            print("Live显示已启动", file=sys.__stdout__)
        except Exception as e:
            print(f"启动Live显示出错: {e}", file=sys.__stdout__)
            raise
        
    def stop(self):
        """停止显示"""
        try:
            print("正在停止Live显示...", file=sys.__stdout__)
            self.live.stop()
            print("Live显示已停止", file=sys.__stdout__)
        except Exception as e:
            print(f"停止Live显示出错: {e}", file=sys.__stdout__)
        
    def update_market_data(self, data: Dict[str, Dict]):
        """
        更新市场数据显示
        
        Args:
            data: 市场数据字典
        """
        # 创建市场数据表格
        table = Table(
            title="市场数据",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold white",
            title_style="bold white"
        )
        
        # 添加列
        table.add_column("币种", style="cyan", justify="center")
        table.add_column("BP价格", style="green", justify="right")
        table.add_column("HL价格", style="green", justify="right")
        table.add_column("CX价格", style="green", justify="right")  # 添加CoinEx价格列
        table.add_column("价格差%", style="yellow", justify="right")
        table.add_column("BP费率(8h)", style="blue", justify="right")
        table.add_column("HL原始(1h)", style="blue", justify="right")
        table.add_column("HL调整(8h)", style="blue", justify="right")
        table.add_column("CX费率(8h)", style="blue", justify="right")  # 添加CoinEx费率列
        table.add_column("费率差%", style="magenta", justify="right")
        table.add_column("总滑点%", style="red", justify="right")
        table.add_column("BP方向", style="red", justify="center")
        table.add_column("HL方向", style="red", justify="center")
        table.add_column("CX方向", style="red", justify="center")  # 添加CoinEx方向列
        
        try:
            # 计数有效数据
            valid_data_count = 0
            
            # 记录市场数据处理
            self.logger.debug(f"市场数据字典键: {list(data.keys())}")
            
            # 创建数据行列表，稍后按资金费率差的绝对值排序
            rows_data = []
            
            # 填充数据行
            for symbol, symbol_data in data.items():
                bp_data = symbol_data.get("backpack", {})
                hl_data = symbol_data.get("hyperliquid", {})
                cx_data = symbol_data.get("coinex", {})  # 获取CoinEx数据
                
                if not isinstance(bp_data, dict):
                    self.logger.warning(f"BP数据格式错误: {bp_data}")
                    bp_data = {"price": None, "funding_rate": None}
                    
                if not isinstance(hl_data, dict):
                    self.logger.warning(f"HL数据格式错误: {hl_data}")
                    hl_data = {"price": None, "funding_rate": None}
                
                if not isinstance(cx_data, dict):
                    self.logger.warning(f"CX数据格式错误: {cx_data}")
                    cx_data = {"price": None, "funding_rate": None}
                
                # 获取价格，确保数据有效
                bp_price = bp_data.get("price")
                hl_price = hl_data.get("price")
                cx_price = cx_data.get("price")  # 获取CoinEx价格
                
                if bp_price is not None or hl_price is not None or cx_price is not None:
                    valid_data_count += 1
                
                # 计算价格差 (使用有效的价格对进行计算)
                price_diff = 0
                if bp_price and hl_price:
                    price_diff = (bp_price - hl_price) / hl_price * 100
                elif bp_price and cx_price:
                    price_diff = (bp_price - cx_price) / cx_price * 100
                elif hl_price and cx_price:
                    price_diff = (hl_price - cx_price) / cx_price * 100
                    
                # 计算资金费率差
                bp_funding = bp_data.get("funding_rate")
                hl_funding = hl_data.get("funding_rate")
                adjusted_hl_funding = hl_data.get("adjusted_funding_rate")  # 直接使用存储的调整后资金费率
                cx_funding = cx_data.get("funding_rate")  # 获取CoinEx资金费率
                
                # 计算调整后的资金费率差 (考虑所有可能的交易所组合)
                funding_diff = 0
                if bp_funding is not None and adjusted_hl_funding is not None:
                    funding_diff = (bp_funding - adjusted_hl_funding) * 100
                elif bp_funding is not None and cx_funding is not None:
                    funding_diff = (bp_funding - cx_funding) * 100
                elif adjusted_hl_funding is not None and cx_funding is not None:
                    funding_diff = (adjusted_hl_funding - cx_funding) * 100
                
                # 计算资金费率差的绝对值用于排序
                funding_diff_abs = abs(funding_diff)
                
                # 获取滑点信息，并记录当前符号所有可用键
                self.logger.debug(f"{symbol}的市场数据键: {list(symbol_data.keys())}")
                total_slippage = symbol_data.get("total_slippage")
                
                # 记录滑点获取情况
                self.logger.debug(f"获取{symbol}的总滑点: {total_slippage}")
                
                if total_slippage is None:
                    # 尝试从流动性分析中获取滑点信息
                    liquidity_analysis = symbol_data.get("liquidity_analysis", {})
                    self.logger.debug(f"{symbol}的流动性分析数据键: {list(liquidity_analysis.keys()) if liquidity_analysis else 'None'}")
                    
                    if liquidity_analysis:
                        # 确定做多和做空的交易所 (考虑三个交易所)
                        exchanges_with_data = []
                        funding_rates = []
                        
                        if bp_funding is not None:
                            exchanges_with_data.append("backpack")
                            funding_rates.append(bp_funding)
                        
                        if adjusted_hl_funding is not None:
                            exchanges_with_data.append("hyperliquid")
                            funding_rates.append(adjusted_hl_funding)
                            
                        if cx_funding is not None:
                            exchanges_with_data.append("coinex")
                            funding_rates.append(cx_funding)
                            
                        if len(exchanges_with_data) >= 2:
                            # 找出资金费率最低和最高的交易所
                            min_idx = funding_rates.index(min(funding_rates))
                            max_idx = funding_rates.index(max(funding_rates))
                            
                            long_exchange = exchanges_with_data[min_idx]  # 资金费率低的做多
                            short_exchange = exchanges_with_data[max_idx]  # 资金费率高的做空
                        else:
                            # 默认设置
                            long_exchange = "hyperliquid"
                            short_exchange = "backpack"
                            
                        # 提取交易所的流动性分析数据
                        long_exchange_data = liquidity_analysis.get(long_exchange, {})
                        short_exchange_data = liquidity_analysis.get(short_exchange, {})
                        
                        self.logger.debug(f"{symbol}的{long_exchange}流动性分析键: {list(long_exchange_data.keys()) if long_exchange_data else 'None'}")
                        self.logger.debug(f"{symbol}的{short_exchange}流动性分析键: {list(short_exchange_data.keys()) if short_exchange_data else 'None'}")
                        
                        # 提取滑点信息
                        long_slippage = long_exchange_data.get("bid_slippage_pct", 0)
                        short_slippage = short_exchange_data.get("ask_slippage_pct", 0)
                        
                        # 计算总滑点
                        if long_slippage is not None and short_slippage is not None:
                            total_slippage = long_slippage + short_slippage
                            self.logger.debug(f"{symbol}的总滑点计算: {long_slippage} + {short_slippage} = {total_slippage}")
                
                # 获取持仓信息（如果存在）
                bp_position_side = symbol_data.get("bp_position_side", None)
                hl_position_side = symbol_data.get("hl_position_side", None)
                cx_position_side = symbol_data.get("cx_position_side", None)  # 获取CoinEx持仓方向
                
                # 存储行数据和排序值
                row_data = {
                    "symbol": symbol,
                    "bp_price": bp_price,
                    "hl_price": hl_price,
                    "cx_price": cx_price,  # 添加CoinEx价格
                    "price_diff": price_diff,
                    "bp_funding": bp_funding,
                    "hl_funding": hl_funding,
                    "adjusted_hl_funding": adjusted_hl_funding,
                    "cx_funding": cx_funding,  # 添加CoinEx资金费率
                    "funding_diff": funding_diff,
                    "funding_diff_abs": funding_diff_abs,  # 用于排序的绝对值
                    "total_slippage": total_slippage,
                    "has_position": symbol_data.get("position"),
                    "bp_position_side": bp_position_side,
                    "hl_position_side": hl_position_side,
                    "cx_position_side": cx_position_side  # 添加CoinEx持仓方向
                }
                rows_data.append(row_data)
            
            # 按资金费率差的绝对值排序（降序）
            sorted_rows = sorted(rows_data, key=lambda x: x["funding_diff_abs"], reverse=True)
            
            # 将排序后的数据添加到表格
            for row in sorted_rows:
                table.add_row(
                    row["symbol"],
                    f"{row['bp_price']:.2f}" if row['bp_price'] is not None else "N/A",
                    f"{row['hl_price']:.2f}" if row['hl_price'] is not None else "N/A",
                    f"{row['cx_price']:.2f}" if row['cx_price'] is not None else "N/A",  # 添加CoinEx价格
                    f"{row['price_diff']:+.4f}" if row['price_diff'] != 0 else "N/A",
                    f"{row['bp_funding']:.6f}" if row['bp_funding'] is not None else "0.000000",
                    f"{row['hl_funding']:.6f}" if row['hl_funding'] is not None else "0.000000",
                    f"{row['adjusted_hl_funding']:.6f}" if row['adjusted_hl_funding'] is not None else "0.000000",
                    f"{row['cx_funding']:.6f}" if row['cx_funding'] is not None else "0.000000",  # 添加CoinEx资金费率
                    f"{row['funding_diff']:+.6f}" if row['funding_diff'] != 0 else "0.000000",
                    f"{row['total_slippage']:.4f}" if row['total_slippage'] is not None else "N/A",
                    "多" if row['bp_position_side'] == "BUY" else "空" if row['bp_position_side'] == "SELL" else "-",
                    "多" if row['hl_position_side'] == "BUY" else "空" if row['hl_position_side'] == "SELL" else "-",
                    "多" if row['cx_position_side'] == "BUY" else "空" if row['cx_position_side'] == "SELL" else "-"  # 添加CoinEx持仓方向
                )
        
            # 创建订单统计信息表格
            stats_table = Table(
                title="订单统计信息",
                box=box.ROUNDED,
                show_header=False,
                title_style="bold white"
            )
            
            stats_table.add_column("项目", style="cyan")
            stats_table.add_column("数值", style="yellow")
            
            # 添加统计信息
            stats_table.add_row("总订单数", str(self.order_stats["total_orders"]))
            stats_table.add_row("成功订单", str(self.order_stats["successful_orders"]))
            stats_table.add_row("失败订单", str(self.order_stats["failed_orders"]))
            
            last_time = "无" if not self.order_stats["last_order_time"] else self.order_stats["last_order_time"].strftime("%H:%M:%S")
            stats_table.add_row("最近订单时间", last_time)
            
            last_msg = "无" if not self.order_stats["last_order_message"] else self.order_stats["last_order_message"]
            stats_table.add_row("最近订单消息", last_msg[:50] + "..." if last_msg and len(last_msg) > 50 else last_msg)
            
            # 创建组合布局
            main_table = Table.grid(padding=1)
            main_table.add_row(table)
            main_table.add_row(Panel(stats_table, border_style="blue"))
            
            # 记录调试信息
            now = time.time()
            self.last_update_time = now
            
            # 保存并直接更新表格
            self.current_table = main_table
            
            self.logger.debug(f"更新表格中，包含{valid_data_count}个有效数据")
            
            # 尝试使用直接的控制台渲染
            try:
                self.live.update(self.current_table)
                self.logger.debug("表格已更新")
            except Exception as e:
                self.logger.error(f"表格更新出错: {e}")
                
                # 如果live更新失败，尝试直接渲染
                try:
                    print("\n" + "-" * 80, file=sys.__stdout__)
                    self.console.print(self.current_table)
                    print("-" * 80, file=sys.__stdout__)
                except Exception as direct_e:
                    self.logger.error(f"直接渲染表格出错: {direct_e}")
            
        except Exception as e:
            self.logger.error(f"更新表格显示出错: {e}")
            # 出错也不中断程序
        
    def add_order_message(self, message: str):
        """
        添加订单信息 - 只输出到日志而不显示在终端
        
        Args:
            message: 订单信息
        """
        try:
            # 更新订单统计
            self.order_stats["total_orders"] += 1
            if "成功" in message or "已完成" in message:
                self.order_stats["successful_orders"] += 1
            elif "失败" in message or "错误" in message:
                self.order_stats["failed_orders"] += 1
                
            # 更新最近订单信息
            self.order_stats["last_order_time"] = datetime.now()
            self.order_stats["last_order_message"] = message
            
            # 记录到日志
            self.logger.info(f"订单消息: {message}")
        except Exception as e:
            self.logger.error(f"处理订单消息时出错: {e}")
            # 出错也不中断程序
            
    def update_order_stats(self, action: str, success: bool):
        """
        根据持仓变化验证结果更新订单统计信息
        
        Args:
            action: 操作类型，"open"表示开仓，"close"表示平仓
            success: 是否成功，基于持仓变化验证的结果
        """
        try:
            # 更新订单统计
            self.order_stats["total_orders"] += 1
            
            if success:
                self.order_stats["successful_orders"] += 1
                action_desc = "开仓" if action == "open" else "平仓"
                self.order_stats["last_order_message"] = f"{action_desc}成功 (持仓变化验证)"
            else:
                self.order_stats["failed_orders"] += 1
                action_desc = "开仓" if action == "open" else "平仓"
                self.order_stats["last_order_message"] = f"{action_desc}失败 (持仓变化验证)"
                
            # 更新最近订单时间
            self.order_stats["last_order_time"] = datetime.now()
            
            # 记录到日志
            msg = f"{action_desc}{'成功' if success else '失败'} (持仓变化验证)"
            self.logger.info(f"订单统计更新: {msg}")
            
        except Exception as e:
            self.logger.error(f"更新订单统计时出错: {e}")
            # 出错也不中断程序