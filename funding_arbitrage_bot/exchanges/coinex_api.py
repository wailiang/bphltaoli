# CoinEx API implementation using CCXT
import ccxt.async_support as ccxt
import logging
import time
import traceback
from typing import Dict, Optional, Any, List

logger = logging.getLogger(__name__)

class CoinExAPI:

    def __init__(self, api_key: str, api_secret: str, config: Dict[str, Any], logger: Optional[logging.Logger] = None):
        """
        初始化CoinEx API客户端

        Args:
            api_key: CoinEx API Key
            api_secret: CoinEx API Secret
            config: 全局配置字典
            logger: 日志记录器实例
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.config = config
        self.exchange = None
        self._initialize_exchange()

        # Use the passed logger or create a default one
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__)
            self.logger.setLevel(logging.INFO)
            if not self.logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)

    # Rest of your initialization code...
    def _initialize_exchange(self):
        """Initializes the CCXT CoinEx exchange instance."""
        if not self.api_key or not self.api_secret:
            logger.warning("CoinEx API Key or Secret not provided. CCXT client initialization skipped.")
            self.exchange = None
            return

        try:
            # Use 'coinex' instead of 'bybit' for config lookup
            options = self.config.get('exchanges', {}).get('coinex', {}).get('options', {})
            # Ensure we target the futures market if specified or needed (Adjust if CoinEx uses a different default)
            if 'defaultType' not in options:
                # Check CoinEx CCXT documentation for the correct default type (e.g., 'spot', 'future', 'swap')
                options['defaultType'] = 'future' # Assuming futures, adjust as needed

            ccxt_config = {
                'apiKey': self.api_key,
                'secret': self.api_secret,
                'enableRateLimit': True,
                'options': options
            }

            # Use ccxt.coinex()
            self.exchange = ccxt.coinex(ccxt_config)
            logger.info(f"CCXT CoinEx client initialized (Default Type: {options.get('defaultType', 'N/A')}).")
        except ccxt.AuthenticationError as e:
            logger.error(f"CCXT CoinEx Authentication Error: {e}. Check API Key/Secret.", exc_info=True)
            self.exchange = None
        except Exception as e:
            logger.error(f"Failed to initialize CCXT CoinEx client: {e}", exc_info=True)
            self.exchange = None

    async def load_markets(self, reload: bool = False):
        """Loads market data from the exchange."""
        if not self.exchange:
            logger.warning("Cannot load markets, CCXT exchange not initialized.")
            return None
        try:
            markets = await self.exchange.load_markets(reload)
            logger.info(f"Successfully loaded/reloaded {len(markets)} markets from CoinEx.")
            return markets
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            logger.error(f"Failed to load CoinEx markets: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred loading CoinEx markets: {e}", exc_info=True)
            return None

    async def close(self):
        """Closes the exchange connection."""
        if self.exchange:
            try:
                await self.exchange.close()
                logger.info("CCXT CoinEx connection closed.")
            except Exception as e:
                logger.error(f"Error closing CCXT CoinEx connection: {e}", exc_info=True)
        self.exchange = None

    def _get_ccxt_symbol(self, base_symbol: str) -> Optional[str]:
        """
        Converts base symbol (e.g., BTC) to CCXT market ID (e.g., BTC/USDT).
        Adjust the logic based on CoinEx's symbol format (spot vs futures/swaps).
        Returns None if the base symbol is invalid or not configured.
        """
        if not base_symbol or not isinstance(base_symbol, str):
            logger.warning(f"Invalid base_symbol provided: {base_symbol}")
            return None

        # Check if a specific symbol is defined in trading_pairs config
        for pair_config in self.config.get('trading_pairs', []):
            if pair_config.get('symbol') == base_symbol:
                # Allow overriding the CCXT symbol format in config
                # Use 'coinex_symbol' key
                if 'coinex_symbol' in pair_config:
                    return pair_config['coinex_symbol']
                else:
                    # Default format - Adjust based on CoinEx standard (e.g., BTC/USDT for spot, BTC/USDT:USDT for swap)
                    # Assuming perpetual swap format for now, verify with CoinEx docs
                    return f"{base_symbol.upper()}/USDT:USDT"

        # If symbol not found in trading_pairs, maybe log a warning or fallback
        logger.warning(f"Base symbol '{base_symbol}' not found in trading_pairs config. Falling back to default format.")
        # Fallback format - Adjust as needed
        return f"{base_symbol.upper()}/USDT:USDT"

    async def get_ticker(self, base_symbol: str) -> Optional[Dict]:
        """Fetches ticker information."""
        if not self.exchange:
            logger.warning("Cannot fetch ticker, exchange not initialized.")
            return None
        ccxt_symbol = self._get_ccxt_symbol(base_symbol)
        if not ccxt_symbol: return None

        try:
            ticker = await self.exchange.fetch_ticker(ccxt_symbol)
            return ticker
        except ccxt.BadSymbol as e:
            logger.error(f"CoinEx BadSymbol fetching ticker for {base_symbol} ({ccxt_symbol}): {e}")
        except ccxt.NetworkError as e:
            logger.error(f"CoinEx NetworkError fetching ticker for {ccxt_symbol}: {e}")
        except ccxt.ExchangeError as e:
            logger.error(f"CoinEx ExchangeError fetching ticker for {ccxt_symbol}: {e}")
        except Exception as e:
            logger.error(f"Error fetching ticker for {ccxt_symbol}: {e}", exc_info=True)
        return None

    async def get_funding_rate(self, base_symbol: str) -> Optional[float]:
        """Fetches the current funding rate for a symbol."""
        if not self.exchange:
            logger.warning("Cannot fetch funding rate, exchange not initialized.")
            return None
        ccxt_symbol = self._get_ccxt_symbol(base_symbol)
        if not ccxt_symbol: return None

        # Ensure the symbol format is correct for funding rates (likely perpetual swap)
        if ':' not in ccxt_symbol:
             logger.warning(f"Symbol {ccxt_symbol} might not be a perpetual swap, funding rate may not be available.")
             # Optionally adjust symbol format here if needed, e.g., append ':USDT'

        try:
            funding_rate_info = await self.exchange.fetch_funding_rate(ccxt_symbol)
            rate = funding_rate_info.get('fundingRate')
            if rate is not None:
                logger.debug(f"Fetched funding rate for {ccxt_symbol}: {rate}")
                return float(rate)
            else:
                logger.warning(f"Funding rate not found in response for {ccxt_symbol}: {funding_rate_info}")
                return None
        except ccxt.NotSupported as e:
            logger.error(f"CoinEx does not support fetch_funding_rate via CCXT or for {ccxt_symbol}: {e}")
        except ccxt.BadSymbol as e:
            logger.error(f"CoinEx BadSymbol fetching funding rate for {base_symbol} ({ccxt_symbol}): {e}")
        except ccxt.NetworkError as e:
            logger.error(f"CoinEx NetworkError fetching funding rate for {ccxt_symbol}: {e}")
        except ccxt.ExchangeError as e:
            logger.error(f"CoinEx ExchangeError fetching funding rate for {ccxt_symbol}: {e}")
        except Exception as e:
            logger.error(f"Error fetching funding rate for {ccxt_symbol}: {e}", exc_info=True)
        return None

    async def get_all_funding_rates(self) -> Dict[str, float]:
        """Fetches funding rates for all available perpetual swap markets."""
        if not self.exchange:
            logger.warning("Cannot fetch all funding rates, exchange not initialized.")
            return {}

        all_rates = {}
        try:
            # Check if CoinEx supports fetch_funding_rates
            if not self.exchange.has.get('fetchFundingRates'):
                 logger.error("CoinEx does not support fetch_funding_rates via CCXT.")
                 # Fallback: Fetch rates individually for configured symbols
                 symbols_to_check = [cfg.get('symbol') for cfg in self.config.get('trading_pairs', []) if cfg.get('symbol')]
                 for base_sym in symbols_to_check:
                     rate = await self.get_funding_rate(base_sym)
                     if rate is not None:
                         all_rates[base_sym] = rate
                 logger.info(f"Fetched {len(all_rates)} funding rates individually from CoinEx.")
                 return all_rates

            funding_rates_data = await self.exchange.fetch_funding_rates()

            for symbol, rate_info in funding_rates_data.items():
                # Filter only perpetuals if necessary (adjust filter based on CoinEx symbol format)
                if ':' in symbol:  # Assuming ':' indicates perpetuals
                    rate = rate_info.get('fundingRate')
                    if rate is not None:
                        # Extract base symbol (e.g., BTC from BTC/USDT:USDT)
                        base_symbol = symbol.split('/')[0]
                        all_rates[base_symbol] = float(rate)

            logger.info(f"Successfully fetched {len(all_rates)} funding rates from CoinEx.")
            return all_rates

        except ccxt.NotSupported as e:
            logger.error(f"CoinEx does not support fetch_funding_rates via CCXT: {e}")
        except ccxt.NetworkError as e:
            logger.error(f"CoinEx NetworkError fetching all funding rates: {e}")
        except ccxt.ExchangeError as e:
            logger.error(f"CoinEx ExchangeError fetching all funding rates: {e}")
        except Exception as e:
            logger.error(f"Error fetching all funding rates: {e}", exc_info=True)
        return {}

    async def get_orderbook(self, base_symbol: str, limit: int = 20) -> Optional[Dict]:
        """Fetches order book data for a symbol."""
        if not self.exchange:
            logger.warning("Cannot fetch orderbook, exchange not initialized.")
            return None

        ccxt_symbol = self._get_ccxt_symbol(base_symbol)
        if not ccxt_symbol: return None

        try:
            orderbook_data = await self.exchange.fetch_order_book(ccxt_symbol, limit=limit)

            # Convert to the format expected by the strategy (assuming px, sz format)
            # Verify this format matches your strategy's needs
            orderbook = {
                "bids": [{"px": float(bid[0]), "sz": float(bid[1])} for bid in orderbook_data["bids"]],
                "asks": [{"px": float(ask[0]), "sz": float(ask[1])} for ask in orderbook_data["asks"]]
            }

            return orderbook

        except ccxt.BadSymbol as e:
            logger.error(f"CoinEx BadSymbol fetching orderbook for {base_symbol} ({ccxt_symbol}): {e}")
        except ccxt.NetworkError as e:
            logger.error(f"CoinEx NetworkError fetching orderbook for {ccxt_symbol}: {e}")
        except ccxt.ExchangeError as e:
            logger.error(f"CoinEx ExchangeError fetching orderbook for {ccxt_symbol}: {e}")
        except Exception as e:
            logger.error(f"Error fetching orderbook for {ccxt_symbol}: {e}", exc_info=True)
        return None

    async def get_positions(self) -> Dict[str, Dict]:
        """
        Fetches open positions and adapts them to the bot's expected format.
        Returns a dictionary mapping base_symbol to position details.
        NOTE: CCXT's fetch_positions support varies. Verify for CoinEx futures/swaps.
        """
        if not self.exchange:
            logger.warning("Cannot fetch positions, exchange not initialized.")
            return {}

        # Check if the method is supported
        if not self.exchange.has.get('fetchPositions'):
            logger.error("CoinEx does not support fetch_positions via CCXT for the selected market type.")
            return {}

        adapted_positions = {}
        try:
            # Specify market type if needed (e.g., 'swap', 'future')
            # params = {'type': 'swap'} # Adjust based on CoinEx CCXT implementation
            positions = await self.exchange.fetch_positions() # Add params if needed

            for pos in positions:
                # Adapt the position data based on CCXT's response structure for CoinEx
                # This structure can vary significantly between exchanges in CCXT
                size_str = pos.get('contracts') or pos.get('contractSize') or pos.get('info', {}).get('size') # Example keys, check actual response
                if size_str is None: continue
                size = float(size_str)

                if size != 0:
                    ccxt_symbol = pos.get('symbol')
                    if not ccxt_symbol: continue

                    base_symbol = ccxt_symbol.split('/')[0]

                    # Determine side ('long'/'short' or 'buy'/'sell' based on response)
                    side_info = pos.get('side') # Check if 'long'/'short'
                    if side_info == 'long':
                        side = 'BUY'
                    elif side_info == 'short':
                        side = 'SELL'
                    else:
                        # Alternative check if side is based on size sign (less common in CCXT standard)
                        # side = 'BUY' if size > 0 else 'SELL' # Be careful with this logic
                        logger.warning(f"Could not determine side for position: {pos}")
                        continue

                    # Extract other relevant fields, checking actual keys from CoinEx response via CCXT
                    entry_price_str = pos.get('entryPrice') or pos.get('info', {}).get('avgPrice')
                    mark_price_str = pos.get('markPrice') or pos.get('info', {}).get('markPrice')
                    unrealized_pnl_str = pos.get('unrealizedPnl') or pos.get('info', {}).get('unrealisedPnl')
                    leverage_str = pos.get('leverage') or pos.get('info', {}).get('leverage')
                    liquidation_price_str = pos.get('liquidationPrice') or pos.get('info', {}).get('liquidationPrice')

                    adapted_positions[base_symbol] = {
                        'symbol': ccxt_symbol,
                        'base_symbol': base_symbol,
                        'size': size,
                        'side': side,
                        'entry_price': float(entry_price_str) if entry_price_str is not None else None,
                        'mark_price': float(mark_price_str) if mark_price_str is not None else None,
                        'unrealized_pnl': float(unrealized_pnl_str) if unrealized_pnl_str is not None else None,
                        'leverage': float(leverage_str) if leverage_str is not None else None,
                        'liquidation_price': float(liquidation_price_str) if liquidation_price_str is not None else None,
                        'raw_position': pos # Keep the original data for debugging
                    }
            return adapted_positions

        except ccxt.AuthenticationError as e:
            logger.error(f"CoinEx AuthenticationError fetching positions: {e}")
        except ccxt.NotSupported as e:
             logger.error(f"CoinEx does not support fetch_positions for the selected market type via CCXT: {e}")
        except ccxt.NetworkError as e:
            logger.error(f"CoinEx NetworkError fetching positions: {e}")
        except ccxt.ExchangeError as e:
            logger.error(f"CoinEx ExchangeError fetching positions: {e}")
        except Exception as e:
            logger.error(f"Error fetching or adapting positions: {e}", exc_info=True)
        return {}

    async def place_order(self, base_symbol: str, side: str, size: float, price: Optional[float] = None, order_type: str = 'LIMIT', params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Places an order on CoinEx.

        Args:
            base_symbol: The base asset symbol (e.g., "BTC").
            side: "BUY" or "SELL".
            size: Order quantity in base asset terms.
            price: Limit price (required for LIMIT orders).
            order_type: "LIMIT" or "MARKET".
            params: Additional parameters for CCXT create_order (e.g., {'timeInForce': 'GTC'}).

        Returns:
            The order dictionary from CCXT, or None on failure.
        """
        if not self.exchange:
            logger.warning("Cannot place order, exchange not initialized.")
            return None

        # Basic Input Validation
        if not base_symbol or side.upper() not in ["BUY", "SELL"] or not isinstance(size, (int, float)) or size <= 0:
            logger.error(f"Invalid order parameters: symbol={base_symbol}, side={side}, size={size}")
            return None
        if order_type.upper() == 'LIMIT' and (price is None or not isinstance(price, (int, float)) or price <= 0):
            logger.error(f"Invalid price for LIMIT order: price={price}")
            return None

        ccxt_symbol = self._get_ccxt_symbol(base_symbol)
        if not ccxt_symbol: return None

        ccxt_side = side.lower()
        ccxt_type = order_type.lower()

        final_params = {}
        if params:
            final_params.update(params)
        # Add specific CoinEx params if needed, e.g., for margin mode or position side
        # final_params['positionSide'] = 'BOTH' # Example, check CoinEx docs

        try:
            logger.info(f"Placing order: {ccxt_symbol} {ccxt_side} {size} {ccxt_type} @ {price if price else 'N/A'} Params: {final_params}")

            order = await self.exchange.create_order(
                symbol=ccxt_symbol,
                type=ccxt_type,
                side=ccxt_side,
                amount=size,
                price=price,
                params=final_params
            )
            logger.info(f"Order placed successfully for {ccxt_symbol}. Order ID: {order.get('id')}")
            return order

        except ccxt.InsufficientFunds as e:
            logger.error(f"CoinEx InsufficientFunds placing order {ccxt_symbol} {side} {size}: {e}")
        except ccxt.InvalidOrder as e:
            logger.error(f"CoinEx InvalidOrder placing order {ccxt_symbol} {side} {size} @ {price}: {e}")
        except ccxt.NetworkError as e:
            logger.error(f"CoinEx NetworkError placing order {ccxt_symbol}: {e}")
        except ccxt.ExchangeError as e: # Catch specific CoinEx errors if possible
            logger.error(f"CoinEx ExchangeError placing order {ccxt_symbol}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error placing order for {ccxt_symbol}: {e}", exc_info=True)
        return None

    async def cancel_order(self, order_id: str, base_symbol: str) -> Optional[Dict]:
        """Cancels an open order."""
        if not self.exchange:
            logger.warning("Cannot cancel order, exchange not initialized.")
            return None

        ccxt_symbol = self._get_ccxt_symbol(base_symbol)
        if not ccxt_symbol: return None

        try:
            logger.info(f"Attempting to cancel order ID: {order_id} for symbol: {ccxt_symbol}")
            # Some exchanges might require extra params for cancel_order
            result = await self.exchange.cancel_order(order_id, ccxt_symbol)
            logger.info(f"Cancel order request successful for ID: {order_id}. Result: {result}")
            # CCXT often returns order info upon cancellation, adapt if needed
            return result

        except ccxt.OrderNotFound as e:
            # It's often safe to assume an OrderNotFound means it's already closed or cancelled
            logger.warning(f"CoinEx OrderNotFound cancelling order {order_id} for {ccxt_symbol}: {e}. Assuming already closed/filled.")
            # Return a simulated success or specific status if your logic requires it
            return {'info': {'status': 'CANCELED_OR_FILLED', 'orderId': order_id}, 'id': order_id, 'symbol': ccxt_symbol}

        except ccxt.InvalidOrder as e:
            # Similar to OrderNotFound, might indicate it's no longer cancellable
            logger.warning(f"CoinEx InvalidOrder cancelling order {order_id} for {ccxt_symbol}: {e}. Likely already closed/filled.")
            return {'info': {'status': 'FILLED_OR_INVALID', 'orderId': order_id}, 'id': order_id, 'symbol': ccxt_symbol}

        except ccxt.NetworkError as e:
            logger.error(f"CoinEx NetworkError cancelling order {order_id} for {ccxt_symbol}: {e}")
        except ccxt.ExchangeError as e:
            logger.error(f"CoinEx ExchangeError cancelling order {order_id} for {ccxt_symbol}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error cancelling order {order_id} for {ccxt_symbol}: {e}", exc_info=True)
        return None

    async def get_balance(self) -> Optional[Dict]:
        """Fetches account balance information."""
        if not self.exchange:
            logger.warning("Cannot fetch balance, exchange not initialized.")
            return None

        try:
            # Specify account type if needed (e.g., 'spot', 'margin', 'future', 'swap')
            # Check CoinEx CCXT docs for required params
            # balance = await self.exchange.fetch_balance({'type': 'swap'})
            balance = await self.exchange.fetch_balance() # Default might work for unified account
            return balance
        except ccxt.AuthenticationError as e:
            logger.error(f"CoinEx AuthenticationError fetching balance: {e}")
        except ccxt.NetworkError as e:
            logger.error(f"CoinEx NetworkError fetching balance: {e}")
        except ccxt.ExchangeError as e:
            logger.error(f"CoinEx ExchangeError fetching balance: {e}")
        except Exception as e:
            logger.error(f"Error fetching balance: {e}", exc_info=True)
        return None

    async def get_price(self, base_symbol: str) -> Optional[float]:
        """Gets the current price for a symbol using the ticker."""
        ticker = await self.get_ticker(base_symbol)
        if ticker and 'last' in ticker and ticker['last'] is not None:
            try:
                return float(ticker['last'])
            except (ValueError, TypeError) as e:
                 logger.error(f"Could not convert ticker 'last' price to float for {base_symbol}: {ticker['last']} - Error: {e}")
                 return None
        elif ticker:
             logger.warning(f"Ticker for {base_symbol} received, but 'last' price is missing or None: {ticker}")
        return None