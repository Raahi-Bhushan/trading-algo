
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yaml
from logger import logger
# from brokers.zerodha import ZerodhaBroker
from brokers import BrokerGateway, OrderRequest, Exchange, OrderType, TransactionType, ProductType
import datetime
import time
import yaml
import logging
from typing import Dict, List, Tuple
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import mibian 

class WaveStrategy:
    """
    Main trading system that implements wave trading strategy

    PS: This strategy is compatible with both Zerodha and Fyers brokers.
    """
    
    def __init__(self, config: Dict, broker, order_tracker=None):
        # Core configuration
        self.config = config
        self.symbol_name = config["exchange"] + ":" + config.get("symbol_name", None)
        self.buy_gap = float(config["buy_gap"])
        self.sell_gap = float(config["sell_gap"])
        self.cool_off_time = int(config["cool_off_time"])
        self.buy_quantity = int(config["buy_quantity"])
        self.sell_quantity = int(config["sell_quantity"])
        self.quantity = self.sell_quantity # TODO: Confirm with Vibhu - we are doing this in initilise function
        self.product_type = config.get("product_type", "NRML")
        self.tag = config.get("tag", "WAVE_SCRAPER")
        self.order_type = config.get("order_type", "LIMIT")
        self.variety = config.get("variety", "REGULAR")
        self.lot_size = int(config.get("lot_size", None))
        
        # Order tracking
        self.order_tracker = order_tracker


        # For Greeks Calculation
        self.min_nifty_delta = float(config.get("min_nifty_delta", -100))
        self.max_nifty_delta = float(config.get("max_nifty_delta", 100))
        self.min_bank_nifty_delta = float(config.get("min_bank_nifty_delta", -100))
        self.max_bank_nifty_delta = float(config.get("max_bank_nifty_delta", 100))
        self.interest_rate = float(config.get("interest_rate", 10)) # Default 10%
        self.todays_volatility = float(config.get("todays_volatility", 20)) # Default 20%
        self.delta_calculation_days = int(config.get("delta_calculation_days", 10)) # Setting this to None will mimic restrict_days = False# 
        self.margin_spread = float(config.get("margin_spread", 100))
        self.margin_single_pe_ce = float(config.get("margin_single_pe_ce", 100))
        self.margin_both_pe_ce = float(config.get("margin_both_pe_ce", 100))
        
        # System state
        self.broker = broker
        self.scraper_last_price = 0
        self.already_executing_order = 0
        self.initial_positions = {}
        self.orders = {}  # Active orders tracking
        
        # Generate multiplier scale for gap scaling
        self.multiplier_scale = self._generate_multiplier_scale()
        
        # Initialize broker and get initial position
        logger.info("Downloading instruments...")
        self.broker.download_instruments() 
        self.all_instruments = self.broker.get_instruments() 
        
        self.initial_positions['position'] = self._get_position_for_symbol()
        
        # Get initial market price
        quote = self.broker.get_quote(self.symbol_name)
        self.scraper_last_price = quote.last_price

        # Get the previous wave sell price
        self.prev_wave_sell_price = None
        self.prev_wave_buy_price = None
        self.prev_quote_price = None

        self.prev_wave_sell_qty = None
        self.prev_wave_buy_qty = None

        self.handle_order_update_call_tracker = {}
        self.handle_order_update_call_tracker_response_dict = {}

        
        logger.info(f"System initialized for {self.symbol_name}")
        logger.info(f"Initial position: {self.initial_positions['position']}, Last Price: {self.scraper_last_price}")

    def _generate_multiplier_scale(self, levels: int = 10) -> Dict[str, List[float]]:
        """Generate multiplier scale for dynamic gap scaling based on position imbalance"""
        buy_scale = [1.3, 1.7, 2.5, 3, 10, 10, 10, 15, 15, 15]
        sell_scale = [1.3, 1.7, 2.5, 3, 10, 10, 10, 15, 15, 15]
        
        multiplier_scale = {"0": [1.0, 1.0]}  # Neutral position
        
        for i in range(1, levels + 1):
            multiplier_scale[str(i)] = [buy_scale[i - 1], 1.0]
            multiplier_scale[str(-i)] = [1.0, sell_scale[i - 1]]
        
        return multiplier_scale

    def _get_symbol_type(self, symbol_name: str) -> str:
        """Get symbol type from symbol name"""
        exchange = self.symbol_name.split(':')[0]
        if exchange == "NFO":
            if symbol_name.endswith("CE"): return "CE"
            if symbol_name.endswith("PE"): return "PE"
            if symbol_name.endswith("FUT"): return "FUT"
        if exchange == "NSE":
            return "STOCK"
        raise ValueError(f"Invalid symbol name or exchange: {symbol_name} {exchange}")

    def _get_position_for_symbol(self) -> int:
        """Get current position quantity for the trading symbol"""
        try:
            positions = self.broker.get_positions()
            # net_positions = positions['net'] # TODO: Remove this if not required
            for position in positions:
                logger.debug(f"Symbol: {position.symbol}")
                if position.symbol == self.symbol_name.split(':')[1]:
                    logger.info(f"Symbol: {position.symbol} | Current Position: {position.quantity_total}")
                    return position.quantity_total
                
            return 0
        except Exception as e:
            logger.error(f"Error getting position: {e}")
            return 0

    def _set_default_restrictions(self) -> Dict:
        """Set default trading restrictions (all allowed)"""
        # This function now just resets restrictions before dynamic calculation
        return {
            'nifty': {
                'futures': {'buy': 'yes', 'sell': 'yes'},
                'pe': {'buy': 'yes', 'sell': 'yes'},
                'ce': {'buy': 'yes', 'sell': 'yes'}
            },
            'bank_nifty': {
                'futures': {'buy': 'yes', 'sell': 'yes'},
                'pe': {'buy': 'yes', 'sell': 'yes'},
                'ce': {'buy': 'yes', 'sell': 'yes'}
            }
        }
    
    def _get_portfolio_greeks(self, index_name: str, verbose: bool = True) -> Dict:
        """
        Calculate detailed greeks for all positions of a given index (NIFTY or BANKNIFTY).
        
        Args:
            index_name (str): The index to calculate for ("NIFTY" or "BANKNIFTY").
            verbose (bool): If True, prints a detailed breakdown.
        
        Returns:
            Dict: A dictionary containing detailed greek values.
        """
        net_positions = self.broker.get_positions()
        
        # Initialize trackers
        total_delta = 0.0
        futures_delta = 0.0
        total_ce_delta = 0.0
        total_pe_delta = 0.0
        total_ce_qty = 0
        total_pe_qty = 0
        total_positive_ce = 0
        total_negative_ce = 0
        total_positive_pe = 0
        total_negative_pe = 0
        
        # Get the appropriate live price for the index spot
        if index_name == "NIFTY":
            quote_data = self.broker.get_quote("NSE:NIFTY 50")
            spot_price = quote_data.last_price
        elif index_name == "NIFTY BANK":
            quote_data = self.broker.get_quote("NSE:NIFTY BANK")
            spot_price = quote_data.last_price
        else:
            raise ValueError(f"Invalid index name: {index_name}")

        for pos in net_positions:
            if not pos.symbol.startswith(index_name):
                continue

            instrument = self.all_instruments[self.all_instruments["symbol"] == pos.symbol].to_dict(orient="records")[0]
            quantity = pos.quantity_total

            # --- Futures Delta Calculation ---
            if instrument['instrument_type'] == "FUT": 
                total_delta += quantity
                futures_delta += quantity
                continue
            
            # --- Options Delta Calculation ---
            if instrument['instrument_type'] in ("CE", "PE"):
                days_to_expiry = int(instrument['days_to_expiry'])
                if days_to_expiry < 0: continue 
                
                if self.delta_calculation_days is not None:
                    if days_to_expiry > self.delta_calculation_days:
                        continue
                
                bs = mibian.BS([spot_price, instrument['strike'], self.interest_rate, days_to_expiry], volatility=self.todays_volatility) 
                
                if instrument['instrument_type'] == "CE":
                    computed_delta = bs.callDelta * quantity
                    total_ce_delta += computed_delta
                    total_ce_qty += quantity
                    if quantity > 0:
                        total_positive_ce = total_positive_ce + abs(quantity)
                    else:
                        total_negative_ce = total_negative_ce + abs(quantity)
                elif instrument['instrument_type'] == "PE":
                    computed_delta = bs.putDelta * quantity
                    total_pe_delta += computed_delta
                    total_pe_qty += quantity
                    if quantity > 0:
                        total_positive_pe = total_positive_pe + abs(quantity)
                    else:
                        total_negative_pe = total_negative_pe + abs(quantity)
                else:
                    raise ValueError(f"Invalid instrument type: {instrument['instrument_type']}")
                
                total_delta += computed_delta

        if verbose:
            logger.info(f"--- {index_name} Delta Breakdown ---")
            logger.info(f"Spot Price: {spot_price}")
            logger.info(f"Total Call Quantity: {total_ce_qty}, Total Put Quantity: {total_pe_qty}")
            logger.info(f"Futures Delta: {futures_delta:.2f}")
            logger.info(f"Options Delta (CE): {total_ce_delta:.2f} | Qty: {total_ce_qty}")
            logger.info(f"Options Delta (PE): {total_pe_delta:.2f} | Qty: {total_pe_qty}")
            logger.info(f"Total Positive CE: {total_positive_ce}, Total Negative CE: {total_negative_ce}")
            logger.info(f"Total Positive PE: {total_positive_pe}, Total Negative PE: {total_negative_pe}")
            logger.info(f"----- FINAL {index_name} DELTA: {total_delta:.2f} -----")

        spread_count = abs(total_positive_ce+total_positive_pe)
        single_pe_ce = abs(total_pe_delta - total_ce_delta)
        both_ce_pe = abs(min(abs(total_ce_delta), abs(total_pe_delta)))
        # What does this variables inside below function mean?
        margin_requirement = (self.calculate_margin_requirement(spread_count, single_pe_ce, both_ce_pe))/75
        formatted_market_requirement = self.formatINR(margin_requirement)

        return {
            'delta': total_delta,
            'futures_delta': futures_delta,
            'ce_delta': total_ce_delta,
            'pe_delta': total_pe_delta,
            'ce_qty': total_ce_qty,
            'pe_qty': total_pe_qty,
            'positive_ce': total_positive_ce,
            'negative_ce': total_negative_ce,
            'positive_pe': total_positive_pe,
            'negative_pe': total_negative_pe,
            'margin_requirement': formatted_market_requirement
        }

    def calculate_margin_requirement(self, spread_count: int, single_pe_ce: int, both_ce_pe: int) -> float:
        """Calculate margin requirement for the portfolio"""
        return spread_count*self.margin_spread + single_pe_ce*self.margin_single_pe_ce + both_ce_pe*self.margin_both_pe_ce 

    def formatINR(self, number: float) -> str:
        """Format number as INR"""
        return f"₹{number:,.2f}"
    
    def _get_dynamic_restrictions(self) -> Dict:
        """Sets trading restrictions based on portfolio delta."""
        restrictions = self._set_default_restrictions()

        symbol = self.symbol_name.split(':')[1]

        if 'nifty' in symbol.lower() and 'bank' not in symbol.lower():
            symbol_class = "nifty"
        elif 'nifty' in symbol.lower() and 'bank' in symbol.lower():
            symbol_class = "banknifty"
        else:
            raise ValueError(f"Invalid symbol: {symbol}")

        # Nifty Delta Check
        if symbol_class == "nifty":
            nifty_restrictions = restrictions['nifty']
            nifty_greeks = self._get_portfolio_greeks("NIFTY")
            if nifty_greeks['delta'] < self.min_nifty_delta:
                nifty_restrictions['futures']['sell'] = "no"
                nifty_restrictions['ce']['sell'] = "no"
                nifty_restrictions['pe']['buy'] = "no"
                logger.warning("NIFTY delta below minimum. Restricting sell-side orders.")
            elif nifty_greeks['delta'] > self.max_nifty_delta:
                nifty_restrictions['futures']['buy'] = "no"
                nifty_restrictions['ce']['buy'] = "no"
                nifty_restrictions['pe']['sell'] = "no"
                logger.warning("NIFTY delta above maximum. Restricting buy-side orders.")
        elif symbol_class == "banknifty":
            bank_nifty_restrictions = restrictions['bank_nifty']
            # Bank Nifty Delta Check
            bank_nifty_greeks = self._get_portfolio_greeks("NIFTY BANK")
            if bank_nifty_greeks['delta'] < self.min_bank_nifty_delta:
                bank_nifty_restrictions['futures']['sell'] = "no"
                bank_nifty_restrictions['ce']['sell'] = "no"
                bank_nifty_restrictions['pe']['buy'] = "no"
                logger.warning("NIFTY BANK delta below minimum. Restricting sell-side orders.")
            elif bank_nifty_greeks['delta'] > self.max_bank_nifty_delta:
                bank_nifty_restrictions['futures']['buy'] = "no"
                bank_nifty_restrictions['ce']['buy'] = "no"
                bank_nifty_restrictions['pe']['sell'] = "no"
                logger.warning("NIFTY BANK delta above maximum. Restricting buy-side orders.")
        else:
            raise ValueError(f"Invalid symbol: {symbol} | {self.symbol_name}")
        return restrictions

    def _get_symbol_restrictions(self, symbol: str) -> Tuple[Dict, bool]:
        """Get trading restrictions for the given symbol."""
        all_restrictions = self._get_dynamic_restrictions()
        
        if symbol.startswith("NIFTY BANK") or symbol.startswith("BANKNIFTY"):
            return all_restrictions['bank_nifty'], False
        elif symbol.startswith("NIFTY"):
            return all_restrictions['nifty'], False
        else:
            # For non-NIFTY/NIFTY BANK symbols, allow all trades
            # return {}, True
            raise ValueError(f"Invalid symbol: {symbol}")

    def _get_scaled_gaps(self, current_diff_scale: float) -> Tuple[float, float]:
        """Calculate scaled gaps based on position imbalance"""
        diff_key = str(int(current_diff_scale))
        
        if diff_key not in self.multiplier_scale:
            mult = [100.0, 1.0] if current_diff_scale > 0 else [1.0, 100.0]
        else:
            mult = self.multiplier_scale[diff_key]
        
        return round(self.buy_gap * mult[0], 1), round(self.sell_gap * mult[1], 1)

    def _get_best_buy_sell_price(self, buy_price_1: float, buy_price_2: float, 
                                sell_price_1: float, sell_price_2: float) -> Dict[str, float]:
        """Get the best prices for buy (lower) and sell (higher) orders"""
        return {'buy': min(buy_price_1, buy_price_2), 'sell': max(sell_price_1, sell_price_2)}

    def _prepare_final_prices(self, scaled_buy_gap: float, scaled_sell_gap: float) -> Dict[str, float]:
        """Prepare final order prices with cool-off period"""
        price = self.broker.get_quote(self.symbol_name).last_price
        self.prev_quote_price = price
        best_prices = self._get_best_buy_sell_price(
            price - scaled_buy_gap, self.scraper_last_price - scaled_buy_gap,
            price + scaled_sell_gap, self.scraper_last_price + scaled_sell_gap
        )
        time.sleep(self.cool_off_time)
        price_after_wait = self.broker.get_quote(self.symbol_name).last_price
        return self._get_best_buy_sell_price(
            best_prices['buy'], price_after_wait - scaled_buy_gap,
            best_prices['sell'], price_after_wait + scaled_sell_gap
        )
    

    def _execute_orders(self, symbol: str, final_buy_price: float, final_sell_price: float,
                       restrict_buy_order: int, restrict_sell_order: int) -> None:
        """Execute buy and sell orders based on restrictions"""
        sell_order_id = -1
        logger.info(f"Executing orders for {symbol} | Restrictions - Buy: {restrict_buy_order}, Sell: {restrict_sell_order}")
        if restrict_sell_order == 0:
            req = OrderRequest(
                symbol=symbol, exchange=Exchange.NFO, transaction_type=TransactionType.SELL,
                quantity=self.sell_quantity, product_type=ProductType.MARGIN, order_type=OrderType.LIMIT,
                price=final_sell_price, tag=self.tag
            ) # TODO: Variety Supported is only Regular for now
            logger.info(f"Placing sell order with request: {req}")
            sell_order_resp = self.broker.place_order(req)

            logger.info("Sell Order Response - {}".format(sell_order_resp))

            sell_order_id = sell_order_resp.order_id if sell_order_resp and hasattr(sell_order_resp, 'order_id') else None
            if sell_order_id:
                if sell_order_id not in self.orders:
                    self.handle_order_update_call_tracker[sell_order_id] = False
                logger.info(f"Placed SELL order {sell_order_id} for {self.sell_quantity} @ {final_sell_price}")
                self.add_order_to_list(sell_order_id, final_sell_price, self.sell_quantity, "SELL", symbol, -1)
                logger.info(f"handle_order_update_call_tracker: {self.handle_order_update_call_tracker}")
                if not self.handle_order_update_call_tracker.get(sell_order_id):
                    if sell_order_id in self.handle_order_update_call_tracker_response_dict:
                        self.handle_order_update(self.handle_order_update_call_tracker_response_dict[sell_order_id])
            else:
                logger.error("Sell order placement failed, no order_id received.")
                sell_order_id = -1

        # only when the sell order has been placed or sell order was restricred and buy order was not restricted
        if (restrict_sell_order == 1 or sell_order_id != -1) and restrict_buy_order == 0:
            req = OrderRequest(
                symbol=symbol, exchange=Exchange.NFO, transaction_type=TransactionType.BUY,
                quantity=self.buy_quantity, product_type=ProductType.MARGIN, order_type=OrderType.LIMIT,
                price=final_buy_price, tag=self.tag
            ) # TODO: Variety Supported is only Regular for now
            logger.info(f"Placing buy order with request: {req}")
            buy_order_resp = self.broker.place_order(req)
            logger.info("Buy Order Response - {}".format(buy_order_resp))
            buy_order_id = buy_order_resp.order_id if buy_order_resp and hasattr(buy_order_resp, 'order_id') else None
            if buy_order_id:
                self.handle_order_update_call_tracker[buy_order_id] = False
                logger.info(f"Placed BUY order {buy_order_id} for {self.buy_quantity} @ {final_buy_price}")
                if sell_order_id != -1:
                    if sell_order_id in self.orders:
                        self.add_order_to_list(sell_order_id, final_sell_price, self.sell_quantity, "SELL", symbol, buy_order_id)
                self.add_order_to_list(buy_order_id, final_buy_price, self.buy_quantity, "BUY", symbol, sell_order_id)
                logger.info(f"handle_order_update_call_tracker: {self.handle_order_update_call_tracker}")
                if not self.handle_order_update_call_tracker.get(buy_order_id):
                    if buy_order_id in self.handle_order_update_call_tracker_response_dict:
                        self.handle_order_update(self.handle_order_update_call_tracker_response_dict[buy_order_id])
            else:
                logger.warning(f"Buy order failed, cancelling associated sell order {sell_order_id}")
                if sell_order_id != -1:
                    self._remove_order(sell_order_id)
                    if sell_order_id in self.handle_order_update_call_tracker:
                        del self.handle_order_update_call_tracker[sell_order_id]
                    if sell_order_id in self.handle_order_update_call_tracker_response_dict:
                        del self.handle_order_update_call_tracker_response_dict[sell_order_id]

    def add_order_to_list(self, order_id, price, quantity, transaction_type, symbol, associated_order_id):
        now = datetime.datetime.now()
        
        # Create order details for OrderTracker
        order_details = {
            'order_id': order_id,
            'price': price,
            'quantity': quantity,
            'transaction_type': transaction_type,
            'symbol': symbol,
            'associated_order': associated_order_id,
            'hour': now.hour,
            'min': now.minute,
            'second': now.second,
            'time': f"{now.hour}:{now.minute}:{now.second}",
            'timestamp': now.isoformat()
        }
        
        # Add to OrderTracker
        self.order_tracker.add_order(order_details)
        
        # Keep local reference for backward compatibility
        self.orders[order_id] = order_details
        logger.info("Current Orders List: {}".format(self.orders))
        self.print_current_status()

    def place_wave_order(self) -> None:
        """Main function to execute wave trading strategy"""
        if self.already_executing_order > 0:
            logger.info("Order execution already in progress.")
            return
        
        self.already_executing_order = 1
        symbol = self.symbol_name.split(':')[1]

        if 'nifty' in symbol.lower() and 'bank' not in symbol.lower():
            symbol_class = "nifty"
        elif 'nifty' in symbol.lower() and 'bank' in symbol.lower():
            symbol_class = "banknifty"
        else:
            raise ValueError(f"Invalid symbol: {symbol}")
        
        try:
            logger.info("--- Starting New Wave Order Cycle ---")
            
            symbol_restrictions, _ = self._get_symbol_restrictions(symbol)
            restrict_buy_order, restrict_sell_order = 0, 0
            
            current_net = self._get_position_for_symbol()
            # If new current_diff_scale > 0 it is more BUY, If it is < 0 it is SELL which has happened.
            # TODO - TO BE FIXED BASED on the number of buy order and sell orders
            # if a buy happens, next buy should happen with a factor of 1.3 , 
            # if a buy and sell happens, ntext ubuy or sell can happen with a factor of 1
            # Quantity should be removed and not used
            # current_diff_scale = (current_net - self.initial_positions['position']) / self.quantity if self.quantity != 0 else 0 # TODO: Check this with Vibhu - We are setting quantity to sell_quantity in initilise function
            current_diff_scale = self.get_current_position_difference()
            symbol_type = self._get_symbol_type(symbol)
            
            if (symbol_type == "ce" or symbol_type == "pe") and current_net == 0:
                logger.warning("No position, Order pushing for positive buy, will be allowed only once.")
            
            elif symbol_type in ("ce", "pe") and current_net > 0:
                restrict_buy_order = 1
                logger.info("Option already long, restricting further buys.")

            if symbol_restrictions.get(symbol_type, {}).get("buy") == "no":
                restrict_buy_order = 1
            if symbol_restrictions.get(symbol_type, {}).get("sell") == "no":
                restrict_sell_order = 1

            logger.info(f"Restrictions - Buy: {restrict_buy_order}, Sell: {restrict_sell_order}")
            
            scaled_buy_gap, scaled_sell_gap = self._get_scaled_gaps(current_diff_scale)
            logger.info(f"Position Imbalance: {current_diff_scale:.2f} -> Scaled Gaps | Buy: {scaled_buy_gap}, Sell: {scaled_sell_gap}")

            final_prices = self._prepare_final_prices(scaled_buy_gap, scaled_sell_gap)
            logger.info(f"Final Prices -> Buy: {final_prices['buy']:.2f}, Sell: {final_prices['sell']:.2f}")


            second_buy_price = None
            second_sell_price = None

            if self.prev_wave_buy_price is not None: 
                second_buy_price = self.prev_wave_buy_price
            else:
                second_buy_price = final_prices['buy']



            logger.info(f"Last Wave Values {self.prev_wave_buy_price}, Sell: {self.prev_wave_sell_price}")

            if self.prev_wave_sell_price is not None:
                second_sell_price = self.prev_wave_sell_price
            else:
                second_sell_price = final_prices['sell']

            final_prices = self._get_best_buy_sell_price(
                final_prices['buy'],  second_buy_price,
                final_prices['sell'], second_sell_price
            )

            # Special case to treat for NIFTY -  If the product is NIFTY and buy price is less than 25, 
            # the buy restrict order is not considered and the buy order is placed. 
            # This is done to save on the margins by closing the position.
            if symbol_class == "nifty" and restrict_buy_order == 1 and final_prices['buy'] < 25:
                restrict_buy_order = 0
                logger.warning("NIFTY buy price is less than 25, not restricting buy order.")
                logger.warning(f"Updated Restrictions- Buy: {restrict_buy_order}, Sell: {restrict_sell_order}")

            self._execute_orders(symbol, final_prices['buy'], final_prices['sell'], restrict_buy_order, restrict_sell_order)


            self.prev_wave_buy_price = final_prices['buy']
            self.prev_wave_sell_price = final_prices['sell']
            
            logger.info(f"Previous Wave Prices -> Buy: {self.prev_wave_buy_price}, Sell: {self.prev_wave_sell_price}")

            logger.info("--- End of Wave Order Cycle ---")
            logger.info("Current Orders List: {}".format(self.orders))
            time.sleep(3)
        except Exception as e:
            logger.error(f"Error in wave order execution: {e}", exc_info=True)
        finally:
            self.already_executing_order = 0
            
    def check_and_enforce_restrictions_on_active_orders(self):
        """Checks if active orders violate new delta restrictions and cancels them."""
        if self.already_executing_order > 0:
            logger.info("Order execution already in progress.")
            return
        
        logger.info(f"Current Wave Prices -> Buy: {self.prev_wave_buy_price}, Sell: {self.prev_wave_sell_price}")

        logger.info("Checking active orders against new delta restrictions...")
        symbol = self.symbol_name.split(':')[1]
        symbol_type = self._get_symbol_type(symbol)

        if 'nifty' in symbol.lower() and 'bank' not in symbol.lower():
            symbol_class = "nifty"
        elif 'nifty' in symbol.lower() and 'bank' in symbol.lower():
            symbol_class = "banknifty"
        else:
            raise ValueError(f"Invalid symbol: {symbol}")
        
        # Get the latest restrictions
        restrictions, _ = self._get_symbol_restrictions(symbol)
        restrict_buy = restrictions.get(symbol_type, {}).get("buy") == "no"
        restrict_sell = restrictions.get(symbol_type, {}).get("sell") == "no"
        
        if not (restrict_buy or restrict_sell):
            logger.info("No new restrictions apply. Active orders are safe.")
            return

        try:
            # Some State variables to track
            sell_order_present = False
            buy_order_present = False
            sell_price = -1 # TODO: Check if this is required
            buy_price = -1 # TODO: Check if this is required
            tag = "" # TODO: Check if this is required

            # Use list(self.orders.items()) to avoid runtime errors during deletion
            for order_id, order_info in list(self.orders.items()):
                if order_id == -1:
                    continue
                
                # Special case to treat for NIFTY -  If the product is NIFTY and buy price is less than 25, 
                # the buy restrict order is not considered and the buy order is placed. 
                # This is done to save on the margins by closing the position.
                if symbol_class == "nifty" and restrict_buy and order_info['type'] == 'BUY' and order_info['price'] < 25:
                    continue

                should_cancel = False
                if restrict_buy and order_info['type'] == 'BUY':
                    should_cancel = True
                if restrict_sell and order_info['type'] == 'SELL':
                    should_cancel = True

                if should_cancel:
                    logger.warning(f"Restriction violation! Cancelling {order_info['type']} order {order_id}.")
                    try:
                        # self.broker.cancel_order(order_id=order_id)
                        # Also cancel the associated order - Not Needed
                        assoc_order_id = order_info.get('associated_order')
                        if assoc_order_id and assoc_order_id in self.orders:
                            # logger.warning(f"Cancelling associated order {assoc_order_id}.") # TODO: If this required ?
                            
                            self.orders[order_id]['associated_order'] = -1
                            self.orders[assoc_order_id]['associated_order'] = -1
                            # self._remove_order(assoc_order_id)
                            # self.broker.cancel_order(order_id=order_id)
                        self._remove_order(order_id)
                        continue
                    except Exception as e:
                        logger.error(f"Failed to cancel order {order_id}: {e}")

                if order_info['type'] == 'SELL':
                    sell_order_present = True
                    sell_price = order_info['price'] # TODO: Check if this is required
                    sell_order_id = order_id
                if order_info['type'] == 'BUY':
                    buy_order_present = True
                    buy_price = order_info['price'] # TODO: Check if this is required
                    buy_order_id = order_id

            
            if not restrict_sell and not sell_order_present:
                logger.warning("Sell order is missing.")
                if self.prev_wave_sell_price is not None:
                    final_sell_price = self.prev_wave_sell_price
                    final_sell_qty = self.prev_wave_sell_qty

                    if self.already_executing_order == 0:
                        sell_order_resp = self.broker.place_order(
                            symbol=symbol, exchange=Exchange.NFO, transaction_type=TransactionType.SELL,
                            quantity=final_sell_qty, product_type=ProductType.MARGIN, order_type=OrderType.LIMIT,
                            price=final_sell_price, tag=self.tag
                        )
                        sell_order_id = sell_order_resp.order_id
                        self.handle_order_update_call_tracker[sell_order_id] = False
                        if buy_order_present:
                            self.add_order_to_list(sell_order_id, final_sell_price, final_sell_qty, "SELL", symbol, buy_order_id)
                            self.orders[buy_order_id]['associated_order'] = sell_order_id
                            if not self.handle_order_update_call_tracker[sell_order_id]:
                                self.handle_order_update(self.handle_order_update_call_tracker_response_dict[sell_order_id])
                        else:
                            self.add_order_to_list(sell_order_id, final_sell_price, final_sell_qty, "SELL", symbol, -1)
                            if not self.handle_order_update_call_tracker[sell_order_id]:
                                self.handle_order_update(self.handle_order_update_call_tracker_response_dict[sell_order_id])
                else:
                    logger.critical("No previous wave sell price found. This should not happen.")

            if not restrict_buy and not buy_order_present:
                logger.warning("Buy order is missing.")
                if self.prev_wave_buy_price is not None:
                    final_buy_price = self.prev_wave_buy_price
                    final_quantity = self.prev_wave_buy_qty

                    if self.already_executing_order == 0:
                        buy_order_resp = self.broker.place_order(
                            symbol=symbol, exchange=Exchange.NFO, transaction_type=TransactionType.BUY,
                            quantity=final_quantity, product_type=ProductType.MARGIN, order_type=OrderType.LIMIT,
                            price=final_buy_price, tag=self.tag
                        )
                        buy_order_id = buy_order_resp.order_id  
                        self.handle_order_update_call_tracker[buy_order_id] = False
                        if sell_order_present:
                            self.add_order_to_list(buy_order_id, final_buy_price, final_quantity, "BUY", symbol, sell_order_id)
                            self.orders[sell_order_id]['associated_order'] = buy_order_id
                            if not self.handle_order_update_call_tracker[buy_order_id]:
                                self.handle_order_update(self.handle_order_update_call_tracker_response_dict[buy_order_id])
                        else:
                            self.add_order_to_list(buy_order_id, final_buy_price, self.final_quantity, "BUY", symbol, -1)
                            if not self.handle_order_update_call_tracker[buy_order_id]:
                                self.handle_order_update(self.handle_order_update_call_tracker_response_dict[buy_order_id])
                else:
                    logger.critical("No previous wave buy price found. This should not happen.")
                
        except Exception as e:
            logger.error(f"Error in check_and_enforce_restrictions_on_active_orders: {e}", exc_info=True)
        
    def check_is_any_order_active(self) -> bool:
        logger.info("Current Order List - {}".format(self.orders))
        for order_id, order_info in self.orders.items():
            if order_id == -1:
                continue
            else:
                return True
        return False

    def print_current_status(self) -> None:
        """Print current system status"""
        current_position = self._get_position_for_symbol()
        
        # Use OrderTracker for status if available
        if self.order_tracker:
            additional_info = {
                'symbol': self.symbol_name,
                'initial_position': self.initial_positions['position'],
                'current_position': current_position
            }
            self.order_tracker.print_status(additional_info)
        else:
            logger.info("="*50)
            logger.info(f"STATUS as of {time.ctime()}")
            logger.info(f"Symbol: {self.symbol_name}, Initial Position: {self.initial_positions['position']}, Current Position: {current_position}")
            logger.info(f"Active Orders Tracked: {len(self.orders)}")
            if self.orders:
                logger.info(f"Tracked Orders: {self.orders}")
            logger.info("="*50)

    def get_current_position_difference(self) -> float:
        """
        Returns the current position difference based on the self.orders
        """
        current_position_difference = 0
        for order_id, order_info in self.orders.items():
            if order_id == -1:
                continue
            if order_info['type'] == 'BUY':
                current_position_difference += order_info['quantity']
            elif order_info['type'] == 'SELL':
                current_position_difference -= order_info['quantity']
        return current_position_difference / self.lot_size
    
    def _complete_order(self, order_id: str):
        """
        Completes an order.
        """
        self.order_tracker.complete_order(order_id)
        order_info = self.orders[order_id]
        self.scraper_last_price = order_info['price']
        logger.info(f"Updated Scraper Last Price: {self.scraper_last_price}")
                        
        # Handle associated order cancellation
        associated_order_id = order_info.get('associated_order')
        if associated_order_id:
            try:
                self.broker.cancel_order(order_id=associated_order_id)
                logger.info(f"Cancelled associated order {associated_order_id}")
            except Exception as e:
                logger.error(f"Error cancelling associated order {associated_order_id}: {e}")

        # Once an order is completed - remove that order from tracking
        del self.orders[order_id]
        self.order_tracker.remove_order(order_id)
        
        self.place_wave_order()
        self.print_current_status()

    def _remove_order(self, order_id: str):
        """
        Removes an order from the orders list.
        """
        if order_id in self.orders:
            logger.info(f"Removing order {order_id} from orders list | Order Info: {self.orders[order_id]}")
            try:
                self.broker.cancel_order(order_id=order_id)
                logger.info(f"Cancelling order {order_id}")
            except Exception as e:
                logger.error(f"Error cancelling order {order_id}: {e}")
            del self.orders[order_id]
            self.order_tracker.remove_order(order_id)
        else:
            logger.warning(f"Order {order_id} not found in orders list")

        self.print_current_status()

    def _cancel_order(self, order_id: str):
        """
        Cancels an order.
        """
        self.order_tracker.cancel_order(order_id)

    def handle_order_update(self, order_data: dict):
        """
        Handles order updates from websocket callbacks.
        
        Args:
            order_data (dict): Order update data from broker
            broker: Broker instance for order cancellation
            strategy_callback: Optional callback function for strategy-specific actions
        """
        order_id = order_data.get('order_id', order_data.get('orders', {}).get('id', None))
        symbol = order_data.get('tradingsymbol', order_data.get('orders', {}).get('symbol', None))
        tag = order_data.get('orderTag', order_data.get('orders', {}).get('orderTag', order_data.get('tag', None)))
        status = order_data.get('status', order_data.get('orders', {}).get('status', 'N/A'))

        if ":" in symbol:
            symbol = symbol.split(":")[1]
        if symbol not in self.symbol_name:
            logger.info(f"Order update received for symbol {symbol} but current symbol is {self.symbol_name.split(':')[1]}")
            return

        if tag and self.tag not in tag:
            logger.info(f"Order update received for tag {tag} but current tag is {self.tag}")
            return

        if not order_id:
            logger.warning("Order update received without order_id")
            return
            
        logger.info(f"Order Update: {order_data}")
        # print("self.orders -> ", self.orders)
        if order_id not in self.orders:
            logger.info(f"Order {order_id} not found in orders list")
            # Saving this for calling later
            # if order_id not in self.handle_order_update_call_tracker:
            self.handle_order_update_call_tracker[order_id] = False
            self.handle_order_update_call_tracker_response_dict[order_id] = order_data
            logger.info(f"Saved most recent order update for order {order_id} for later call")

        if order_id in self.orders:
            self.handle_order_update_call_tracker[order_id] = True
            order_info = self.orders[order_id]
            status = order_data.get('status', order_data.get('orders', {}).get('status', 'N/A'))
            associated_order_id = order_info.get('associated_order', order_info.get('orders', {}).get('associated_order', 'N/A'))


            logger.info(f"associated order id = {associated_order_id} --- ")
            
            if status == 'COMPLETE' or status == 2: # TODO: Check this - this is for fyers and zerodha
                logger.info(f"Order {order_id} executed successfully")
                self._complete_order(order_id)
                self.order_tracker.record_order_complete(order_id, order_info['transaction_type'])
                self.prev_wave_buy_price = None
                self.prev_wave_sell_price = None
                
                    
            elif status == 'CANCELLED' or status == 1:
                logger.info(f"Order {order_id} was cancelled")
                # Cancel the associated order if the main order is cancelled anda if it exists
                if associated_order_id != -1:
                    self._remove_order(associated_order_id)
                self._remove_order(order_id)

            elif (status == 'OPEN' or status == 'UPDATE') or status == 6: #TODO Vibhu - Need to check update status for Fryers
                # Update order details
                if 'price' in order_data:
                    logger.info(f"There is price in Order_Info = {order_data} - "+str(order_data.get('price')))
                    self.orders[order_id]['price'] = order_data.get('price', order_data.get('orders', {}).get('limitPrice', 'N/A'))
                if 'quantity' in order_data:
                    self.orders[order_id]['quantity'] = order_data.get('quantity', order_data.get('orders', {}).get('qty', 'N/A'))
                logger.info(f"Order {order_id} updated: {self.orders[order_id]}")

                side = order_data.get('transaction_type', order_data.get('orders', {}).get('side', 'N/A'))
                if side == 'BUY':
                    self.prev_wave_buy_price = order_data.get('price', order_data.get('orders', {}).get('limitPrice', 'N/A'))
                    self.prev_wave_buy_qty = order_data.get('quantity', order_data.get('orders', {}).get('quantity', 'N/A')) #TODO Vibhu - here the parameter has to be checked.
                    logger.info(f"Updated Previous Wave Buy Price: {self.prev_wave_buy_price}")
        
                if side == 'SELL':
                    self.prev_wave_sell_price = order_data.get('price', order_data.get('orders', {}).get('limitPrice', 'N/A'))
                    self.prev_wave_sell_qty = order_data.get('quantity', order_data.get('orders', {}).get('quantity', 'N/A')) #TODO - here the parameter has to be checked.
                    logger.info(f"Updated Previous Wave Sell Price: {self.prev_wave_sell_price}")
             
            elif status == 'REJECTED' or status == 5:
                logger.warning(f"Order {order_id} was rejected, Reason - {order_data.get('status_message', order_data.get('orders', {}).get('message', 'N/A'))}")
                # Cancel the associated order if the main order is cancelled anda if it exists
                if associated_order_id != -1:
                    self._remove_order(associated_order_id)
                self._remove_order(order_id)
            else:
                logger.info(f"Order {order_id} status: {status} | NOT HANDLED")


               
        else:
            logger.warning(f"Unknown order update: {order_data.get('transaction_type', order_data.get('orders', {}).get('side', 'N/A'))} -- "
                        f"{order_data.get('tradingsymbol', order_data.get('orders', {}).get('symbol', 'N/A'))} -- {order_data.get('price', order_data.get('orders', {}).get('limitPrice', 'N/A'))}")
            


# Below Logic is for
# 1. command line arguments and 
# 2. run the strategy in a loop

# =============================================================================
# MAIN SCRIPT EXECUTION
# =============================================================================
# 
# This section provides a complete command-line interface for running the
# Wave Trading Strategy with flexible configuration options.
#
# FEATURES:
# =========
# 1. **Configuration Management**: 
#    - Loads defaults from YAML file
#    - Supports command-line overrides
#    - Validates all parameters
#
# 2. **Argument Parsing**:
#    - Comprehensive help and examples
#    - Type validation and choices
#    - Hierarchical configuration (CLI > YAML > defaults)
#
# 3. **Trading Loop**:
#    - Real-time websocket data processing
#    - Strategy execution on each tick
#    - Error handling and recovery
#    - Order tracking and management
#
# USAGE EXAMPLES:
# ==============
# 
# # Basic usage with defaults
# python wave.py
# 
# # Override specific parameters
# python wave.py --symbol-name NIFTY25SEPFUT --buy-gap 25 --sell-gap 25
# 
# # Full customization
# python wave.py \
#     --symbol-name NIFTY25SEPFUT \
#     --buy-gap 25 --sell-gap 25 \
#     --buy-quantity 75 --sell-quantity 75 --cool-off-time 1
#
# =============================================================================

if __name__ == "__main__":
    import time
    import yaml
    import sys
    import argparse
    from dispatcher import DataDispatcher
    from orders import OrderTracker
    from strategy.wave import WaveStrategy
    # from brokers.zerodha import ZerodhaBroker
    from logger import logger
    from queue import Queue
    import random
    import traceback
    import warnings
    
    # ==========================================================================
    # SECTION 1: CONFIGURATION LOADING AND PARSING
    # ==========================================================================
    
    # Load default configuration from YAML file
    config_file = os.path.join(os.path.dirname(__file__), "configs/wave.yml")
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)['default']

    def create_argument_parser():
        """Create and configure argument parser with detailed help"""
        parser = argparse.ArgumentParser(
            description="Wave Trading Strategy",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
    Examples:
    # Use default configuration from wave.yml
    python wave.py
    
    # Override specific parameters
    python wave.py --symbol-name NIFTY25SEPFUT --buy-gap 25 --sell-gap 25
    
    # Full parameter override
    python wave.py \\
        --symbol-name NIFTY25SEPFUT \\
        --buy-gap 25 --sell-gap 25 \\
        --buy-quantity 75 --sell-quantity 75 \\
        --cool-off-time 1 \\
        --product-type NRML --tag WAVE_SCRAPER

CONFIGURATION HIERARCHY:
=======================
1. Command line arguments (highest priority)
2. wave.yml default values (fallback)

PARAMETER GROUPS:
================
• Core Parameters: symbol-name, exchange
• Gap Parameters: buy-gap, sell-gap
• Order Management: buy-quantity, sell-quantity, product-type, tag
• Risk Management: min-nifty-delta, max-nifty-delta, min-bank-nifty-delta, max-bank-nifty-delta
• Greeks Calculation: interest-rate, todays-volatility, delta-calculation-days
• Margin Parameters: margin-spread, margin-single-pe-ce, margin-both-pe-ce
            """
        )
        
        # =======================================================================
        # CORE TRADING PARAMETERS
        # =======================================================================
        
        parser.add_argument('--symbol-name', type=str,
                        help='Trading symbol (e.g., NIFTY25SEPFUT, BANKNIFTY25SEPFUT). '
                             'This identifies the specific futures contract to trade.')
        
        parser.add_argument('--exchange', type=str, choices=['NFO'],
                        help='Exchange for trading (NFO for F&O)')
        
        # =======================================================================
        # GAP PARAMETERS
        # =======================================================================
        
        parser.add_argument('--buy-gap', type=float,
                        help='Price gap below current price for buy orders. '
                             'E.g., if buy-gap is 25 and current price is 24500, '
                             'buy orders will be placed at 24475.')
        
        parser.add_argument('--sell-gap', type=float,
                        help='Price gap above current price for sell orders. '
                             'E.g., if sell-gap is 25 and current price is 24500, '
                             'sell orders will be placed at 24525.')
        
        # =======================================================================
        # ORDER MANAGEMENT PARAMETERS
        # =======================================================================
        
        parser.add_argument('--buy-quantity', type=int,
                        help='Quantity to trade for buy orders.')
        
        parser.add_argument('--sell-quantity', type=int,
                        help='Quantity to trade for sell orders.')
        
        parser.add_argument('--cool-off-time', type=int,
                        help='Cool-off time in seconds between price checks.')
        
        parser.add_argument('--product-type', type=str, choices=['NRML', 'MIS'],
                        help='Product type for orders (NRML for normal, MIS for intraday)')
        
        parser.add_argument('--order-type', type=str, choices=['MARKET', 'LIMIT'],
                        help='Order type for placing trades (MARKET or LIMIT)')
        
        parser.add_argument('--variety', type=str, choices=['REGULAR', 'AMO', 'CO', 'ICEBERG'],
                        help='Order variety (REGULAR, AMO, CO, ICEBERG)')
        
        parser.add_argument('--tag', type=str,
                        help='Tag for identifying orders in broker interface.')
        
        # =======================================================================
        # RISK MANAGEMENT PARAMETERS
        # =======================================================================
        
        parser.add_argument('--min-nifty-delta', type=float,
                        help='Minimum allowed NIFTY portfolio delta.')
        
        parser.add_argument('--max-nifty-delta', type=float,
                        help='Maximum allowed NIFTY portfolio delta.')
        
        parser.add_argument('--min-bank-nifty-delta', type=float,
                        help='Minimum allowed BANKNIFTY portfolio delta.')
        
        parser.add_argument('--max-bank-nifty-delta', type=float,
                        help='Maximum allowed BANKNIFTY portfolio delta.')
        
        # =======================================================================
        # GREEKS CALCULATION PARAMETERS
        # =======================================================================
        
        parser.add_argument('--interest-rate', type=float,
                        help='Interest rate for options pricing calculations.')
        
        parser.add_argument('--todays-volatility', type=float,
                        help='Today\'s volatility for options pricing calculations.')
        
        parser.add_argument('--delta-calculation-days', type=int,
                        help='Number of days to expiry for delta calculations.')
        
        # =======================================================================
        # MARGIN PARAMETERS
        # =======================================================================
        
        parser.add_argument('--margin-spread', type=float,
                        help='Margin requirement for spread positions.')
        
        parser.add_argument('--margin-single-pe-ce', type=float,
                        help='Margin requirement for single PE/CE positions.')
        
        parser.add_argument('--margin-both-pe-ce', type=float,
                        help='Margin requirement for both PE/CE positions.')
        
        # =======================================================================
        # UTILITY OPTIONS
        # =======================================================================
        
        parser.add_argument('--show-config', action='store_true',
                        help='Display current configuration (after applying overrides) and exit. '
                             'Useful for verifying parameter values before trading.')
        
        parser.add_argument('--config-file', type=str, default=config_file,
                        help='Path to YAML configuration file containing default values. '
                             'Defaults to strategy/configs/wave.yml')
        
        parser.add_argument('--env-file', type=str, default='.env', help='Path to the .env file')

        return parser

    def show_config(config):
        """
        Display current configuration in organized format
        
        Args:
            config (dict): Configuration dictionary to display
            
        """
        print("\n" + "="*80)
        print("WAVE TRADING STRATEGY CONFIGURATION")
        print("="*80)
        
        # Group parameters by functionality for better readability
        sections = {
            "Core Trading Parameters": [
                'symbol_name', 'exchange'
            ],
            "Gap Parameters": [
                'buy_gap', 'sell_gap'
            ],
            "Order Management": [
                'buy_quantity', 'sell_quantity', 'cool_off_time', 'product_type', 'order_type', 'variety', 'tag'
            ],
            "Risk Management (Delta Limits)": [
                'min_nifty_delta', 'max_nifty_delta', 'min_bank_nifty_delta', 'max_bank_nifty_delta'
            ],
            "Greeks Calculation": [
                'interest_rate', 'todays_volatility', 'delta_calculation_days'
            ],
            "Margin Requirements": [
                'margin_spread', 'margin_single_pe_ce', 'margin_both_pe_ce'
            ]
        }
        
        for section, fields in sections.items():
            print(f"\n{section}:")
            print("-" * len(section))
            for field in fields:
                value = config.get(field, 'NOT SET')
                # Add units/context for clarity
                unit_context = {
                    'buy_gap': 'points',
                    'sell_gap': 'points',
                    'buy_quantity': 'units',
                    'sell_quantity': 'units',
                    'cool_off_time': 'seconds',
                    'interest_rate': '%',
                    'todays_volatility': '%',
                    'delta_calculation_days': 'days',
                    'margin_spread': '₹',
                    'margin_single_pe_ce': '₹',
                    'margin_both_pe_ce': '₹'
                }
                unit = unit_context.get(field, '')
                print(f"  {field:25}: {value} {unit}".strip())
        
        print("\n" + "="*80)
        print("TRADING LOGIC SUMMARY:")
        print("="*80)
        print(f"• Trading Symbol: {config.get('symbol_name', 'N/A')}")
        print(f"• Buy Gap: {config.get('buy_gap', 'N/A')} points below current price")
        print(f"• Sell Gap: {config.get('sell_gap', 'N/A')} points above current price")
        print(f"• Buy Quantity: {config.get('buy_quantity', 'N/A')} units")
        print(f"• Sell Quantity: {config.get('sell_quantity', 'N/A')} units")
        print(f"• Cool-off time: {config.get('cool_off_time', 'N/A')} seconds")
        print(f"• NIFTY Delta limits: {config.get('min_nifty_delta', 'N/A')} to {config.get('max_nifty_delta', 'N/A')}")
        print(f"• BANKNIFTY Delta limits: {config.get('min_bank_nifty_delta', 'N/A')} to {config.get('max_bank_nifty_delta', 'N/A')}")
        print("="*80)

    # ==========================================================================
    # SECTION 2: ARGUMENT PARSING AND CONFIGURATION MERGING
    # ==========================================================================
    
    # Parse command line arguments
    parser = create_argument_parser()
    args = parser.parse_args()

    # Define mapping between argument names and configuration keys
    arg_to_config_mapping = {
        'symbol_name': 'symbol_name',
        'exchange': 'exchange',
        'buy_gap': 'buy_gap',
        'sell_gap': 'sell_gap',
        'buy_quantity': 'buy_quantity',
        'sell_quantity': 'sell_quantity',
        'cool_off_time': 'cool_off_time',
        'product_type': 'product_type',
        'order_type': 'order_type',
        'variety': 'variety',
        'tag': 'tag',
        'min_nifty_delta': 'min_nifty_delta',
        'max_nifty_delta': 'max_nifty_delta',
        'min_bank_nifty_delta': 'min_bank_nifty_delta',
        'max_bank_nifty_delta': 'max_bank_nifty_delta',
        'interest_rate': 'interest_rate',
        'todays_volatility': 'todays_volatility',
        'delta_calculation_days': 'delta_calculation_days',
        'margin_spread': 'margin_spread',
        'margin_single_pe_ce': 'margin_single_pe_ce',
        'margin_both_pe_ce': 'margin_both_pe_ce'
    }

    # Apply command line overrides to configuration
    overridden_params = []
    for arg_name, config_key in arg_to_config_mapping.items():
        # Convert dashes to underscores for argument attribute access
        arg_value = getattr(args, arg_name.replace('-', '_'))
        if arg_value is not None:
            config[config_key] = arg_value
            overridden_params.append(f"{config_key}={arg_value}")

    # Handle utility options
    if args.show_config:
        show_config(config)
        sys.exit(0)

    # ==========================================================================
    # SECTION 3: CONFIGURATION VALIDATION AND LOGGING
    # ==========================================================================
    
    # Validate that user has updated default configuration values
    def validate_configuration(config):
        """
        Validate that user has updated at least some default configuration values
        Returns True if config is valid, False otherwise
        """
        # Define default values that indicate user hasn't updated config
        default_values = {
            'symbol_name': 'NIFTY25SEPFUT',  # This is a placeholder value
            'buy_gap': 25,
            'sell_gap': 25,
            'buy_quantity': 75,
            'sell_quantity': 75,
            'cool_off_time': 1,
            'order_type': 'LIMIT',
            'variety': 'REGULAR',
            'min_nifty_delta': -100,
            'max_nifty_delta': 100,
            'min_bank_nifty_delta': -100,
            'max_bank_nifty_delta': 100,
            'interest_rate': 10,
            'todays_volatility': 20,
            'delta_calculation_days': 10,
            'margin_spread': 100,
            'margin_single_pe_ce': 100,
            'margin_both_pe_ce': 100
        }
        
        # Check which values are still at defaults
        unchanged_values = []
        changed_values = []
        for key, default_value in default_values.items():
            if config.get(key) == default_value:
                unchanged_values.append(key)
            else:
                changed_values.append(key)
        
        # If ALL values are still at defaults, show error and exit
        if len(changed_values) == 0:
            print("\n" + "="*80)
            print("❌ CONFIGURATION VALIDATION FAILED")
            print("="*80)
            print("ALL configuration values are still at their defaults!")
            print("You must update at least some parameters before running the strategy.")
            print()
            print("CRITICAL PARAMETERS TO UPDATE:")
            print("• symbol_name: Must match current futures contract (e.g., NIFTY25SEPFUT)")
            print("• buy_gap/sell_gap: Price gaps for order placement")
            print("• buy_quantity/sell_quantity: Position sizes for buy and sell orders")
            print("• order_type/variety: Order type and variety settings")
            print("• min_nifty_delta/max_nifty_delta: Portfolio delta limits")
            print()
            print("Example command line usage:")
            print("python wave.py \\")
            print("    --symbol-name NIFTY25SEPFUT \\")
            print("    --buy-gap 30 --sell-gap 30 \\")
            print("    --buy-quantity 50 --sell-quantity 50 \\")
            print("    --order-type LIMIT --variety REGULAR \\")
            print("    --min-nifty-delta -50 --max-nifty-delta 50")
            print("="*80)
            return False
        
        # If SOME values are still at defaults, show warning and ask for confirmation
        if len(unchanged_values) > 0:
            print("\n" + "="*80)
            print("⚠️  CONFIGURATION WARNING")
            print("="*80)
            print("Some configuration values are still at their defaults:")
            print()
            
            for value in unchanged_values:
                print(f"  ⚠️  {value}: {config.get(value)} (default)")
            
            if len(changed_values) > 0:
                print("\nUpdated values:")
                for value in changed_values:
                    print(f"  ✅ {value}: {config.get(value)} (updated)")
            
            print("\n" + "="*80)
            print("⚠️  WARNING: Running with default values may result in:")
            print("   • Trading wrong futures contract")
            print("   • Incorrect position sizes")
            print("   • Poor risk management")
            print("   • Potential losses")
            print("="*80)
            
            # Ask for user confirmation
            while True:
                response = input("\nDo you want to proceed with this configuration? (yes/no): ").lower().strip()
                if response in ['yes', 'y']:
                    print("\n✅ Proceeding with current configuration...")
                    return True
                elif response in ['no', 'n']:
                    print("\n❌ Strategy execution cancelled by user.")
                    print("Please update your configuration and try again.")
                    return False
                else:
                    print("Please enter 'yes' or 'no'.")
        
        # If all values have been updated, proceed without confirmation
        print("\n" + "="*80)
        print("✅ CONFIGURATION VALIDATION PASSED")
        print("="*80)
        print("All critical parameters have been updated from defaults.")
        print("Proceeding with strategy execution...")
        print("="*80)
        return True
    
    # Run configuration validation
    if not validate_configuration(config):
        logger.error("Configuration validation failed. Please update your configuration.")
        sys.exit(1)

    # Log configuration source and overrides
    if overridden_params:
        logger.info(f"Configuration loaded from {config_file} with command line overrides:")
        for param in overridden_params:
            logger.info(f"  Override: {param}")
    else:
        logger.info(f"Using default configuration from {config_file}")

    # Log key trading parameters for verification
    logger.info(f"Trading Configuration:")
    logger.info(f"  Symbol: {config['symbol_name']}, Exchange: {config['exchange']}")
    logger.info(f"  Gaps - Buy: {config['buy_gap']}, Sell: {config['sell_gap']}")
    logger.info(f"  Quantities - Buy: {config['buy_quantity']}, Sell: {config['sell_quantity']}")
    logger.info(f"  Cool-off: {config['cool_off_time']}s")
    logger.info(f"  Order Type: {config['order_type']}, Variety: {config['variety']}")
    logger.info(f"  NIFTY Delta Limits: {config['min_nifty_delta']} to {config['max_nifty_delta']}")
    logger.info(f"  BANKNIFTY Delta Limits: {config['min_bank_nifty_delta']} to {config['max_bank_nifty_delta']}")

    # ==========================================================================
    # SECTION 4: TRADING INFRASTRUCTURE SETUP
    # ==========================================================================
    
    # Create broker interface for market data and order execution
    broker = BrokerGateway.from_name(os.getenv("BROKER_NAME"))
    
    if broker is None:
        logger.error("Broker not initialized. Please configure it properly.")
        sys.exit(1)

    # Create order tracking system for position management
    order_tracker = OrderTracker()

    # ==========================================================================
    # SECTION 5: STRATEGY INITIALIZATION AND EXECUTION
    # ==========================================================================
    # Initialize trading system
    trading_system = WaveStrategy(config, broker, order_tracker)

    # Get instrument token for the underlying index
    # This token is used for websocket subscription to receive real-time price updates
    # try:
    #     quote_data = broker.get_quote(config['index_symbol'])
    #     instrument_token = quote_data[config['index_symbol']]['instrument_token']
    #     logger.info(f"✓ Index instrument token obtained: {instrument_token}")
    # except Exception as e:
    #     logger.error(f"Failed to get instrument token for {config['index_symbol']}: {e}")
    #     sys.exit(1)

    # Initialize data dispatcher for handling real-time market data
    # The dispatcher manages queues and routes market data to strategy
    dispatcher = DataDispatcher()
    dispatcher.register_main_queue(Queue())

    order_dispatcher = DataDispatcher()
    order_dispatcher.register_main_queue(Queue())

    # ==========================================================================
    # SECTION 5: WEBSOCKET CALLBACK CONFIGURATION  
    # ==========================================================================
    
    # Define websocket event handlers for real-time data processing
    
    def on_ticks(ws, ticks):
        logger.debug("Received ticks: {}".format(ticks))
        # Send tick data to strategy processing queue
        if isinstance(ticks, list):
            dispatcher.dispatch(ticks)
        else:
            if "symbol" in ticks:
                dispatcher.dispatch(ticks)

    def on_connect(ws, response):
        logger.info("Websocket connected successfully: {}".format(response))

    def on_order_update(ws, data):
        # logger.info(f"Order Update Received: {data}")
        trading_system.handle_order_update(data)
        
    # # Assign callbacks to broker's websocket instance
    # broker.on_ticks = on_ticks
    # broker.on_connect = on_connect
    # broker.on_order_update = on_order_update

    # ==========================================================================
    # SECTION 6: WEBSOCKET CONNECTION
    # ==========================================================================
    
    # Connect to the websocket
    # broker.connect_websocket(on_ticks=on_ticks, on_connect=on_connect)
    broker.connect_order_websocket(on_order_update=on_order_update)
    time.sleep(10) # Wait for 10 seconds to ensure the websocket is connected
        
    # Place initial wave order
    if not trading_system.check_is_any_order_active():
         trading_system.place_wave_order()
    
    
    # ==========================================================================
    # SECTION 6: MAIN MONITORING LOOP
    # ==========================================================================
    try:
        logger.info("--- Starting Main Monitoring Loop ---")
        while True:
            time.sleep(10)
            logger.info("Waking up for periodic check...")
            
            trading_system.print_current_status()
            
            trading_system.check_and_enforce_restrictions_on_active_orders()

            if not trading_system.check_is_any_order_active():
                logger.info("No active orders found. Placing a new wave order.")
                trading_system.place_wave_order()
                logger.info("Continuing to monitor.")
                
    except KeyboardInterrupt:
        # Handle graceful shutdown on Ctrl+C
        logger.info("SHUTDOWN REQUESTED - Stopping strategy...")
    except Exception as fatal_error:
        # Handle fatal errors that require strategy shutdown
        logger.error("FATAL ERROR in main trading loop:")
        logger.error(f"Error: {fatal_error}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        logger.info("STRATEGY SHUTDOWN COMPLETE")

