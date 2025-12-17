import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yaml
from logger import logger
from brokers import BrokerGateway, OrderRequest, Exchange, OrderType, TransactionType, ProductType

class SurvivorStrategy:
    """
    Survivor Options Trading Strategy
    
    This strategy implements a systematic approach to options trading based on price movements
    of the NIFTY index. The core concept is to sell options (both PE and CE) when the underlying
    index moves beyond certain thresholds, capturing premium decay while managing risk through
    dynamic gap adjustments.
    
    STRATEGY OVERVIEW:
    ==================
    
    1. **Dual-Side Trading**: The strategy monitors both upward and downward movements:
       - PE (Put) Trading: Triggered when NIFTY price moves UP beyond pe_gap threshold
       - CE (Call) Trading: Triggered when NIFTY price moves DOWN beyond ce_gap threshold
    
    2. **Gap-Based Execution**: 
       - Maintains reference points (nifty_pe_last_value, nifty_ce_last_value)
       - Executes trades when price deviates beyond configured gaps
       - Uses multipliers to scale position sizes based on gap magnitude
    
    3. **Dynamic Strike Selection**:
       - Selects option strikes based on symbol_gap from current price
       - Adjusts strikes if option premium is below minimum threshold
       - Ensures adequate liquidity and pricing
    
    4. **Reset Mechanism**:
       - Automatically adjusts reference points when market moves favorably
       - Prevents excessive accumulation of positions
       - Maintains strategy responsiveness to market conditions
    
    TRADING LOGIC EXAMPLE:
    =====================
    
    Scenario: NIFTY at 24,500, pe_gap=25, pe_symbol_gap=200
    
    1. Initial State: nifty_pe_last_value = 24,500
    2. NIFTY rises to 24,530 (difference = 30)
    3. Since 30 > pe_gap(25), trigger PE sell
    4. Sell multiplier = 30/25 = 1 (rounded down)
    5. Select PE strike at 24,500-200 = 24,300 PE
    6. Update reference: nifty_pe_last_value = 24,525 (24,500 + 25*1)
    
    CONFIGURATION PARAMETERS:
    ========================
    
    Core Parameters:
    - symbol_initials: Option series identifier (e.g., 'NIFTY25JAN30')
    - index_symbol: Underlying index for tracking (e.g., 'NSE:NIFTY 50')
    
    Gap Parameters:
    - pe_gap/ce_gap: Price movement thresholds to trigger trades
    - pe_symbol_gap/ce_symbol_gap: Strike distance from current price
    - pe_reset_gap/ce_reset_gap: Favorable movement thresholds for reference reset
    
    Quantity & Risk:
    - pe_quantity/ce_quantity: Base quantities for each trade
    - min_price_to_sell: Minimum option premium threshold
    - sell_multiplier_threshold: Maximum position scaling limit
    
    RISK MANAGEMENT:
    ===============
    
    1. **Premium Filtering**: Only sells options above min_price_to_sell
    2. **Position Scaling**: Limits multiplier to prevent oversized positions
    3. **Strike Adjustment**: Dynamically adjusts strikes for adequate premium
    4. **Reset Logic**: Prevents runaway reference point drift

    PS: This will only work with Zerodha broker out of the box. For Fyers, there needs to be some straight forward changes to get quotes, place orders etc.
    """
    
    def __init__(self, broker, config, order_tracker):
        # Assign config values as instance variables with 'strat_var_' prefix
        for k, v in config.items():
            setattr(self, f'strat_var_{k}', v)
        # External dependencies
        self.broker = broker
        self.symbol_initials = self.strat_var_symbol_initials
        self.order_tracker = order_tracker  # Store OrderTracker
        self.broker.download_instruments()
        self.instruments = self.broker.get_instruments()
        self.instruments = self.instruments[self.instruments['symbol'].str.contains(self.symbol_initials)]

        if self.instruments.shape[0] == 0:
            logger.error(f"No instruments found for {self.symbol_initials}")
            logger.error(f"Instument {self.symbol_initials} not found. Please check the symbol initials")
            return
        
        self.strike_difference = None      
        self._initialize_state()
        self.lot_size = self.instruments['lot_size'].iloc[0]
        
        # Calculate and store strike difference for the option series
        self.strike_difference = self._get_strike_difference(self.symbol_initials)
        logger.info(f"Strike difference for {self.symbol_initials} is {self.strike_difference}")

    def _nifty_quote(self):
        symbol_code = self.strat_var_index_symbol
        return self.broker.get_quote(symbol_code)

    def _initialize_state(self):

        # Initialize reset flags - these track when reset conditions are triggered
        self.pe_reset_gap_flag = 0  # Set to 1 when PE trade is executed
        self.ce_reset_gap_flag = 0  # Set to 1 when CE trade is executed
        
        # Get current market data for initialization
        current_quote = self._nifty_quote()
        
        # Initialize PE reference value
        if self.strat_var_pe_start_point == 0:
            # Use current market price as starting reference
            self.nifty_pe_last_value = current_quote.last_price
            logger.debug(f"Nifty PE Start Point is 0, so using LTP: {self.nifty_pe_last_value}")
        else:
            # Use configured starting point
            self.nifty_pe_last_value = self.strat_var_pe_start_point

        # Initialize CE reference value
        if self.strat_var_ce_start_point == 0:
            # Use current market price as starting reference
            self.nifty_ce_last_value = current_quote.last_price
            logger.debug(f"Nifty CE Start Point is 0, so using LTP: {self.nifty_ce_last_value}")
        else:
            # Use configured starting point
            self.nifty_ce_last_value = self.strat_var_ce_start_point
            
        logger.info(f"Nifty PE Start Value during initialization: {self.nifty_pe_last_value}, "
                   f"Nifty CE Start Value during initialization: {self.nifty_ce_last_value}")

    def _get_strike_difference(self, symbol_initials):
        if self.strike_difference is not None:
            return self.strike_difference
            
        # Filter for CE instruments to calculate strike difference 
        ce_instruments = self.instruments[
            self.instruments['symbol'].str.contains(symbol_initials) & 
            self.instruments['symbol'].str.endswith('CE')
        ]
        
        if ce_instruments.shape[0] < 2:
            logger.error(f"Not enough CE instruments found for {symbol_initials} to calculate strike difference")
            return 0
        # Sort by strike
        ce_instruments_sorted = ce_instruments.sort_values('strike')
        # Take the top 2
        top2 = ce_instruments_sorted.head(2)
        # Calculate the difference
        self.strike_difference = abs(top2.iloc[1]['strike'] - top2.iloc[0]['strike'])
        return self.strike_difference

    def on_ticks_update(self, ticks):
        """
        Main strategy execution method called on each tick update
        
        Args:
            ticks (dict): Market data containing 'last_price' and other tick information
            
        This is the core method that:
        1. Extracts current price from tick data
        2. Evaluates PE trading opportunities
        3. Evaluates CE trading opportunities  
        4. Applies reset logic for reference values
        
        Called externally by the main trading loop when new market data arrives
        """
        current_price = ticks['last_price'] if 'last_price' in ticks else ticks['ltp']
        
        # Process trading opportunities for both sides
        self._handle_pe_trade(current_price)  # Handle Put option opportunities
        self._handle_ce_trade(current_price)  # Handle Call option opportunities
        
        # Apply reset logic to adjust reference values
        self._reset_reference_values(current_price)

    def _check_sell_multiplier_breach(self, sell_multiplier):
        """
        Risk management check for position scaling
        
        Args:
            sell_multiplier (int): The calculated multiplier for position sizing
            
        Returns:
            bool: True if multiplier exceeds threshold, False otherwise
            
        This prevents excessive position sizes when large price movements occur.
        For example, if threshold is 3 and price moves 100 points with gap=25,
        multiplier would be 4, which exceeds threshold and blocks the trade.
        """
        if sell_multiplier > self.strat_var_sell_multiplier_threshold:
            logger.warning(f"Sell multiplier {sell_multiplier} breached the threshold {self.strat_var_sell_multiplier_threshold}")
            return True
        return False

    def _handle_pe_trade(self, current_price):
        """
        Handle PE (Put) option trading logic
        
        Args:
            current_price (float): Current NIFTY index price
            
        PE Trading Logic:
        - Triggered when current_price > nifty_pe_last_value + pe_gap
        - Sells PE options (benefits from upward price movement)
        - Updates reference value after execution
        
        Process:
        1. Check if upward movement exceeds gap threshold
        2. Calculate sell multiplier based on gap magnitude
        3. Validate multiplier doesn't breach risk limits
        4. Find appropriate PE strike with adequate premium
        5. Execute trade and update reference value
        
        Example:
        - Reference: 24,500, Gap: 25, Current: 24,560
        - Difference: 60, Multiplier: 60/25 = 2
        - Sell 2x PE quantity, Update reference to 24,550
        """
        # No action needed if price hasn't moved up sufficiently
        if current_price <= self.nifty_pe_last_value:
            self._log_stable_market(current_price)
            return

        # Calculate price difference and check if it exceeds gap threshold
        price_diff = round(current_price - self.nifty_pe_last_value, 0)
        if price_diff > self.strat_var_pe_gap:
            # Calculate multiplier for position sizing
            sell_multiplier = int(price_diff / self.strat_var_pe_gap)
            
            # Risk check: Ensure multiplier doesn't exceed threshold
            if self._check_sell_multiplier_breach(sell_multiplier):
                logger.warning(f"Sell multiplier {sell_multiplier} breached the threshold {self.strat_var_sell_multiplier_threshold}")
                return

            # Update reference value based on executed gaps
            self.nifty_pe_last_value += self.strat_var_pe_gap * sell_multiplier
            
            # Calculate total quantity to trade
            total_quantity = sell_multiplier * self.strat_var_pe_quantity

            # Find suitable PE option with adequate premium
            temp_gap = self.strat_var_pe_symbol_gap
            while True:
                # Find PE instrument at specified gap from current price
                instrument = self._find_nifty_symbol_from_gap("PE", current_price, gap=temp_gap)
                if not instrument:
                    logger.warning("No suitable instrument found for PE with gap %s", temp_gap)
                    return 
                
                # Get current quote for the selected instrument
                if ":" not in instrument['symbol']:
                    symbol_code = self.strat_var_exchange + ":" + instrument['symbol']
                else:
                    symbol_code = instrument['symbol']
                quote = self.broker.get_quote(symbol_code)
                
                # Check if premium meets minimum threshold
                if quote.last_price < self.strat_var_min_price_to_sell:
                    logger.info(f"Last price {quote.last_price} is less than min price to sell {self.strat_var_min_price_to_sell}")
                    # Try closer strike if premium is too low
                    temp_gap -= self.lot_size
                    continue
                    
                # Execute the trade
                logger.info(f"Execute PE sell @ {instrument['symbol']} × {total_quantity}, Market Price")
                self._place_order(instrument['symbol'], total_quantity)
                
                # Set reset flag to enable reset logic
                self.pe_reset_gap_flag = 1
                break

    def _handle_ce_trade(self, current_price):
        """
        Handle CE (Call) option trading logic
        
        Args:
            current_price (float): Current NIFTY index price
            
        CE Trading Logic:
        - Triggered when current_price < nifty_ce_last_value - ce_gap
        - Sells CE options (benefits from downward price movement)
        - Updates reference value after execution
        
        Process:
        1. Check if downward movement exceeds gap threshold
        2. Calculate sell multiplier based on gap magnitude
        3. Validate multiplier doesn't breach risk limits
        4. Find appropriate CE strike with adequate premium
        5. Execute trade and update reference value
        
        Example:
        - Reference: 24,500, Gap: 25, Current: 24,440
        - Difference: 60, Multiplier: 60/25 = 2
        - Sell 2x CE quantity, Update reference to 24,450
        """
        # No action needed if price hasn't moved down sufficiently
        if current_price >= self.nifty_ce_last_value:
            self._log_stable_market(current_price)
            return

        # Calculate price difference and check if it exceeds gap threshold
        price_diff = round(self.nifty_ce_last_value - current_price, 0)  
        if price_diff > self.strat_var_ce_gap:
            # Calculate multiplier for position sizing
            sell_multiplier = int(price_diff / self.strat_var_ce_gap)
            
            # Risk check: Ensure multiplier doesn't exceed threshold
            if self._check_sell_multiplier_breach(sell_multiplier):
                logger.warning(f"Sell multiplier {sell_multiplier} breached the threshold {self.strat_var_sell_multiplier_threshold}")
                return

            # Update reference value based on executed gaps
            self.nifty_ce_last_value -= self.strat_var_ce_gap * sell_multiplier
            
            # Calculate total quantity to trade
            total_quantity = sell_multiplier * self.strat_var_ce_quantity

            # Find suitable CE option with adequate premium
            temp_gap = self.strat_var_ce_symbol_gap 
            while True:
                # Find CE instrument at specified gap from current price
                instrument = self._find_nifty_symbol_from_gap("CE", current_price, gap=temp_gap)
                if not instrument:
                    logger.warning("No suitable instrument found for CE with gap %s", temp_gap)
                    return
                    
                # Get current quote for the selected instrument
                if ":" not in instrument['symbol']:
                    symbol_code = self.strat_var_exchange + ":" + instrument['symbol']
                else:
                    symbol_code = instrument['symbol']
                quote = self.broker.get_quote(symbol_code)
                # Check if premium meets minimum threshold
                if quote.last_price < self.strat_var_min_price_to_sell:
                    logger.info(f"Last price {quote.last_price} is less than min price to sell {self.strat_var_min_price_to_sell}, trying next strike")
                    # Try closer strike if premium is too low
                    temp_gap -= self.lot_size
                    continue
                    
                # Execute the trade
                logger.info(f"Execute CE sell @ {instrument['symbol']} × {total_quantity}, Market Price")
                self._place_order(instrument['symbol'], total_quantity)
                
                # Set reset flag to enable reset logic
                self.ce_reset_gap_flag = 1
                break

    def _reset_reference_values(self, current_price):
        """
        Reset reference values when market moves favorably
        
        Args:
            current_price (float): Current NIFTY index price
            
        Reset Logic:
        - PE Reset: When price drops significantly below PE reference AND reset flag is set
        - CE Reset: When price rises significantly above CE reference AND reset flag is set
        
        Purpose:
        1. Prevents reference values from drifting too far from market
        2. Maintains strategy responsiveness to changing market conditions
        3. Reduces risk of excessive position accumulation
        
        Reset Conditions:
        - PE: (pe_last_value - current_price) > pe_reset_gap AND pe_reset_gap_flag = 1
        - CE: (current_price - ce_last_value) > ce_reset_gap AND ce_reset_gap_flag = 1
        
        Example PE Reset:
        - PE Reference: 24,550, Current: 24,480, Reset Gap: 50
        - Difference: 70 > 50, so reset PE reference to 24,530 (24,480 + 50)
        """
        # PE Reset Logic: Reset when price drops significantly below PE reference
        if (self.nifty_pe_last_value - current_price) > self.strat_var_pe_reset_gap and self.pe_reset_gap_flag:
            logger.info(f"Resetting PE value from {self.nifty_pe_last_value} to {current_price + self.strat_var_pe_reset_gap}")
            # Reset PE reference to current price plus reset gap
            self.nifty_pe_last_value = current_price + self.strat_var_pe_reset_gap

        # CE Reset Logic: Reset when price rises significantly above CE reference  
        if (current_price - self.nifty_ce_last_value) > self.strat_var_ce_reset_gap and self.ce_reset_gap_flag:
            logger.info(f"Resetting CE value from {self.nifty_ce_last_value} to {current_price - self.strat_var_ce_reset_gap}")
            # Reset CE reference to current price minus reset gap
            self.nifty_ce_last_value = current_price - self.strat_var_ce_reset_gap

    def _find_nifty_symbol_from_gap(self, option_type, ltp, gap):
        """
        Find the most suitable option instrument based on strike distance from current price
        
        Args:
            option_type (str): 'PE' or 'CE' - type of option to find
            ltp (float): Last traded price of the underlying (current NIFTY price)
            gap (int): Distance from current price to target strike
            
        Returns:
            dict: Instrument details including symbol, strike, etc., or None if not found
            
        Strike Selection Logic:
        1. For PE: target_strike = ltp - gap (out-of-the-money puts)
        2. For CE: target_strike = ltp + gap (out-of-the-money calls)
        3. Find closest available strike within half strike difference tolerance
        4. Return the best match
        
        Example:
        - LTP: 24,500, Gap: 200, Option Type: PE
        - Target Strike: 24,300
        - Find closest available strike to 24,300 (e.g., 24,300 or 24,250)
        
        Filtering Criteria:
        - Must match symbol_initials (correct expiry series)
        - Must be the correct option type (PE/CE)
        - Must be in NFO-OPT segment
        - Must be within acceptable strike range
        """
        # Convert gap to symbol_gap based on option type
        if option_type == "PE":
            symbol_gap = -gap  # Negative for PE (below current price)
        else:
            symbol_gap = gap   # Positive for CE (above current price)
            
        # Calculate target strike price
        target_strike = ltp + symbol_gap
        
        # Filter instruments for matching criteria
        df = self.instruments[
            (self.instruments['symbol'].str.contains(self.strat_var_symbol_initials)) &
            (self.instruments['instrument_type'] == option_type) &
            (self.instruments['segment'] == "NFO-OPT")
        ]
        
        if df.empty:
            return None
            
        # Find closest strike within acceptable tolerance
        df['target_strike_diff'] = (df['strike'] - target_strike).abs()
        
        # Filter to strikes within half strike difference (tolerance for rounding)
        tolerance = self._get_strike_difference(self.strat_var_symbol_initials) / 2
        df = df[df['target_strike_diff'] <= tolerance]
        
        if df.empty:
            logger.error(f"No instrument found for {self.strat_var_symbol_initials} {option_type} "
                        f"within {tolerance} of {target_strike}")
            return None
            
        # Return the closest match
        best = df.sort_values('target_strike_diff').iloc[0]
        return best.to_dict()

    def _find_price_eligible_symbol(self, option_type):
        """
        Find an option symbol that meets premium requirements
        
        Args:
            option_type (str): 'PE' or 'CE'
            
        Returns:
            dict: Instrument details for eligible option, or None if none found
            
        This method iteratively searches for options that:
        1. Meet the gap criteria
        2. Have premium above minimum threshold
        3. Are liquid and tradeable
        
        Note: This method appears to have some issues and may not be actively used
        in the current implementation. The main trading methods use inline logic instead.
        """
        # Get initial gap based on option type
        temp_gap = self.strat_var_pe_symbol_gap if option_type == "PE" else self.strat_var_ce_symbol_gap
        
        while True:
            # Get current market price
            ltp = self._nifty_quote().last_price
            
            # Find instrument at current gap
            instrument = self._find_nifty_symbol_from_gap(
                self.instruments, self.strat_var_symbol_initials, temp_gap, option_type, ltp, self.lot_size
            )
            
            if instrument is None:
                return None
                
            # Check if premium meets minimum threshold
            symbol_code = f"{self.strat_var_exchange}:{instrument['symbol']}"
            price = float(self.broker.get_quote(symbol_code).last_price)
            
            if price < self.strat_var_min_price_to_sell:
                # Try closer strike if premium too low
                temp_gap -= self.lot_size
            else:
                return instrument

    def _place_order(self, symbol, quantity):
        """
        Execute order placement through the broker
        
        Args:
            symbol (str): Trading symbol for the option
            quantity (int): Number of lots/shares to trade
            
        Process:
        1. Place market order through broker interface
        2. Log order details
        3. Track order in order management system
        4. Handle order failures gracefully
        
        Order Parameters:
        - Transaction Type: From configuration (typically SELL)
        - Order Type: From configuration (typically MARKET)
        - Exchange: From configuration (typically NFO)
        - Product: From configuration (NRML/MIS)
        - Variety: Always REGULAR
        - Tag: "Survivor" for identification
        """
        # Place order through broker interface
        if self.strat_var_exchange == "NFO":
            exchange = Exchange.NFO

        req = OrderRequest(
                symbol=symbol, exchange=exchange, transaction_type=TransactionType.SELL,
                quantity=quantity, product_type=ProductType.MARGIN, order_type=OrderType.MARKET,
                price=None, tag=self.strat_var_tag
            )
        order_resp = self.broker.place_order(req)
        order_status = order_resp.status
        logger.debug(f"Order placement response: {order_resp}")
        order_id = order_resp.order_id

        # Handle order placement failure
        if order_id == -1 or order_status == "error":
            logger.error(f"Order placement failed for {symbol} × {quantity}, Market Price")
            return
            
        logger.info(f"Placing order for {symbol} × {quantity}, Market Price")
        
        # Track the order using OrderTracker
        from datetime import datetime
        order_details = {
            "order_id": order_id,
            "symbol": symbol,
            "transaction_type": self.strat_var_trans_type,
            "quantity": quantity,
            "price": None,  # Market order
            "timestamp": datetime.now().isoformat(),
        }
        
        # Add to order tracking system
        # self.order_tracker.add_order(order_details)
        
        # Log order placement for strategy tracking
        logger.info(f"Survivor order tracked: {order_id} - {self.strat_var_trans_type} {symbol} × {quantity}")
        

    def _log_stable_market(self, current_val):
        """
        Log current market state when no trading action is taken

        """
        logger.info(
            f"{self.strat_var_symbol_initials} Nifty under control. "
            f"PE = {self.nifty_pe_last_value}, "
            f"CE = {self.nifty_ce_last_value}, "
            f"Current = {current_val}, "
            f"CE Gap = {self.strat_var_ce_gap}, "
            f"PE Gap = {self.strat_var_pe_gap}"
        )


# Below Logic is for
# 1. command line arguments and 
# 2. run the strategy in a loop

# =============================================================================
# MAIN SCRIPT EXECUTION
# =============================================================================
# 
# This section provides a complete command-line interface for running the
# Survivor Strategy with flexible configuration options.
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
# python system/main.py
# 
# # Override specific parameters
# python system/main.py --symbol-initials NIFTY25807 --pe-gap 25 --ce-gap 25
# 
# # Full customization
# python system/main.py \
#     --symbol-initials NIFTY25807 \
#     --pe-symbol-gap 250 --ce-symbol-gap 250 \
#     --pe-gap 25 --ce-gap 25 \
#     --pe-quantity 75 --ce-quantity 75
#
# =============================================================================

