from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
import time
from typing import Any, Dict, List, Optional, Union

from .enums import Exchange, OrderType, ProductType, TransactionType, Validity
from .errors import MarginUnavailableError, UnsupportedOperationError
from .interface import BrokerDriver
from .schemas import (
    BrokerCapabilities,
    Funds,
    Instrument,
    OrderRequest,
    OrderResponse,
    Position,
    Quote,
)
from ..symbols.registry import symbol_registry


class BrokerGateway:
    """Facade orchestrating symbol normalization and delegation to a driver."""

    def __init__(self, driver: BrokerDriver, broker_name: str) -> None:
        self.driver = driver
        self.broker_name = broker_name

    # --- Construction helpers ---
    @classmethod
    def from_name(cls, name: str) -> "BrokerGateway":
        from ..registry import BrokerRegistry

        driver = BrokerRegistry.create(name)
        return cls(driver=driver, broker_name=name.lower())

    # --- Capability ---
    def get_capabilities(self) -> BrokerCapabilities:
        return self.driver.get_capabilities()

    # --- Account ---
    def get_funds(self) -> Funds:
        return self.driver.get_funds()

    def get_positions(self) -> List[Position]:
        return self.driver.get_positions()

    def get_position(self, symbol: str, exchange: Optional[str] = None) -> Optional[Position]:
        return self.driver.get_position(symbol, exchange)

    # --- Orders ---
    def place_order(self, request: Union[OrderRequest, Dict[str, Any]]) -> Union[OrderResponse, Dict[str, Any]]:
        # Back-compat: accept Fyers-like dicts and return legacy-shaped dict
        if isinstance(request, dict):
            req_obj = self._dict_to_order_request(request)
            resp = self.place_order(req_obj)  # type: ignore[arg-type]
            # Convert to legacy Fyers-like response shape
            result: Dict[str, Any] = {
                "s": "ok" if resp.status == "ok" else "error",
                "id": resp.order_id,
            }
            if resp.message:
                result["message"] = resp.message
            if resp.raw is not None:
                result["raw"] = resp.raw
            return result

        # Typed path
        internal = f"{request.exchange.value}:{request.symbol}"
        broker_symbol = symbol_registry.to_broker_symbol(self.broker_name, internal)
        req2 = replace(
            request,
            symbol=broker_symbol.split(":", 1)[1] if ":" in broker_symbol else broker_symbol,
        )
        return self.driver.place_order(req2)

    def cancel_order(self, order_id: Union[str, Dict[str, Any]]) -> Union[OrderResponse, Dict[str, Any]]:
        # Back-compat: allow dict {"id": ...}
        if isinstance(order_id, dict):
            oid = str(order_id.get("id") or order_id.get("order_id") or "")
            resp = self.driver.cancel_order(oid)
            return {"s": "ok" if resp.status == "ok" else "error", "id": oid, "raw": resp.raw}
        return self.driver.cancel_order(str(order_id))

    def modify_order(self, order_id: str, updates: Dict[str, Any]) -> OrderResponse:
        return self.driver.modify_order(order_id, updates)

    def get_orderbook(self) -> List[Dict[str, Any]]:
        return self.driver.get_orderbook()

    def get_tradebook(self) -> List[Dict[str, Any]]:
        return self.driver.get_tradebook()

    def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        return self.driver.get_order(order_id)

    # --- Market data ---
    def get_quote(self, symbol: str) -> Quote:
        internal = symbol_registry.normalize(symbol)
        broker_symbol = symbol_registry.to_broker_symbol(self.broker_name, internal)
        return self.driver.get_quote(broker_symbol)

    def get_quotes(self, symbols: List[str]) -> Dict[str, Quote]:
        internal_symbols = [symbol_registry.normalize(s) for s in symbols]
        broker_symbols = [symbol_registry.to_broker_symbol(self.broker_name, s) for s in internal_symbols]
        return self.driver.get_quotes(broker_symbols)

    def get_history(self, symbol: str, interval: str, start: str, end: str, oi: bool = False) -> List[Dict[str, Any]]:
        """
        Retrieve historical data with automatic chunking to handle API limitations.
        
        Args:
            symbol (str): Trading symbol
            interval (str): Timeframe interval (e.g., "1m", "5m", "1d")
            start (str): Start date in format YYYY-MM-DD
            end (str): End date in format YYYY-MM-DD
            oi (bool): Whether to include open interest data
            
        Returns:
            List[Dict[str, Any]]: Combined historical data from all chunks
        """
        internal = symbol_registry.normalize(symbol)
        broker_symbol = symbol_registry.to_broker_symbol(self.broker_name, internal)
        
        # Convert string dates to datetime objects
        start_dt = datetime.strptime(start, "%Y-%m-%d")
        end_dt = datetime.strptime(end, "%Y-%m-%d")
        
        # Determine chunk size based on interval
        if interval in ["day", "1d", "D", "1D"]:
            # For daily resolution: up to 366 days per request
            max_days = 366
        elif interval in ["5S", "10S", "15S", "30S", "45S"]:
            # For seconds resolution: up to 30 trading days
            max_days = 30
        else:
            # For minute resolutions: up to 100 days per request
            max_days = 100
        
        # Initialize result container
        all_candles = []
        
        # Break the date range into chunks
        current_start = start_dt
        while current_start <= end_dt:
            # Calculate end date for this chunk
            current_end = min(current_start + timedelta(days=max_days - 1), end_dt)
            
            # Format dates for API request
            chunk_start = current_start.strftime("%Y-%m-%d")
            chunk_end = current_end.strftime("%Y-%m-%d")
            
            # Get data for this chunk
            chunk_data = self.driver.get_history(broker_symbol, interval, chunk_start, chunk_end, oi)
            
            # Extend results with chunk data
            if chunk_data:
                all_candles.extend(chunk_data)
            
            # Add a small delay to avoid rate limiting
            time.sleep(0.5)
            
            # Move to next chunk
            current_start = current_end + timedelta(days=1)
        
        return all_candles

    # --- Option chain ---
    def get_option_chain(self, underlying: str, exchange: str, **kwargs: Any) -> List[Dict[str, Any]]:
        return self.driver.get_option_chain(underlying, exchange, **kwargs)

    # --- Instruments ---
    def download_instruments(self) -> None:
        self.driver.download_instruments()

    def get_instruments(self) -> List[Instrument]:
        return self.driver.get_instruments()

    def get_nse_futures_symbols(self) -> List[str]:
        return self.driver.get_nse_futures_symbols()

    # --- Websocket ---
    def connect_websocket(
        self,
        *,
        on_ticks: Any | None = None,
        on_connect: Any | None = None,
        on_error: Any | None = None,
        on_close: Any | None = None,
        on_reconnect: Any | None = None,
        on_noreconnect: Any | None = None,
        **kwargs: Any,
    ) -> None:
        # Forward known callbacks and any extra kwargs (e.g., simulate_date for fyrodha)
        self.driver.connect_websocket(
            on_ticks=on_ticks,
            on_connect=on_connect,
            on_error=on_error,
            on_close=on_close,
            on_reconnect=on_reconnect,
            on_noreconnect=on_noreconnect,
            **kwargs,
        )

    def symbols_to_subscribe(self, symbols: List[str]) -> None:
        internal_symbols = [symbol_registry.normalize(s) for s in symbols]
        broker_symbols = [symbol_registry.to_broker_symbol(self.broker_name, s) for s in internal_symbols]
        self.driver.symbols_to_subscribe(broker_symbols)

    def connect_order_websocket(
        self,
        *,
        on_order_update: Any | None = None,
        on_trades: Any | None = None,
        on_positions: Any | None = None,
        on_general: Any | None = None,
        on_error: Any | None = None,
        on_close: Any | None = None,
        on_connect: Any | None = None,
    ) -> None:
        self.driver.connect_order_websocket(
            on_order_update=on_order_update,
            on_trades=on_trades,
            on_positions=on_positions,
            on_general=on_general,
            on_error=on_error,
            on_close=on_close,
            on_connect=on_connect,
        )

    def unsubscribe(self, symbols: List[str]) -> None:
        internal_symbols = [symbol_registry.normalize(s) for s in symbols]
        broker_symbols = [symbol_registry.to_broker_symbol(self.broker_name, s) for s in internal_symbols]
        self.driver.unsubscribe(broker_symbols)

    # --- Advanced orders ---
    def place_gtt_order(self, *args: Any, **kwargs: Any) -> OrderResponse:
        return self.driver.place_gtt_order(*args, **kwargs)

    def place_bracket_order(self, *args: Any, **kwargs: Any) -> OrderResponse:
        return self.driver.place_bracket_order(*args, **kwargs)

    def place_cover_order(self, *args: Any, **kwargs: Any) -> OrderResponse:
        return self.driver.place_cover_order(*args, **kwargs)

    def place_basket_orders(self, requests: List[OrderRequest]) -> List[OrderResponse]:
        return self.driver.place_basket_orders(requests)

    def place_multileg_order(self, *args: Any, **kwargs: Any) -> OrderResponse:
        return self.driver.place_multileg_order(*args, **kwargs)

    # --- Margins ---
    def get_margins_required(self, orders: List[Dict[str, Any]]) -> Any:
        # Enforce policy: never estimate margins locally; delegate and let driver raise errors if unavailable
        if not self.driver.get_capabilities().supports_place_order:
            raise UnsupportedOperationError("Broker does not support order placement/margins")
        result = self.driver.get_margins_required(orders)
        if result is None:
            raise MarginUnavailableError("Broker did not return margins; unavailable")
        return result

    def get_span_margin(self, orders: List[Dict[str, Any]]) -> Any:
        result = self.driver.get_span_margin(orders)
        if result is None:
            raise MarginUnavailableError("Broker did not return span margins; unavailable")
        return result

    def get_multiorder_margin(self, orders: List[Dict[str, Any]]) -> Any:
        result = self.driver.get_multiorder_margin(orders)
        if result is None:
            raise MarginUnavailableError("Broker did not return multiorder margins; unavailable")
        return result

    # --- Internal helpers ---
    def _normalize_margin_orders(self, orders: List[Any]) -> List[Dict[str, Any]]:
        """Normalize incoming margin order inputs to the selected broker's expected payload.

        Accepts either legacy Fyers-shaped dicts or standardized OrderRequest objects and
        converts them to the adapter-specific request shape.
        """
        from .schemas import OrderRequest as _OrderRequest

        broker = (self.broker_name or "").lower()
        out: List[Dict[str, Any]] = []

        # Fast path: Fyers accepts Fyers-shaped dicts and we keep OrderRequest mapping inside driver
        if broker == "fyers":
            for o in orders:
                if isinstance(o, dict):
                    out.append(o)
                else:
                    # Let driver map standardized request
                    out.append({"__order_request__": o})  # sentinel for driver path
            return out

        # Zerodha mapping: convert generic/Fyers-like dicts or OrderRequest into order_margins payload
        if broker == "zerodha":
            for o in orders:
                if isinstance(o, _OrderRequest):
                    exch = o.exchange.value
                    tsym = o.symbol
                    sym_u = tsym.upper()
                    # Derivatives on Zerodha live under NFO/BFO
                    if exch == "NSE" and (sym_u.endswith("CE") or sym_u.endswith("PE") or "FUT" in sym_u):
                        exch = "NFO"
                    # Equity trims -EQ suffix
                    if sym_u.endswith("-EQ"):
                        tsym = tsym[:-3]
                    out.append(
                        {
                            "exchange": exch,
                            "tradingsymbol": tsym,
                            "transaction_type": "BUY" if o.transaction_type.value == "BUY" else "SELL",
                            "variety": "regular",
                            "product": {"INTRADAY": "MIS", "CNC": "CNC", "MARGIN": "NRML"}[o.product_type.value],
                            "order_type": {"MARKET": "MARKET", "LIMIT": "LIMIT", "STOP": "SL-M", "STOP_LIMIT": "SL"}[o.order_type.value],
                            "quantity": int(o.quantity),
                            "price": float(o.price) if o.price is not None else 0.0,
                            "trigger_price": float(o.stop_price) if o.stop_price is not None else 0.0,
                        }
                    )
                elif isinstance(o, dict):
                    # Likely Fyers-shaped
                    symbol = str(o.get("symbol", ""))
                    exch = symbol.split(":", 1)[0] if ":" in symbol else "NSE"
                    tsym = symbol.split(":", 1)[1] if ":" in symbol else symbol
                    tsym_u = tsym.upper()
                    # Map exchange for derivatives
                    if exch == "NSE" and (tsym_u.endswith("CE") or tsym_u.endswith("PE") or "FUT" in tsym_u):
                        exch = "NFO"
                    # Trim -EQ for equity
                    if tsym_u.endswith("-EQ"):
                        tsym = tsym[:-3]
                    side = o.get("side")
                    txn = "BUY" if int(side) == 1 else "SELL"
                    typ = int(o.get("type", 2))
                    order_type = {1: "LIMIT", 2: "MARKET", 3: "SL-M", 4: "SL"}.get(typ, "MARKET")
                    prod = str(o.get("productType", "INTRADAY")).upper()
                    product = {"INTRADAY": "MIS", "CNC": "CNC", "MARGIN": "NRML"}.get(prod, "MIS")
                    qty = int(o.get("qty") or o.get("quantity") or 1)
                    price = float(o.get("limitPrice") or o.get("price") or 0.0)
                    trigger = float(o.get("stopPrice") or o.get("trigger_price") or o.get("stopLoss") or 0.0)
                    out.append(
                        {
                            "exchange": exch,
                            "tradingsymbol": tsym,
                            "transaction_type": txn,
                            "variety": "regular",
                            "product": product,
                            "order_type": order_type,
                            "quantity": qty,
                            "price": price if order_type == "LIMIT" else 0.0,
                            "trigger_price": trigger,
                        }
                    )
                else:
                    # Unknown
                    continue
            return out

        # Default passthrough
        return [o if isinstance(o, dict) else {"__order_request__": o} for o in orders]

    def _dict_to_order_request(self, payload: Dict[str, Any]) -> OrderRequest:
        """Convert a Fyers-like order dict into a standardized OrderRequest."""
        # Symbol handling
        symbol = str(payload.get("symbol", ""))
        if ":" in symbol:
            exch_str, sym = symbol.split(":", 1)
        else:
            exch_str, sym = "NSE", symbol
        # Remove -EQ suffix for canonical symbol
        if sym.upper().endswith("-EQ"):
            sym = sym[:-3]

        # Quantity
        qty = int(payload.get("qty") or payload.get("quantity") or 1)

        # Order type mapping
        fy_type = int(payload.get("type", 2))
        order_type = {1: OrderType.LIMIT, 2: OrderType.MARKET, 3: OrderType.STOP, 4: OrderType.STOP_LIMIT}.get(
            fy_type, OrderType.MARKET
        )

        # Side mapping
        side = int(payload.get("side", 1))
        txn = TransactionType.BUY if side == 1 else TransactionType.SELL

        # Product mapping
        prod_str = str(payload.get("productType", "INTRADAY")).upper()
        product = {
            "INTRADAY": ProductType.INTRADAY,
            "CNC": ProductType.CNC,
            "MARGIN": ProductType.MARGIN,
        }.get(prod_str, ProductType.INTRADAY)

        # Prices
        price = payload.get("limitPrice")
        stop_price = payload.get("stopPrice")

        # Validity
        validity = Validity.DAY if str(payload.get("validity", "DAY")).upper() == "DAY" else Validity.IOC

        tag = payload.get("orderTag") or payload.get("tag")

        return OrderRequest(
            symbol=sym,
            exchange=Exchange[exch_str],
            quantity=qty,
            order_type=order_type,
            transaction_type=txn,
            product_type=product,
            price=float(price) if price is not None else None,
            stop_price=float(stop_price) if stop_price is not None else None,
            validity=validity,
            tag=str(tag) if tag is not None else None,
            extras={
                k: payload[k]
                for k in (
                    "disclosedQty",
                    "offlineOrder",
                    "stopLoss",
                    "takeProfit",
                )
                if k in payload
            },
        )


