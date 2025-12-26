from __future__ import annotations

from datetime import datetime
import os
from typing import Any, Dict, List, Optional
from urllib import request

from ...core.enums import Exchange, OrderType, ProductType, TransactionType, Validity
from ...core.errors import MarginUnavailableError, UnsupportedOperationError
from ...core.interface import BrokerDriver
from ...core.schemas import (
    BrokerCapabilities,
    Funds,
    Instrument,
    OrderRequest,
    OrderResponse,
    Position,
    Quote,
)
from ...mappings import MappingRegistry as M
import pandas as pd
import numpy as np

class ZerodhaDriver(BrokerDriver):
    """Zerodha driver using kiteconnect when available.

    This initial pass exposes the interface; concrete methods will be implemented
    incrementally to keep changes reviewable.
    """

    def __init__(self, *, login_mode: Optional[str] = None) -> None:
        super().__init__()
        self.capabilities = BrokerCapabilities(
            supports_historical=True,
            supports_quotes=True,
            supports_funds=True,
            supports_positions=True,
            supports_place_order=True,
            supports_modify_order=True,
            supports_cancel_order=True,
            supports_tradebook=True,
            supports_orderbook=True,
            supports_websocket=True,
            supports_order_websocket=True,
            supports_master_contract=True,
            supports_option_chain=False,
            supports_gtt=True,
            supports_bracket_order=False,
            supports_cover_order=True,
            supports_multileg_order=False,
            supports_basket_orders=True,
        )
        self._kite = None  # kiteconnect client if available
        self._kite_ws = None

        # Try to wire a ready KiteConnect if env provides api_key + access_token
        import os
        api_key = os.getenv("BROKER_API_KEY") or os.getenv("KITE_API_KEY") or os.getenv("ZERODHA_API_KEY")
        access_token = (
            os.getenv("BROKER_ACCESS_TOKEN") or os.getenv("KITE_ACCESS_TOKEN") or os.getenv("ZERODHA_ACCESS_TOKEN")
        )
        if api_key and access_token:
            try:  # pragma: no cover - external package
                from kiteconnect import KiteConnect  # type: ignore

                kite = KiteConnect(api_key=api_key)
                kite.set_access_token(access_token)
                self._kite = kite
            except Exception:
                self._kite = None

        # Optional TOTP login if requested and tokens missing
        if self._kite is None:
            login_mode = (os.getenv("BROKER_LOGIN_MODE") or "auto").lower()
            if login_mode in ("totp", "auto"):
                kite_totp = self._authenticate_via_totp()
                if kite_totp is not None:
                    self._kite = kite_totp

        # Optional manual login if no token and login_mode permits
        if self._kite is None:
            login_mode = (os.getenv("BROKER_LOGIN_MODE") or "auto").lower()
            if login_mode in ("manual", "auto"):
                try:  # pragma: no cover - interactive
                    from kiteconnect import KiteConnect  # type: ignore
                    from ...auth.manual import manual_exchange_request_token

                    api_key2 = api_key or os.getenv("KITE_API_KEY") or os.getenv("ZERODHA_API_KEY")
                    api_secret = os.getenv("BROKER_API_SECRET") or os.getenv("KITE_API_SECRET") or os.getenv("ZERODHA_API_SECRET")
                    if api_key2 and api_secret:
                        kite2 = KiteConnect(api_key=api_key2)
                        url = kite2.login_url()
                        request_token = manual_exchange_request_token(url)
                        sess = kite2.generate_session(request_token, api_secret)
                        token = sess.get("access_token")
                        if token:
                            kite2.set_access_token(token)
                            self._kite = kite2
                except Exception:
                    # Keep unauthenticated if manual flow fails
                    pass

    def _authenticate_via_totp(self) -> Optional[Any]:
        """Programmatic TOTP login using Zerodha web endpoints to obtain access token.

        Requires env vars:
        - BROKER_API_KEY (or KITE_API_KEY/ZERODHA_API_KEY)
        - BROKER_API_SECRET (or KITE_API_SECRET/ZERODHA_API_SECRET)
        - BROKER_ID
        - BROKER_TOTP_KEY
        - BROKER_PASSWORD
        """
        import os
        try:  # pragma: no cover - external packages
            import requests  # type: ignore
            import pyotp  # type: ignore
            from kiteconnect import KiteConnect  # type: ignore
        except Exception:
            return None

        api_key = os.getenv("BROKER_API_KEY") or os.getenv("KITE_API_KEY") or os.getenv("ZERODHA_API_KEY")
        api_secret = os.getenv("BROKER_API_SECRET") or os.getenv("KITE_API_SECRET") or os.getenv("ZERODHA_API_SECRET")
        broker_id = os.getenv("BROKER_ID")
        totp_secret = os.getenv("BROKER_TOTP_KEY")
        password = os.getenv("BROKER_PASSWORD")
        if not all([api_key, api_secret, broker_id, totp_secret, password]):
            return None

        try:
            session = requests.Session()
            login_resp = session.post(
                "https://kite.zerodha.com/api/login",
                data={"user_id": broker_id, "password": password},
                timeout=30,
            )
            login_data = login_resp.json()
            if "data" not in login_data:
                return None
            request_id = login_data["data"]["request_id"]

            twofa_resp = session.post(
                "https://kite.zerodha.com/api/twofa",
                data={
                    "user_id": broker_id,
                    "request_id": request_id,
                    "twofa_value": pyotp.TOTP(totp_secret).now(),
                },
                timeout=30,
            )
            twofa_data = twofa_resp.json()
            if "data" not in twofa_data:
                return None

            connect_url = f"https://kite.trade/connect/login?api_key={api_key}"
            connect_resp = session.get(connect_url, allow_redirects=True, timeout=30)
            if "request_token=" not in connect_resp.url:
                return None
            request_token = connect_resp.url.split("request_token=")[1].split("&")[0]

            kite = KiteConnect(api_key=api_key)
            sess = kite.generate_session(request_token, api_secret)
            access_token = sess.get("access_token")
            if not access_token:
                return None
            kite.set_access_token(access_token)
            return kite
        except Exception:
            return None

    # --- Account ---
    def get_funds(self) -> Funds:
        if not self._kite:
            return Funds(equity=0.0, available_cash=0.0, used_margin=0.0, net=0.0, raw={"error": "unauthenticated"})
        try:
            data = self._kite.margins(segment="equity")
            equity = float(data.get("net", 0))
            available_cash = float(data.get("available", {}).get("cash", 0))
            used_margin = float(data.get("utilised", {}).get("debits", 0))
            net = float(data.get("net", 0))
            return Funds(equity=equity, available_cash=available_cash, used_margin=used_margin, net=net, raw=data)
        except Exception as e:  # noqa: BLE001
            return Funds(equity=0.0, available_cash=0.0, used_margin=0.0, net=0.0, raw={"error": str(e)})

    def get_positions(self) -> List[Position]:
        if not self._kite:
            return []
        try:
            pos = self._kite.positions()
            combined: List[Position] = []
            for p in pos.get("day", []) + pos.get("net", []):
                exchange = Exchange[p.get("exchange", "NSE").upper()]
                quantity_total = int(p.get("quantity", 0))
                quantity_available = int(p.get("quantity", 0)) - int(p.get("overnight_quantity", 0))
                avg_price = float(p.get("average_price", 0))
                pnl = float(p.get("pnl", 0))
                combined.append(
                    Position(
                        symbol=p.get("tradingsymbol"),
                        exchange=exchange,
                        quantity_total=quantity_total,
                        quantity_available=quantity_available,
                        average_price=avg_price,
                        pnl=pnl,
                        product_type=(
                            ProductType.MARGIN
                            if p.get("product") == "NRML"
                            else (ProductType.INTRADAY if p.get("product") == "MIS" else ProductType.CNC)
                        ),
                        raw=p,
                    )
                )
            return combined
        except Exception:
            return []

    # --- Orders ---
    def place_order(self, request: OrderRequest) -> OrderResponse:
        if not self._kite:
            return OrderResponse(status="error", order_id=None, message="unauthenticated")
        try:
            order_type = M.order_type["zerodha"][request.order_type]
            product = M.product_type["zerodha"][request.product_type]
            txn_type = M.transaction_type["zerodha"][request.transaction_type]
            validity = M.validity["zerodha"][request.validity]
            if request.price <= 0:
                request.price = 0.05
            order_id = self._kite.place_order(
                variety=self._kite.VARIETY_REGULAR,
                exchange=request.exchange.value,
                tradingsymbol=request.symbol,
                transaction_type=txn_type,
                quantity=request.quantity,
                product=product,
                order_type=order_type,
                price=request.price if request.order_type == OrderType.LIMIT else None,
                validity=validity,
                trigger_price=request.stop_price,
                tag=request.tag,
            )
            resp = OrderResponse(status="ok", order_id=str(order_id), raw={"order_id": order_id})
            # Optional: immediately notify via callback that order placement succeeded
            if isinstance(resp, OrderResponse) and resp.status == "ok":
                if getattr(self, "_on_order_update_cb", None):
                    try:
                        self._on_order_update_cb(None, {"event": "order_update", "status": "ok", "order_id": str(order_id), "message": None, "raw": {"order_id": order_id}})
                    except Exception:
                        pass
                return resp
            
            if isinstance(resp, OrderResponse) and resp.status == "error":
                return OrderResponse(status="error", order_id=str(resp.order_id), raw=resp.to_dict() if isinstance(resp, OrderResponse) else None)
            
            return OrderResponse(status="error", order_id=-1, message=str(resp), raw=resp.to_dict() if isinstance(resp, OrderResponse) else None)
        except Exception as e:  # noqa: BLE001
            # Emit synthetic order error update to mimic broker event stream for testing
            if getattr(self, "_on_order_update_cb", None):
                try:
                    self._on_order_update_cb(None, {"event": "order_update", "status": "error", "order_id": None, "message": str(e)})
                except Exception:
                    pass
            return OrderResponse(status="error", order_id=None, message=str(e))

    def cancel_order(self, order_id: str) -> OrderResponse:
        if not self._kite:
            return OrderResponse(status="error", order_id=order_id, message="unauthenticated")
        try:
            resp = self._kite.cancel_order(variety=self._kite.VARIETY_REGULAR, order_id=order_id)
            return OrderResponse(status="ok", order_id=str(order_id), raw=resp)
        except Exception as e:  # noqa: BLE001
            return OrderResponse(status="error", order_id=str(order_id), message=str(e))

    def modify_order(self, order_id: str, updates: Dict[str, Any]) -> OrderResponse:
        if not self._kite:
            return OrderResponse(status="error", order_id=order_id, message="unauthenticated")
        try:
            resp = self._kite.modify_order(variety=self._kite.VARIETY_REGULAR, order_id=order_id, **updates)
            return OrderResponse(status="ok", order_id=str(order_id), raw=resp)
        except Exception as e:  # noqa: BLE001
            return OrderResponse(status="error", order_id=str(order_id), message=str(e))

    def get_orderbook(self) -> List[Dict[str, Any]]:
        if not self._kite:
            return []
        try:
            return self._kite.orders()
        except Exception:
            return []

    def get_tradebook(self) -> List[Dict[str, Any]]:
        if not self._kite:
            return []
        try:
            return self._kite.trades()
        except Exception:
            return []

    # --- Market data ---
    def get_quote(self, symbol: str) -> Quote:
        if not self._kite:
            return Quote(symbol=symbol.split(":", 1)[-1], exchange=Exchange[symbol.split(":", 1)[0]] if ":" in symbol else Exchange.NSE, last_price=0.0, raw={"error": "unauthenticated"})
        data = self._kite.quote(symbol)
        payload = next(iter(data.values()))
        last_price = float(payload.get("last_price", 0.0))
        exch, tradingsymbol = symbol.split(":", 1)
        return Quote(symbol=tradingsymbol, exchange=Exchange[exch], last_price=last_price, raw=data)

    def get_history(self, symbol: str, interval: str, start: str, end: str, oi: bool = False) -> List[Dict[str, Any]]:
        if not self._kite:
            raise PermissionError("Broker is not authenticated. Please provide valid credentials.")
        exch, tradingsymbol = symbol.split(":", 1)
        # Normalize common interval aliases to Kite format
        imap = {
            "3m": "3minute",
            "5m": "5minute",
            "10m": "10minute",
            "15m": "15minute",
            "30m": "30minute",
            "60m": "60minute",
            "1d": "day",
            "day": "day",
        }
        key = interval.strip().lower()
        interval_kite = imap.get(key, None)

        if interval_kite is None:
            raise Exception(f"Invalid interval: {interval}")
        try:
            try:
                instruments = self._kite.instruments(exch)
            except Exception:
                instruments = self._kite.instruments()
            token = None
            for inst in instruments:
                if inst.get("exchange") == exch and inst.get("tradingsymbol") == tradingsymbol:
                    token = inst.get("instrument_token")
                    break
            if token is None and exch == "NSE":
                for inst in self._kite.instruments("NFO"):
                    if inst.get("tradingsymbol") == tradingsymbol:
                        token = inst.get("instrument_token")
                        break
            if token is None:
                return []
            data = self._kite.historical_data(token, from_date=start, to_date=end, interval=interval_kite)
            # Normalize to [{ts, open, high, low, close, volume}]
            out: List[Dict[str, Any]] = []
            for c in data or []:
                try:
                    dt = c.get("date")
                    ts = int(getattr(dt, "timestamp", lambda: None)()) if dt is not None else None
                    if ts is None:
                        # Attempt to coerce using pandas-like to_pydatetime if present
                        ts = int(dt.to_pydatetime().timestamp()) if hasattr(dt, "to_pydatetime") else None
                except Exception:
                    ts = None
                out.append({
                    "ts": ts,
                    "open": float(c.get("open", 0.0)),
                    "high": float(c.get("high", 0.0)),
                    "low": float(c.get("low", 0.0)),
                    "close": float(c.get("close", 0.0)),
                    "volume": int(c.get("volume", 0)) if c.get("volume") is not None else None,
                })
            return out
        except Exception as e:
            print(f"Error getting history: {e}")
            return []

    # --- Instruments ---
    def download_instruments(self) -> None:
        df = pd.DataFrame(self._kite.instruments())
        columns = ["instrument_token", "exchange_token", "tradingsymbol", "name", "last_price", "expiry", "strike", "tick_size", "lot_size", "instrument_type", "segment", "exchange"]
        header_mapping = {
            "instrument_token": "token",
            "exchange_token": "exchange_token",
            "tradingsymbol": "symbol",
            "name": "name",
            "last_price": "last_price",
            "expiry": "expiry",
            "strike": "strike",
            "tick_size": "tick_size",
            "lot_size": "lot_size",
            "instrument_type": "instrument_type",
            "segment": "segment",
            "exchange": "exchange"
        }
        df = df[columns]
        df.columns = list(header_mapping.values())
        df['expiry'] = pd.to_datetime(df['expiry']).dt.date
        df['days_to_expiry'] = df['expiry'].apply(lambda x: np.busday_count(datetime.now().date(), x) + 1 if not pd.isna(x) else np.nan)
        self.master_contract_df = df
        self.cache_file = ".cache/zerodha_master_contract.csv"
        if not os.path.exists(os.path.dirname(self.cache_file)):
            os.makedirs(os.path.dirname(self.cache_file))
        df.to_csv(self.cache_file, index=False)
        return df

    def get_instruments(self) -> List[Instrument]:
        return self.master_contract_df

    # --- Option chain ---
    def get_option_chain(self, underlying: str, exchange: str, **kwargs: Any) -> List[Dict[str, Any]]:
        if not self._kite:
            return []
        # Accept either raw underlying name or EXCH:UNDERLYING
        if ":" in underlying:
            _, underlying_name = underlying.split(":", 1)
        else:
            underlying_name = underlying
        try:
            instruments = self._kite.instruments(exchange)
        except Exception:
            instruments = []
        out = [
            i
            for i in instruments
            if i.get("name") == underlying_name and i.get("segment") in ("NFO-OPT", "BFO-OPT")
        ]
        return out

    # --- WS ---
    def connect_websocket(
        self,
        *,
        on_ticks: Any | None = None,
        on_connect: Any | None = None,
        on_error: Any | None = None,
        on_close: Any | None = None,
        on_reconnect: Any | None = None,
        on_noreconnect: Any | None = None,
    ) -> None:
        if not self._kite:
            return
        try:  # pragma: no cover - external package
            from kiteconnect import KiteTicker  # type: ignore

            # Obtain existing tokens from client
            api_key = getattr(self._kite, "api_key", None)
            access_token = getattr(self._kite, "access_token", None) or getattr(self._kite, "_access_token", None)
            if not (api_key and access_token):
                return
            ws = KiteTicker(api_key=api_key, access_token=access_token)
            # Assign callbacks if provided
            if on_ticks is not None:
                ws.on_ticks = on_ticks
            if on_connect is not None:
                ws.on_connect = on_connect
            if on_error is not None:
                ws.on_error = on_error
            if on_close is not None:
                ws.on_close = on_close
            if on_reconnect is not None and hasattr(ws, "on_reconnect"):
                ws.on_reconnect = on_reconnect
            if on_noreconnect is not None and hasattr(ws, "on_noreconnect"):
                ws.on_noreconnect = on_noreconnect
            self._kite_ws = ws
            ws.connect(threaded=True)
        except Exception:
            return

    def symbols_to_subscribe(self, symbols: List[str]) -> None:  # type: ignore[override]
        # Zerodha expects instrument tokens. We need to map EXCH:SYMBOL to tokens using instruments API.
        if not self._kite_ws or not self._kite:
            return
        try:
            instruments = []
            try:
                instruments = self._kite.instruments()
            except Exception:
                instruments = []
            index: Dict[str, int] = {}
            for inst in instruments:
                key = f"{inst.get('exchange')}:{inst.get('tradingsymbol')}"
                tok = inst.get("instrument_token")
                if key and tok is not None:
                    index[key] = int(tok)
            tokens: List[int] = []
            for s in symbols:
                if isinstance(s, int):
                    tokens.append(int(s))
                elif isinstance(s, str) and ":" in s:
                    tok = index.get(s)
                    if tok is not None:
                        tokens.append(int(tok))
            if tokens:
                self._kite_ws.subscribe(tokens)
                if hasattr(self._kite_ws, "set_mode"):
                    self._kite_ws.set_mode(self._kite_ws.MODE_FULL, tokens)
        except Exception:
            return

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
        # KiteTicker uses on_order_update on the same socket
        # If data websocket already connected, attach order update callback
        ws = getattr(self, "_kite_ws", None)
        if on_order_update is not None:
            setattr(self, "_on_order_update_cb", on_order_update)
        if ws is None:
            # If not connected, attempt a connection with provided callbacks
            self.connect_websocket(on_ticks=None, on_connect=on_connect, on_error=on_error, on_close=on_close)
            ws = getattr(self, "_kite_ws", None)
        if ws is not None and on_order_update is not None and hasattr(ws, "on_order_update"):
            try:
                ws.on_order_update = on_order_update
            except Exception:
                pass

    def unsubscribe(self, symbols: List[str]) -> None:  # type: ignore[override]
        return None

    # --- Margins ---
    def get_margins_required(self, orders: List[Dict[str, Any]] | List[OrderRequest]) -> Any:
        if not self._kite:
            raise MarginUnavailableError("Zerodha margins unavailable: unauthenticated")
        try:
            payload: List[Dict[str, Any]] = []
            for o in orders:
                if isinstance(o, OrderRequest):
                    payload.append(
                        {
                            "exchange": o.exchange.value,
                            "tradingsymbol": o.symbol,
                            "transaction_type": M.transaction_type["zerodha"][o.transaction_type],
                            "variety": "regular",
                            "product": M.product_type["zerodha"][o.product_type],
                            "order_type": M.order_type["zerodha"][o.order_type],
                            "quantity": int(o.quantity),
                            "price": float(o.price) if o.price is not None else None,
                            "trigger_price": float(o.stop_price) if o.stop_price is not None else None,
                        }
                    )
                else:
                    payload.append(o)
            return self._kite.order_margins(payload)
        except Exception as e:  # noqa: BLE001
            raise MarginUnavailableError(f"Zerodha order_margins failed: {e}") from e

    def get_span_margin(self, orders: List[Dict[str, Any]]) -> Any:
        return self.get_margins_required(orders)

    def get_multiorder_margin(self, orders: List[Dict[str, Any]]) -> Any:
        return self.get_margins_required(orders)

    # --- Profile ---
    def get_profile(self) -> Dict[str, Any]:
        if not self._kite:
            return {"error": "unauthenticated"}
        try:
            return self._kite.profile()
        except Exception as e:  # noqa: BLE001
            return {"error": str(e)}

    def exit_positions(self, *args: Any, **kwargs: Any) -> Any:
        raise UnsupportedOperationError("ZerodhaDriver.exit_positions not implemented yet in brokers2")

    def convert_position(self, *args: Any, **kwargs: Any) -> Any:
        raise UnsupportedOperationError("ZerodhaDriver.convert_position not implemented yet in brokers2")


