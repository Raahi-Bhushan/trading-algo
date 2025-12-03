from __future__ import annotations

from datetime import datetime
import os
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import requests

from ...core.enums import Exchange, OrderType, ProductType, TransactionType, Validity
from ...core.errors import AuthError, MarginUnavailableError, UnsupportedOperationError
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
from ...net.ratelimiter import rate_limited_fyers
from ...symbols.registry import SymbolRegistry


class FyersDriver(BrokerDriver):
    """Fyers driver using fyers_apiv3 SDK when available.

    This initial implementation provides the interface and raises clear errors for
    unimplemented methods to keep behavior explicit during rollout.
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
            supports_order_websocket=False,
            supports_master_contract=False,
            supports_option_chain=True,
            supports_gtt=False,
            supports_bracket_order=False,
            supports_cover_order=False,
            supports_multileg_order=True,
            supports_basket_orders=True,
        )
        # Attempt to wire SDK if access token is provided
        self._client_id: Optional[str] = None
        self._access_token: Optional[str] = None
        self._fyers_model = None
        self.master_contract_df = None

        # Lazy import to avoid hard dependency if not used
        import os

        self._client_id = os.getenv("BROKER_API_KEY") or os.getenv("FYERS_API_KEY")
        self._access_token = os.getenv("FYERS_ACCESS_TOKEN") or os.getenv("BROKER_ACCESS_TOKEN")

        # If no token, try login flows based on BROKER_LOGIN_MODE
        login_mode_env = (os.getenv("BROKER_LOGIN_MODE") or "auto").lower()
        if not self._access_token and self._client_id and login_mode_env in ("totp", "auto"):
            token = self._authenticate_via_totp()
            if token:
                self._access_token = token

        if self._client_id and self._access_token:
            try:  # pragma: no cover - relies on external package
                from fyers_apiv3 import fyersModel  # type: ignore

                self._fyers_model = fyersModel.FyersModel(
                    client_id=self._client_id,
                    token=self._access_token,
                    is_async=False,
                    log_path="logs",
                )
            except Exception:
                self._fyers_model = None

    def _authenticate_via_totp(self) -> Optional[str]:
        """Programmatic TOTP login to obtain Fyers access token.

        Requires env vars:
        - BROKER_ID, BROKER_TOTP_KEY, BROKER_TOTP_PIN
        - BROKER_API_KEY, BROKER_API_SECRET
        - BROKER_TOTP_REDIRECT_URI (or BROKER_TOTP_REDIDRECT_URI)
        """
        import os
        import base64
        import hashlib
        from urllib.parse import urlparse, parse_qs

        try:  # pragma: no cover - external dependence
            import requests  # type: ignore
            import pyotp  # type: ignore
        except Exception:
            return None

        def _b64(s: str) -> str:
            return base64.b64encode(str(s).encode("ascii")).decode("ascii")

        fy_id = os.getenv("BROKER_ID")
        totp_key = os.getenv("BROKER_TOTP_KEY")
        pin = os.getenv("BROKER_TOTP_PIN")
        client_id = os.getenv("BROKER_API_KEY") or os.getenv("FYERS_API_KEY")
        secret_key = os.getenv("BROKER_API_SECRET") or os.getenv("FYERS_API_SECRET")
        redirect_uri = os.getenv("BROKER_TOTP_REDIRECT_URI") or os.getenv("BROKER_TOTP_REDIDRECT_URI")
        if not all([fy_id, totp_key, pin, client_id, secret_key, redirect_uri]):
            return None

        try:
            # 1) Send login OTP
            r1 = requests.post(
                url="https://api-t2.fyers.in/vagator/v2/send_login_otp_v2",
                json={"fy_id": _b64(fy_id), "app_id": "2"},
                timeout=30,
            ).json()
            request_key = r1.get("request_key")
            if not request_key:
                return None

            # 2) Verify OTP
            r2 = requests.post(
                url="https://api-t2.fyers.in/vagator/v2/verify_otp",
                json={"request_key": request_key, "otp": pyotp.TOTP(totp_key).now()},
                timeout=30,
            ).json()
            request_key2 = r2.get("request_key")
            if not request_key2:
                return None

            # 3) Verify PIN
            ses = requests.Session()
            r3 = ses.post(
                url="https://api-t2.fyers.in/vagator/v2/verify_pin_v2",
                json={"request_key": request_key2, "identity_type": "pin", "identifier": _b64(pin)},
                timeout=30,
            ).json()
            ses.headers.update({"authorization": f"Bearer {r3.get('data', {}).get('access_token')}"})

            # 4) Get auth code
            payload3 = {
                "fyers_id": fy_id,
                "app_id": client_id[:-4],
                "redirect_uri": redirect_uri,
                "appType": "100",
                "code_challenge": "",
                "state": "None",
                "scope": "",
                "nonce": "",
                "response_type": "code",
                "create_cookie": True,
            }
            r4 = ses.post("https://api-t1.fyers.in/api/v3/token", json=payload3, timeout=30).json()
            parsed = urlparse(r4.get("Url", ""))
            query = parse_qs(parsed.query)
            auth_code = (query.get("auth_code") or [None])[0]
            if not auth_code:
                return None

            # 5) Validate auth code to get access token
            checksum_input = f"{client_id}:{secret_key}"
            app_id_hash = hashlib.sha256(checksum_input.encode("utf-8")).hexdigest()
            r5 = ses.post(
                "https://api-t1.fyers.in/api/v3/validate-authcode",
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                json={"grant_type": "authorization_code", "appIdHash": app_id_hash, "code": auth_code},
                timeout=30,
            )
            r5.raise_for_status()
            auth_data = r5.json()
            if auth_data.get("s") == "ok" and auth_data.get("access_token"):
                return auth_data.get("access_token")
            return None
        except Exception:
            return None

    # --- Helpers ---
    @staticmethod
    def _format_symbol(exchange: Exchange, tradingsymbol: str) -> str:
        sym_u = tradingsymbol.upper()
        exch = exchange
        if exchange == Exchange.NFO:
            exch = Exchange.NSE
        elif exchange == Exchange.BFO:
            exch = Exchange.BSE
        is_future = "FUT" in sym_u
        is_option = sym_u.endswith("CE") or sym_u.endswith("PE")
        is_index = sym_u.endswith("-INDEX")
        if is_future or is_option or is_index:
            if ":" not in tradingsymbol:
                return f"{exch.value}:{tradingsymbol}"
            else:
                return tradingsymbol
        if not sym_u.endswith("-EQ"):
            if ":" not in tradingsymbol:
                return f"{exch.value}:{tradingsymbol}-EQ"
            else:
                return tradingsymbol
        if ":" not in tradingsymbol:
            return f"{exch.value}:{tradingsymbol}"
        else:
            return tradingsymbol

    # --- Account ---
    def get_funds(self) -> Funds:
        if not self._fyers_model:
            return Funds(equity=0.0, available_cash=0.0, used_margin=0.0, net=0.0, raw={"s": "error", "message": "unauthenticated"})
        try:
            data = self._fyers_model.funds()
            if isinstance(data, dict) and data.get("s") == "error":
                return Funds(equity=0.0, available_cash=0.0, used_margin=0.0, net=0.0, raw=data)
            fund = (data or {}).get("fund_limit", [{}])[0]
            equity = float(fund.get("equityAmount", 0))
            available = float(fund.get("availableBalance", 0))
            used = float(fund.get("utilizedAmount", 0))
            net = equity
            return Funds(equity=equity, available_cash=available, used_margin=used, net=net, raw=data)
        except Exception as e:  # noqa: BLE001
            return Funds(equity=0.0, available_cash=0.0, used_margin=0.0, net=0.0, raw={"s": "error", "message": str(e)})

    def get_positions(self) -> List[Position]:
        if not self._fyers_model:
            return []
        try:
            data = self._fyers_model.positions()
            if not isinstance(data, dict) or data.get("s") == "error":
                return []
            container = data.get("data", data)
            raw_positions = (
                container.get("netPositions")
                or container.get("overall")
                or container.get("positionDetails")
                or []
            )
            if isinstance(raw_positions, dict):
                raw_positions = list(raw_positions.values())
            if not isinstance(raw_positions, list):
                return []
            out: List[Position] = []
            for p in raw_positions:
                if not isinstance(p, dict):
                    continue
                symbol_full = p.get("symbol", "NSE:UNKNOWN-EQ")
                try:
                    exch_str, s = symbol_full.split(":", 1)
                    exchange = Exchange[exch_str]
                except Exception:  # noqa: BLE001
                    exchange = Exchange.NSE
                    s = symbol_full
                tradingsymbol = s.replace("-EQ", "")
                quantity_total = int(p.get("qtyTraded", p.get("qty", p.get("quantity", 0))))
                quantity_available = int(p.get("netQty", p.get("quantity", quantity_total)))
                avg_price = float(p.get("avgPrice", p.get("avg", p.get("average_price", 0))))
                pnl = float(p.get("pl", 0))
                prod = (
                    ProductType.INTRADAY
                    if p.get("productType") == "INTRADAY"
                    else (ProductType.MARGIN if p.get("productType") == "MARGIN" else ProductType.CNC)
                )
                out.append(
                    Position(
                        symbol=tradingsymbol,
                        exchange=exchange,
                        quantity_total=quantity_total,
                        quantity_available=quantity_available,
                        average_price=avg_price,
                        pnl=pnl,
                        product_type=prod,
                        raw=p,
                    )
                )
            return out
        except Exception:
            return []

    # --- Orders ---
    def place_order(self, request: OrderRequest) -> OrderResponse:
        if not self._fyers_model:
            return OrderResponse(status="error", order_id=None, message="unauthenticated")
        try:
            order_type = M.order_type["fyers"][request.order_type]
            product = M.product_type["fyers"][request.product_type]
            side = M.transaction_type["fyers"][request.transaction_type]
            validity = M.validity["fyers"][request.validity]

            symbol_full = self._format_symbol(request.exchange, request.symbol)
            payload = {
                "symbol": symbol_full,
                "qty": request.quantity,
                "type": order_type,
                "side": side,
                "productType": product,
                "limitPrice": request.price or 0.0,
                "stopPrice": request.stop_price or 0.0,
                "validity": validity,
                "disclosedQty": 0,
                "offlineOrder": False,
                "orderTag": request.tag,
            }
            payload.update(request.extras or {})
            resp = self._fyers_model.place_order(payload)
            if isinstance(resp, dict) and resp.get("s") == "ok":
                result = OrderResponse(status="ok", order_id=str(resp.get("id") or resp.get("order_id")), raw=resp)
                # Emit synthetic order update to user callback if present
                if getattr(self, "_on_orders_cb", None):
                    try:
                        self._on_orders_cb({"event": "order_update", "status": "ok", "order_id": result.order_id, "raw": resp})
                    except Exception:
                        pass
                return result
            if isinstance(resp, dict) and resp.get("s") == "error":
                return OrderResponse(status="error", order_id=str(resp.get("id") or resp.get("order_id")), raw=resp if isinstance(resp, dict) else None)
            
            return OrderResponse(status="error", order_id=-1, message=str(resp), raw=resp if isinstance(resp, dict) else None)
        except Exception as e:  # noqa: BLE001
            # Emit synthetic error update
            if getattr(self, "_on_orders_cb", None):
                try:
                    self._on_orders_cb({"event": "order_update", "status": "error", "order_id": None, "message": str(e)})
                except Exception:
                    pass
            return OrderResponse(status="error", order_id=None, message=str(e))

    def cancel_order(self, order_id: str) -> OrderResponse:
        if not self._fyers_model:
            return OrderResponse(status="error", order_id=order_id, message="unauthenticated")
        try:
            resp = self._fyers_model.cancel_order({"id": order_id})
            return OrderResponse(status="ok", order_id=order_id, raw=resp if isinstance(resp, dict) else None)
        except Exception as e:  # noqa: BLE001
            return OrderResponse(status="error", order_id=order_id, message=str(e))

    def modify_order(self, order_id: str, updates: Dict[str, Any]) -> OrderResponse:
        if not self._fyers_model:
            return OrderResponse(status="error", order_id=order_id, message="unauthenticated")
        try:
            payload = {"id": order_id}
            payload.update(updates)
            resp = self._fyers_model.modify_order(payload)
            return OrderResponse(status="ok", order_id=order_id, raw=resp if isinstance(resp, dict) else None)
        except Exception as e:  # noqa: BLE001
            return OrderResponse(status="error", order_id=order_id, message=str(e))

    def get_orderbook(self) -> List[Dict[str, Any]]:
        if not self._fyers_model:
            return []
        try:
            resp = self._fyers_model.orderbook()
            if isinstance(resp, dict):
                return resp.get("orderBook", []) or resp.get("orderbook", []) or []
            return []
        except Exception:
            return []

    def get_order_status(self, order_id: str) -> str:
        if not self._fyers_model:
            return "UNKNOWN"
        try:
            order_book = self.get_orderbook()
            for order in order_book:
                if order.get("id") == order_id:
                    return order.get("status")
            return "UNKNOWN"
        except Exception:
            return "UNKNOWN"

    def get_tradebook(self) -> List[Dict[str, Any]]:
        if not self._fyers_model:
            return []
        try:
            resp = self._fyers_model.tradebook()
            if isinstance(resp, dict):
                return resp.get("tradeBook", []) or resp.get("tradebook", []) or []
            return []
        except Exception:
            return []

    # --- Market data ---
    def get_quote(self, symbol: str) -> Quote:
        if not self._fyers_model:
            return Quote(symbol=symbol.split(":", 1)[-1].replace("-EQ", ""), exchange=Exchange.NSE, last_price=0.0, raw={"s": "error", "message": "unauthenticated"})
        # Accept either full EXCH:SYM or plain symbol; ensure Fyers format
        if ":" in symbol:
            exch_str, sym = symbol.split(":", 1)
            full = self._format_symbol(Exchange[exch_str], sym.replace("-EQ", ""))
            exchange = Exchange[exch_str]
        else:
            exchange = Exchange.NSE
            full = self._format_symbol(exchange, symbol)
        try:
            resp = self._fyers_model.quotes({"symbols": full})
            payload = (resp or {}).get("d", [{}])[0].get("v", {})
            last_price = float(payload.get("lp", 0.0))
        except Exception:
            last_price = 0.0
            resp = {"s": "error"}
        return Quote(symbol=full.split(":", 1)[1].replace("-EQ", ""), exchange=exchange, last_price=last_price, raw=resp if isinstance(resp, dict) else None)

    def get_quotes(self, symbols: List[str]) -> Dict[str, Quote]:  # type: ignore[override]
        if not self._fyers_model:
            return {}
        # Ensure each is fully qualified for Fyers
        fulls: List[str] = []
        for s in symbols:
            if ":" in s:
                ex, sym = s.split(":", 1)
                fulls.append(self._format_symbol(Exchange[ex], sym.replace("-EQ", "")))
            else:
                fulls.append(self._format_symbol(Exchange.NSE, s))
        data = {"symbols": ",".join(fulls)}
        try:
            resp = self._fyers_model.quotes(data)
        except Exception:
            return {}
        out: Dict[str, Quote] = {}
        try:
            for item in (resp or {}).get("d", []):
                sym = item.get("n")
                payload = item.get("v", {})
                last_price = float(payload.get("lp", 0.0))
                exch, tsym = sym.split(":", 1)
                out[sym] = Quote(symbol=tsym.replace("-EQ", ""), exchange=Exchange[exch], last_price=last_price, raw=item)
        except Exception:
            return out
        return out

    def get_history(self, symbol: str, interval: str, start: str, end: str, oi: bool = False) -> List[Dict[str, Any]]:
        if not self._fyers_model:
            return []
        interval_map = {
            "1m": "1",
            "3m": "3",
            "5m": "5",
            "10m": "10",
            "15m": "15",
            "30m": "30",
            "60m": "60",
            "day": "D",
            "1d": "D",
        }
        if ":" in symbol:
            exch_str, sym = symbol.split(":", 1)
            exchange = Exchange[exch_str]
            full = self._format_symbol(exchange, sym.replace("-EQ", ""))
        else:
            exchange = Exchange.NSE
            full = self._format_symbol(exchange, symbol)
        res = interval_map.get(interval, interval)
        try:
            payload = {
                "symbol": full,
                "resolution": res,
                "date_format": "1",
                "range_from": start,
                "range_to": end,
                "cont_flag": "1",
                "oi_flag": "1" if oi else "0",
            }
            resp = self._fyers_model.history(payload)
            if not (isinstance(resp, dict) and resp.get("s") == "ok"):
                return []
            raw = resp.get("candles", [])
            out: List[Dict[str, Any]] = []
            for c in raw:
                # Expect [ts, o, h, l, c, v]
                if not isinstance(c, (list, tuple)) or len(c) < 5:
                    continue
                try:
                    ts = int(c[0])
                except Exception:
                    # Fallback: skip if timestamp invalid
                    continue
                o = float(c[1])
                h = float(c[2])
                l = float(c[3])
                cl = float(c[4])
                vol = int(c[5]) if len(c) > 5 and c[5] is not None else None
                oi = int(c[6]) if len(c) > 6 and c[6] is not None else None
                out.append({
                    "ts": ts,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": cl,
                    "volume": vol,
                    "oi": oi,
                })
            return out
        except Exception as E:
            return []

    # --- Instruments ---
    def download_instruments(self) -> None:
        self.master_contract_urls = [
            "https://public.fyers.in/sym_details/NSE_FO.csv", 
            "https://public.fyers.in/sym_details/BSE_FO.csv", 
            "https://public.fyers.in/sym_details/NSE_CD.csv", 
            "https://public.fyers.in/sym_details/NSE_COM.csv", 
            "https://public.fyers.in/sym_details/NSE_CM.csv", 
            "https://public.fyers.in/sym_details/BSE_CM.csv", 
            "https://public.fyers.in/sym_details/MCX_COM.csv"
            ]
        self.master_contract_df = None
        self.cache_file = ".cache/fyers_master_contract.csv"
        
        # Instrument type mapping
        self.instrument_types = {
            14: "INDEX",  # Index instruments
            15: "STOCK"   # Stock instruments
        }
        response_text = ""
        # Download the CSV file
        for url in self.master_contract_urls:
            response_temp = requests.get(url, timeout=30)
            response_temp.raise_for_status()
            response_text += response_temp.text

        # Save to file
        if not os.path.exists(os.path.dirname(self.cache_file)):
            os.makedirs(os.path.dirname(self.cache_file))
            
        with open(self.cache_file, 'w') as f:
            f.write(response_text)
        
        # Define column headers
        headers = [
            "Fytoken", "Symbol Details", "Exchange Instrument type", "Minimum lot size",
            "Tick size", "ISIN", "Trading Session", "Last update date", "Expiry date",
            "Symbol ticker", "Exchange", "Segment", "Scrip code", "Underlying symbol",
            "Underlying scrip code", "Strike price", "Option type", "Underlying FyToken",
            "Reserved column1", "Reserved column2", "Reserved column3"
        ]
        
        header_mapping = {
            "Fytoken": "token",
            "Symbol Details": "symbol_details",
            "Exchange Instrument type": "instrument_type",
            "Minimum lot size": "lot_size",
            "Tick size": "tick_size",
            "ISIN": "isin",
            "Trading Session": "trading_session",
            "Last update date": "last_update_date",
            "Expiry date": "expiry",
            "Symbol ticker": "symbol",
            "Exchange": "exchange",
            "Strike price": "strike",
            "Segment": "segment",
            "Scrip code": "scrip_code",
            "Underlying symbol": "underlying_symbol"
        }

        # Read as DataFrame with headers
        df = pd.read_csv(self.cache_file, names=headers, header=None)
        df = df[header_mapping.keys()]

        df.columns = header_mapping.values()
        df['instrument_type'] = df['instrument_type'].map(self.instrument_types) # MAPPING TO STOCK/INDEX TODO - NOT USED MAYBE
        df['instrument_type'] = df['symbol'].apply(lambda x: 'FUT' if x.endswith("FUT") else 'CE' if x.endswith("CE") else 'PE' if x.endswith("PE") else 'EQ')
        df['expiry'] = pd.to_datetime(df['expiry'], unit='s', errors='coerce')
        df['expiry'] = df['expiry'].apply(lambda x: pd.to_datetime(x).date() if not pd.isna(x) else np.nan)
        df['days_to_expiry'] = df['expiry'].apply(lambda x: np.busday_count(datetime.now().date(), x) + 1 if not pd.isna(x) else np.nan)
        # Updating Segment matching to match with what we have in the zerodha
        def segment_mapping(x):
            if x.endswith("FUT"):
                if x.startswith("NSE"):
                    return "NFO-FUT"
                elif x.startswith("BSE"):
                    return "BFO-FUT"
            elif x.endswith("CE") or x.endswith("PE"):
                if x.startswith("NSE"):
                    return "NFO-OPT"
                elif x.startswith("BSE"):
                    return "BFO-OPT"
            else:
                if x.startswith("NSE"):
                    return "NSE"
                elif x.startswith("BSE"):
                    return "BSE"
        df['segment'] = df['symbol'].apply(segment_mapping)
        df.to_csv(self.cache_file, index=False)
        self.master_contract_df = df

    def get_instruments(self) -> List[Instrument]:
        return self.master_contract_df

    def get_nse_futures_symbols(self) -> List[str]:
        if self.master_contract_df is None:
            self.download_instruments()

        futures_df = self.master_contract_df[self.master_contract_df['segment'] == 'NFO-FUT']
        return futures_df['symbol'].tolist()

    # --- Option chain ---
    def get_option_chain(self, underlying: str, exchange: str, **kwargs: Any) -> List[Dict[str, Any]]:
        if not self._fyers_model:
            return []
        # Build proper Fyers symbol: ensure single EXCH:SYMBOL and append -EQ for equities
        if ":" in underlying:
            symbol_full = underlying
        else:
            symbol_full = f"{exchange}:{underlying}"
        exch_part, sym_part = symbol_full.split(":", 1)
        sym_u = sym_part.upper()
        # Map indices if user passed human form
        index_map = {
            "NIFTY 50": "NIFTY50-INDEX",
            "NIFTY BANK": "NIFTYBANK-INDEX",
            "FINNIFTY": "FINNIFTY-INDEX",
            "SENSEX": "SENSEX-INDEX",
            "BANKEX": "BANKEX-INDEX",
        }
        if sym_u in index_map:
            sym_part = index_map[sym_u]
        elif not (sym_u.endswith("CE") or sym_u.endswith("PE") or "FUT" in sym_u or sym_u.endswith("-INDEX")):
            # Equity underlying
            if not sym_u.endswith("-EQ"):
                sym_part = f"{sym_part}-EQ"
        data = {"symbol": f"{exch_part}:{sym_part}"}
        ts = kwargs.get("timestamp")
        if ts:
            data["timestamp"] = str(ts)
        strikecount = int(kwargs.get("strikecount", 5))
        try:
            resp = self._fyers_model.optionchain({**data, "strikecount": strikecount})
            return resp if isinstance(resp, list) else resp
        except Exception:
            return []

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
        if not (self._client_id and self._access_token):
            return
        try:  # pragma: no cover - external package
            from fyers_apiv3.FyersWebsocket import data_ws  # type: ignore

            def _on_connect():
                if callable(on_connect):
                    try:
                        on_connect(None)
                    except Exception:
                        pass

            def _on_close(msg):
                if callable(on_close):
                    try:
                        on_close(None, None, msg)
                    except Exception:
                        pass

            def _on_message(message):
                # Fyers sends dicts and lists; invoke callback consistently
                if callable(on_ticks):
                    try:
                        on_ticks(None, message)
                    except Exception:
                        pass
                else:
                    # Default sample print to indicate activity
                    try:
                        print("Fyers WS msg:", str(message)[:120])
                    except Exception:
                        pass

            ws = data_ws.FyersDataSocket(
                access_token=self._access_token,
                log_path="logs",
                litemode=False,
                write_to_file=False,
                reconnect=True,
                on_connect=_on_connect,
                on_close=_on_close,
                on_message=_on_message,
            )
            self._ws = ws
            ws.connect()
        except Exception:
            return

    def symbols_to_subscribe(self, symbols: List[str]) -> None:  # type: ignore[override]
        # Fyers expects formatted symbols. Gateway already resolved to broker symbols.
        ws = getattr(self, "_ws", None)
        if ws is None:
            return
        try:
            ws.subscribe(symbols=symbols, data_type="SymbolUpdate", channel=15)
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
        if not (self._client_id and self._access_token):
            return
        try:  # pragma: no cover - external package
            from fyers_apiv3.FyersWebsocket import order_ws  # type: ignore

            ws_token = (
                self._access_token if ":" in str(self._access_token) else f"{self._client_id}:{self._access_token}"
            )

            def _on_open():
                try:
                    for dt in ("OnOrders", "OnTrades", "OnPositions", "OnGeneral"):
                        self._order_ws.subscribe(data_type=dt)
                    self._order_ws.keep_running()
                except Exception:
                    pass

            # Normalize orders callback signature to (ws, message)
            def _orders_wrapper(message):
                if on_order_update is None:
                    return
                try:
                    on_order_update(None, message)
                except TypeError:
                    try:
                        on_order_update(message)
                    except Exception:
                        pass
                except Exception:
                    pass

            self._order_ws = order_ws.FyersOrderSocket(
                access_token=ws_token,
                write_to_file=False,
                log_path="",
                on_trades=on_trades,
                on_positions=on_positions,
                on_orders=_orders_wrapper,
                on_general=on_general,
                on_error=on_error or (lambda m: None),
                on_connect=_on_open if on_connect is None else on_connect,
                on_close=on_close or (lambda m: None),
            )
            self._order_ws.connect()
            # Store callback for synthetic events when placing orders via REST
            if on_order_update is not None:
                try:
                    setattr(self, "_on_orders_cb", _orders_wrapper)
                except Exception:
                    pass
        except Exception:
            return

    def unsubscribe(self, symbols: List[str]) -> None:  # type: ignore[override]
        ws = getattr(self, "_ws", None)
        if ws is None:
            return
        try:
            ws.unsubscribe(symbols=symbols, data_type="SymbolUpdate", channel=15)
        except Exception:
            return

    # --- Margins ---
    def get_margins_required(self, orders: List[Dict[str, Any]] | List[OrderRequest]) -> Any:
        # Policy: must fetch from broker; try HTTP endpoint if SDK lacks method
        # Sanitize payload to Fyers expected shape
        sanitized: List[Dict[str, Any]] = []
        for o in orders:
            if isinstance(o, OrderRequest):
                # Map standardized OrderRequest to Fyers multiorder payload
                symbol_full = self._format_symbol(o.exchange, o.symbol)
                sanitized.append(
                    {
                        "symbol": symbol_full,
                        "qty": int(o.quantity),
                        "side": int(M.transaction_type["fyers"][o.transaction_type]),
                        "type": int(M.order_type["fyers"][o.order_type]),
                        "productType": str(M.product_type["fyers"][o.product_type]),
                        "limitPrice": float(o.price or 0.0),
                        "stopLoss": float((o.extras or {}).get("stopLoss", 0.0)),
                        "stopPrice": float(o.stop_price or 0.0),
                        "takeProfit": float((o.extras or {}).get("takeProfit", 0.0)),
                        "validity": str(M.validity["fyers"][o.validity]),
                        "disclosedQty": int((o.extras or {}).get("disclosedQty", 0)),
                    }
                )
            else:
                # Dict path
                symbol = o.get("symbol")
                if isinstance(symbol, str) and ":" in symbol:
                    exch_str, sym = symbol.split(":", 1)
                    symbol = self._format_symbol(Exchange[exch_str], sym.replace("-EQ", ""))
                is_future = isinstance(symbol, str) and "FUT" in symbol.upper()
                sanitized.append(
                    {
                        "symbol": symbol,
                        "qty": int(o.get("qty") or o.get("quantity") or 1),
                        "side": int(o.get("side", 1)),
                        "type": int(o.get("type", 2)),
                        "productType": o.get("productType") or ("MARGIN" if is_future else "INTRADAY"),
                        "limitPrice": float(o.get("limitPrice", 0.0)),
                        "stopLoss": float(o.get("stopLoss", 0.0)),
                        "stopPrice": float(o.get("stopPrice", 0.0)),
                        "takeProfit": float(o.get("takeProfit", 0.0)),
                        "validity": o.get("validity", "DAY"),
                        "disclosedQty": int(o.get("disclosedQty", 0)),
                    }
                )
        # Prefer SDK method if present (not exposed in v3), then HTTP
        try:
            if getattr(self, "_access_token", None) and getattr(self, "_client_id", None):
                from ...net.http import post_json

                url = "https://api-t1.fyers.in/api/v3/multiorder/margin"
                headers = {
                    "Authorization": f"{self._client_id}:{self._access_token}",
                    "Content-Type": "application/json",
                }
                resp = post_json(url, headers=headers, json={"data": sanitized})
                return resp
        except Exception as e:  # noqa: BLE001
            raise MarginUnavailableError(f"Fyers margins failed: {e}") from e
        raise MarginUnavailableError("Fyers margins unavailable")

    def get_span_margin(self, orders: List[Dict[str, Any]]) -> Any:
        # Accept standardized OrderRequest or legacy dicts
        sanitized: List[Dict[str, Any]] = []
        for o in orders:
            if isinstance(o, OrderRequest):
                symbol_full = self._format_symbol(o.exchange, o.symbol)
                sanitized.append(
                    {
                        "symbol": symbol_full,
                        "qty": int(o.quantity),
                        "side": int(M.transaction_type["fyers"][o.transaction_type]),
                        "type": int(M.order_type["fyers"][o.order_type]),
                        "productType": str(M.product_type["fyers"][o.product_type]),
                        "limitPrice": float(o.price or 0.0),
                        "stopLoss": float((o.extras or {}).get("stopLoss", 0.0)),
                    }
                )
            else:
                symbol = o.get("symbol")
                if isinstance(symbol, str) and ":" in symbol:
                    exch_str, sym = symbol.split(":", 1)
                    symbol = self._format_symbol(Exchange[exch_str], sym.replace("-EQ", ""))
                sanitized.append(
                    {
                        "symbol": symbol,
                        "qty": int(o.get("qty") or o.get("quantity") or 1),
                        "side": int(o.get("side", 1)),
                        "type": int(o.get("type", 2)),
                        "productType": o.get("productType") or "INTRADAY",
                        "limitPrice": float(o.get("limitPrice", 0.0)),
                        "stopLoss": float(o.get("stopLoss", 0.0)),
                    }
                )
        # If any order is clearly equity (ends with -EQ and not derivative), fallback to multiorder margin (broker-fetched)
        try:
            has_equity = any(
                isinstance(it.get("symbol"), str)
                and it.get("symbol").upper().endswith("-EQ")
                and ("FUT" not in it.get("symbol").upper())
                and (not it.get("symbol").upper().endswith("CE") and not it.get("symbol").upper().endswith("PE"))
                for it in sanitized
            )
        except Exception:
            has_equity = False
        if has_equity:
            return self.get_margins_required(sanitized)
        try:
            if getattr(self, "_access_token", None) and getattr(self, "_client_id", None):
                # Match legacy implementation payload formatting
                import json as _json
                import requests as _requests  # type: ignore
                url = "https://api.fyers.in/api/v2/span_margin"
                headers = {
                    "Authorization": f"{self._client_id}:{self._access_token}",
                    "Content-Type": "application/json",
                }
                resp = _requests.post(url, headers=headers, data=_json.dumps({"data": sanitized}), timeout=30)
                try:
                    resp.raise_for_status()
                    return resp.json()
                except Exception:
                    # If span margin fails (e.g., for equity legs), fallback to multiorder margin
                    return self.get_margins_required(sanitized)
        except Exception as e:  # noqa: BLE001
            raise MarginUnavailableError(f"Fyers span margins failed: {e}") from e
        raise MarginUnavailableError("Fyers span margins unavailable: unauthenticated or invalid config")

    def get_multiorder_margin(self, orders: List[Dict[str, Any]]) -> Any:
        return self.get_margins_required(orders)

    # --- Profile ---
    def get_profile(self) -> Dict[str, Any]:
        if not self._fyers_model:
            return {"s": "error", "message": "unauthenticated"}
        try:
            return self._fyers_model.get_profile()
        except Exception as e:  # noqa: BLE001
            return {"s": "error", "message": str(e)}

    def exit_positions(self, *args: Any, **kwargs: Any) -> Any:
        raise UnsupportedOperationError("FyersDriver.exit_positions not implemented yet in brokers2")

    def convert_position(self, *args: Any, **kwargs: Any) -> Any:
        raise UnsupportedOperationError("FyersDriver.convert_position not implemented yet in brokers2")

    # --- Basket orders ---
    def place_basket_orders(self, requests: List[OrderRequest]) -> List[OrderResponse]:  # type: ignore[override]
        if not self._fyers_model:
            return [OrderResponse(status="error", order_id=None, message="unauthenticated")]
        payloads: List[Dict[str, Any]] = []
        for r in requests:
            payloads.append(
                {
                    "symbol": self._format_symbol(r.exchange, r.symbol),
                    "qty": r.quantity,
                    "type": M.order_type["fyers"][r.order_type],
                    "side": M.transaction_type["fyers"][r.transaction_type],
                    "productType": M.product_type["fyers"][r.product_type],
                    "limitPrice": r.price or 0.0,
                    "stopPrice": r.stop_price or 0.0,
                    "validity": M.validity["fyers"][r.validity],
                    "disclosedQty": 0,
                    "offlineOrder": False,
                }
            )
        try:
            # If SDK supports basket
            if hasattr(self._fyers_model, "place_basket_orders"):
                resp = getattr(self._fyers_model, "place_basket_orders")(payloads)
                if isinstance(resp, dict) and resp.get("s") == "ok":
                    oid = str(resp.get("id") or resp.get("order_id"))
                    return [OrderResponse(status="ok", order_id=oid, raw=resp) for _ in payloads]
                return [OrderResponse(status="error", order_id=None, message=str(resp))]
            # Fallback: individual placement
            results: List[OrderResponse] = []
            for p in payloads:
                r = self._fyers_model.place_order(p)
                if isinstance(r, dict) and r.get("s") == "ok":
                    results.append(OrderResponse(status="ok", order_id=str(r.get("id") or r.get("order_id")), raw=r))
                else:
                    results.append(OrderResponse(status="error", order_id=None, message=str(r)))
            return results
        except Exception as e:  # noqa: BLE001
            return [OrderResponse(status="error", order_id=None, message=str(e))]


