import hashlib
import hmac
import time
import json
import uuid
from copy import copy
from datetime import datetime, timedelta
from types import TracebackType
from collections.abc import Callable
from functools import partial

from vnpy.event import EventEngine, Event, EVENT_TIMER
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Interval,
    OrderType,
    Product,
    Status,
    Offset,
    OptionType
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.utility import ZoneInfo
from vnpy.trader.object import (
    AccountData,
    BarData,
    CancelRequest,
    ContractData,
    HistoryRequest,
    OrderData,
    OrderRequest,
    PositionData,
    SubscribeRequest,
    TickData,
    TradeData
)
from vnpy_rest import Request, Response, RestClient
from vnpy_websocket import WebsocketClient


# Timezone
BYBIT_TZ: ZoneInfo = ZoneInfo("Asia/Shanghai")

# Real server hosts
REAL_REST_HOST: str = "https://api.bybit.com"
REAL_PRIVATE_WEBSOCKET_HOST: str = "wss://stream.bybit.com/v5/private"
REAL_TRADE_WEBSOCKET_HOST: str = "wss://stream.bybit.com/v5/trade"
REAL_SPOT_WEBSOCKET_HOST: str = "wss://stream.bybit.com/v5/public/spot"
REAL_LINEAR_WEBSOCKET_HOST: str = "wss://stream.bybit.com/v5/public/linear"
REAL_INVERSE_WEBSOCKET_HOST: str = "wss://stream.bybit.com/v5/public/inverse"
REAL_OPTION_WEBSOCKET_HOST: str = "wss://stream.bybit.com/v5/public/option"

# Demo server hosts
DEMO_REST_HOST: str = "https://api-demo.bybit.com"
DEMO_PRIVATE_WEBSOCKET_HOST: str = "wss://stream-demo.bybit.com/v5/private"
DEMO_TRADE_WEBSOCKET_HOST: str = "wss://stream-demo.bybit.com/v5/trade"
DEMO_SPOT_WEBSOCKET_HOST: str = "wss://stream-demo.bybit.com/v5/public/spot"
DEMO_LINEAR_WEBSOCKET_HOST: str = "wss://stream-demo.bybit.com/v5/public/linear"
DEMO_INVERSE_WEBSOCKET_HOST: str = "wss://stream-demo.bybit.com/v5/public/inverse"
DEMO_OPTION_WEBSOCKET_HOST: str = "wss://stream-demo.bybit.com/v5/public/option"

# Testnet server hosts
TESTNET_REST_HOST: str = "https://api-testnet.bybit.com"
TESTNET_PRIVATE_WEBSOCKET_HOST: str = "wss://stream-testnet.bybit.com/v5/private"
TESTNET_TRADE_WEBSOCKET_HOST: str = "wss://stream-testnet.bybit.com/v5/trade"
TESTNET_SPOT_WEBSOCKET_HOST: str = "wss://stream-testnet.bybit.com/v5/public/spot"
TESTNET_LINEAR_WEBSOCKET_HOST: str = "wss://stream-testnet.bybit.com/v5/public/linear"
TESTNET_INVERSE_WEBSOCKET_HOST: str = "wss://stream-testnet.bybit.com/v5/public/inverse"
TESTNET_OPTION_WEBSOCKET_HOST: str = "wss://stream-testnet.bybit.com/v5/public/option"

# Product type map
PRODUCT_BYBIT2VT: dict[str, Product] = {
    "spot": Product.SPOT,
    "linear": Product.SWAP,
    "inverse": Product.SWAP,
    "option": Product.OPTION,
}

# Option type map
OPTION_TYPE_BYBIT2VT: dict[str, OptionType] = {
    "Call": OptionType.CALL,
    "Put": OptionType.PUT
}

# Order status map
STATUS_BYBIT2VT: dict[str, Status] = {
    "Created": Status.NOTTRADED,
    "New": Status.NOTTRADED,
    "PartiallyFilled": Status.PARTTRADED,
    "Filled": Status.ALLTRADED,
    "Cancelled": Status.CANCELLED,
    "Rejected": Status.REJECTED,
}

# Order type map
ORDER_TYPE_VT2BYBIT: dict[OrderType, str] = {
    OrderType.LIMIT: "Limit",
    OrderType.MARKET: "Market",
}
ORDER_TYPE_BYBIT2VT: dict[str, OrderType] = {v: k for k, v in ORDER_TYPE_VT2BYBIT.items()}

# Direction map
DIRECTION_VT2BYBIT: dict[Direction, str] = {
    Direction.LONG: "Buy",
    Direction.SHORT: "Sell"
}
DIRECTION_BYBIT2VT: dict[str, Direction] = {v: k for k, v in DIRECTION_VT2BYBIT.items()}

# Interval map
INTERVAL_VT2BYBIT: dict[Interval, str] = {
    Interval.MINUTE: "1",
    Interval.HOUR: "60",
    Interval.DAILY: "D",
    Interval.WEEKLY: "W",
}
TIMEDELTA_MAP: dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
    Interval.WEEKLY: timedelta(days=7),
}

# Tick field map
TICK_FIELD_BYBIT2VT: dict[str, str] = {
    "lastPrice": "last_price",
    "highPrice24h": "high_price",
    "lowPrice24h": "low_price",
    "volume24h": "volume",
    "turnover24h": "turnover",
    "openInterest": "open_interest",
}


# Global data storage
symbol_category_map: dict[str, str] = {}


class BybitGateway(BaseGateway):
    """
    The Bybit trading gateway for VeighNa.

    Supports spot, linear, inverse and option trading
    through Bybit V5 API.
    """

    default_name: str = "BYBIT"

    default_setting: dict = {
        "API Key": "",
        "Secret Key": "",
        "Server": ["REAL", "DEMO", "TESTNET"],
        "Proxy Host": "",
        "Proxy Port": 0,
    }

    exchanges: list[Exchange] = [Exchange.GLOBAL]

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """
        The init method of the gateway.

        event_engine: the global event engine object of VeighNa
        gateway_name: the unique name for identifying the gateway
        """
        super().__init__(event_engine, gateway_name)

        self.rest_api: BybitRestApi = BybitRestApi(self)
        self.private_ws_api: BybitPrivateWebsocketApi = BybitPrivateWebsocketApi(self)
        self.public_ws_api: BybitPublicWebsocketApi = BybitPublicWebsocketApi(self)
        self.trade_ws_api: BybitTradeWebsocketApi = BybitTradeWebsocketApi(self)

        self.symbol_contract_map: dict[str, ContractData] = {}
        self.name_contract_map: dict[str, ContractData] = {}

        self.timer_count: int = 0

    def connect(self, setting: dict) -> None:
        """
        Start server connections.

        This method establishes connections to Bybit servers
        using the provided settings.

        Parameters:
            setting: A dictionary containing connection parameters including
                     API credentials, server selection, and proxy configuration.
        """
        key: str = setting["API Key"]
        secret: str = setting["Secret Key"]
        server: str = setting["Server"]
        proxy_host: str = setting["Proxy Host"]
        proxy_port: int = setting["Proxy Port"]

        self.rest_api.connect(
            key,
            secret,
            server,
            proxy_host,
            proxy_port
        )
        self.private_ws_api.connect(
            key,
            secret,
            server,
            proxy_host,
            proxy_port
        )
        self.public_ws_api.connect(
            server,
            proxy_host,
            proxy_port
        )

        # Connect trade websocket API
        self.trade_ws_api.connect(
            key,
            secret,
            server,
            proxy_host,
            proxy_port
        )

        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def subscribe(self, req: SubscribeRequest) -> None:
        """
        Subscribe to market data.

        Parameters:
            req: Subscription request object containing symbol information
        """
        self.public_ws_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """
        Send new order to Bybit.

        This function delegates order placement to the trade websocket API
        if connected, otherwise falls back to the REST API.

        Parameters:
            req: Order request object containing order details

        Returns:
            str: The VeighNa order ID if successful, empty string otherwise
        """
        # Send order via websocket API if connected, otherwise use REST API
        if self.trade_ws_api.is_connected:
            return self.trade_ws_api.send_order(req)
        else:
            return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """
        Cancel existing order on Bybit.

        This function delegates order cancellation to the trade websocket API
        if connected, otherwise falls back to the REST API.

        Parameters:
            req: Cancel request object containing order details
        """
        # Cancel order via websocket API if connected, otherwise use REST API
        if self.trade_ws_api.is_connected:
            self.trade_ws_api.cancel_order(req)
        else:
            self.rest_api.cancel_order(req)

    def query_account(self) -> None:
        """
        Query account balance.

        This method is not implemented because Bybit provides account balance
        updates through the websocket API.
        """
        pass

    def query_position(self) -> None:
        """
        Query asset positions.

        This method is not implemented because Bybit provides position updates
        through the websocket API.
        """
        pass

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """
        Query historical kline data.

        Parameters:
            req: History request object containing query parameters

        Returns:
            list[BarData]: List of historical kline data bars
        """
        return self.rest_api.query_history(req)

    def on_contract(self, contract: ContractData) -> None:
        """
        Cache contract data and push a contract event.

        Parameters:
            contract: Contract data object
        """
        self.symbol_contract_map[contract.symbol] = contract

        category: str = symbol_category_map.get(contract.symbol, "")
        self.name_contract_map[f"{category}:{contract.name}"] = contract

        super().on_contract(contract)

    def get_contract_by_symbol(self, symbol: str) -> ContractData | None:
        """
        Get contract by VeighNa symbol.

        Parameters:
            symbol: The VeighNa symbol of the contract

        Returns:
            ContractData: Contract data object if found, None otherwise
        """
        return self.symbol_contract_map.get(symbol, None)

    def get_contract_by_name(self, name: str, category: str) -> ContractData | None:
        """
        Get contract by exchange symbol name and category.

        Parameters:
            name: The exchange name of the contract
            category: The product category (spot, linear, inverse, option)

        Returns:
            ContractData: Contract data object if found, None otherwise
        """
        return self.name_contract_map.get(f"{category}:{name}", None)

    def close(self) -> None:
        """
        Close server connections.

        This method stops all API connections and releases resources.
        """
        self.rest_api.stop()
        self.private_ws_api.stop()
        self.public_ws_api.stop()
        self.trade_ws_api.stop()

    def process_timer_event(self, event: Event) -> None:
        """
        Process timer events for sending heartbeat messages.

        Parameters:
            event: Timer event object
        """
        self.timer_count += 1
        if self.timer_count < 20:
            return
        self.timer_count = 0

        self.public_ws_api.send_heartbeat()
        self.private_ws_api.send_heartbeat()
        self.trade_ws_api.send_heartbeat()


class BybitRestApi(RestClient):
    """The REST API of BybitGateway"""

    def __init__(self, gateway: BybitGateway) -> None:
        """
        The init method of the api.

        Parameters:
            gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BybitGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""

        self.time_offset: int = 0
        self.order_count: int = 0

    def sign(self, request: Request) -> Request:
        """
        Standard callback for signing a request.

        This method adds the necessary authentication parameters and signature
        to requests that require API key authentication.

        Parameters:
            request: Request object to be signed

        Returns:
            Request: Modified request with authentication parameters
        """
        # Prepare payload
        parameters: dict = {}
        if request.params:
            parameters = request.params
        elif request.data:
            parameters = request.data

        req_params: str = prepare_payload(request.method, parameters)

        # Generate signature
        timestamp: int = int(time.time() * 1000) - self.time_offset
        recv_window: int = 30_000

        param_str: str = str(timestamp) + self.key + str(recv_window) + req_params
        signature: str = generate_signature(self.secret, param_str)

        # Add request headers
        request.headers = {
            "Content-Type": "application/json",
            "X-BAPI-API-KEY": self.key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": str(timestamp),
            "X-BAPI-RECV-WINDOW": str(recv_window),
        }

        if request.method != "GET":
            request.data = req_params

        return request

    def new_orderid(self) -> str:
        """
        Generate a unique local order id.

        Returns:
            str: A unique order ID in the format 'YYYYMMDD-HHMMSS-XXXXXXXX'
        """
        prefix: str = datetime.now().strftime("%Y%m%d-%H%M%S-")

        self.order_count += 1
        suffix: str = str(self.order_count).rjust(8, "0")

        orderid: str = prefix + suffix
        return orderid

    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """
        Start server connection.

        This method establishes a connection to Bybit REST API server
        using the provided credentials and configuration.

        Parameters:
            key: API Key for authentication
            secret: API Secret for request signing
            server: Server type ("REAL" or "DEMO")
            proxy_host: Proxy server hostname or IP
            proxy_port: Proxy server port
        """
        self.key = key
        self.secret = secret

        # Select server host based on environment
        server_hosts: dict[str, str] = {
            "REAL": REAL_REST_HOST,
            "DEMO": DEMO_REST_HOST,
            "TESTNET": TESTNET_REST_HOST,
        }

        host: str = server_hosts[server]
        self.init(host, proxy_host, proxy_port)

        self.start()
        self.gateway.write_log("REST API started")

        self.query_time()

    def query_time(self) -> None:
        """
        Query server time.

        This function sends a request to get the exchange server time,
        which is used to synchronize local time with server time.
        """
        self.add_request(
            "GET",
            "/v5/market/time",
            callback=self.on_query_time
        )

    def query_contract(self) -> None:
        """
        Query available contracts.

        This function sends requests to get exchange information for all
        supported product categories (spot, linear, inverse, option).
        """
        for category in ["spot", "linear", "inverse", "option"]:
            params: dict = {
                "category": category,
                "limit": 1000
            }

            self.add_request(
                "GET",
                "/v5/market/instruments-info",
                self.on_query_contract,
                params=params
            )

    def query_order(self) -> None:
        """
        Query open orders.

        This function sends requests to get all active orders
        that have not been fully filled or cancelled. For linear
        contracts, it queries both USDT and USDC settled contracts.
        """
        for category in ["spot", "linear", "inverse", "option"]:
            params: dict = {"category": category}

            if category == "linear":
                # Query linear contracts separately for USDT and USDC settlement
                for coin in ["USDT", "USDC"]:
                    params["settleCoin"] = coin

                    self.add_request(
                        "GET",
                        "/v5/order/realtime",
                        self.on_query_order,
                        params=params
                    )
            else:
                self.add_request(
                    "GET",
                    "/v5/order/realtime",
                    self.on_query_order,
                    params=params
                )

    def query_account(self) -> None:
        """
        Query account balance.

        This function sends requests to get wallet balance
        for both UNIFIED and CONTRACT account types.
        """
        params: dict = {"accountType": "UNIFIED"}

        self.add_request(
            "GET",
            "/v5/account/wallet-balance",
            self.on_query_account,
            params=params
        )

    def query_position(self) -> None:
        """
        Query holding positions.

        This function sends requests to get all open positions
        for linear, inverse, and option contracts. For linear
        contracts, it queries both USDT and USDC settled contracts.
        """
        for category in ["linear", "inverse", "option"]:
            params: dict = {
                "category": category,
                "limit": 200
            }

            if category == "linear":
                # Query linear contracts separately for USDT and USDC settlement
                for coin in ["USDT", "USDC"]:
                    params["settleCoin"] = coin

                    self.add_request(
                        "GET",
                        "/v5/position/list",
                        self.on_query_position,
                        params=params
                    )
            else:
                self.add_request(
                    "GET",
                    "/v5/position/list",
                    self.on_query_position,
                    params=params
                )

    def send_order(self, req: OrderRequest) -> str:
        """
        Send new order to Bybit via REST API.

        This function creates and sends a new order request to the exchange.
        It serves as a fallback when the trade websocket API is not connected.

        Parameters:
            req: Order request object containing order details

        Returns:
            str: The VeighNa order ID
        """
        # Generate new order id
        orderid: str = self.new_orderid()

        # Push a submitting order event
        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)

        # Look up contract for exchange name
        contract: ContractData | None = self.gateway.get_contract_by_symbol(req.symbol)
        if not contract:
            self.gateway.write_log(f"Failed to send order, symbol not found: {req.symbol}")
            order.status = Status.REJECTED
            self.gateway.on_order(order)
            return order.vt_orderid

        # Create order parameters
        data: dict = {
            "category": symbol_category_map.get(req.symbol, ""),
            "symbol": contract.name,
            "orderType": ORDER_TYPE_VT2BYBIT[req.type],
            "side": DIRECTION_VT2BYBIT[req.direction],
            "qty": str(req.volume),
            "price": str(req.price),
            "orderLinkId": orderid
        }

        # Set reduce-only flag for closing orders
        if req.offset == Offset.CLOSE:
            data["reduceOnly"] = True

        # Send request
        self.add_request(
            "POST",
            "/v5/order/create",
            data=data,
            extra=order,
            callback=self.on_send_order,
            on_failed=self.on_send_order_failed,
            on_error=self.on_send_order_error,
        )

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """
        Cancel existing order on Bybit via REST API.

        This function sends a request to cancel an existing order.
        It determines whether to use exchange order ID or client order ID
        based on the format of the order ID (UUID format has 4 dashes).

        Parameters:
            req: Cancel request object containing order details
        """
        # Look up contract for exchange name
        contract: ContractData | None = self.gateway.get_contract_by_symbol(req.symbol)
        if not contract:
            self.gateway.write_log(f"Failed to cancel order, symbol not found: {req.symbol}")
            return

        # Create cancel parameters
        data: dict = {
            "category": symbol_category_map.get(req.symbol, ""),
            "symbol": contract.name
        }

        # Determine the type of order ID to use for cancellation
        # Bybit exchange order IDs are UUIDs with 4 dashes
        dash_count: int = req.orderid.count("-")

        if dash_count == 4:
            data["orderId"] = req.orderid
        else:
            data["orderLinkId"] = req.orderid

        # Send cancel request
        self.add_request(
            "POST",
            "/v5/order/cancel",
            data=data,
            callback=self.on_cancel_order
        )

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """
        Query kline history data.

        This function sends requests to get historical kline data
        for a specific trading instrument and time period. It queries
        data iteratively until the end time is reached.

        Parameters:
            req: History request object containing query parameters

        Returns:
            list[BarData]: List of historical kline data bars
        """
        # Look up contract for exchange name
        contract: ContractData | None = self.gateway.get_contract_by_symbol(req.symbol)
        if not contract:
            self.gateway.write_log(f"Failed to query history, symbol not found: {req.symbol}")
            return []

        if not req.interval:
            self.gateway.write_log(f"Failed to query history, interval not specified: {req.symbol}")
            return []

        count: int = 1000
        start_time: int = int(req.start.timestamp()) * 1000
        category: str = symbol_category_map.get(req.symbol, "")

        buf: dict[datetime, BarData] = {}
        last_end_dt: datetime | None = None

        while True:
            # Create query params
            params: dict = {
                "category": category,
                "symbol": contract.name,
                "interval": INTERVAL_VT2BYBIT[req.interval],
                "start": start_time,
                "limit": count
            }

            # Get response from server
            resp: Response = self.request(
                "GET",
                "/v5/market/kline",
                params=params
            )

            # Break loop if request failed
            if resp.status_code // 100 != 2:
                msg: str = f"Query kline history failed, status code: {resp.status_code}, message: {resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                packet: dict = resp.json()
                result: dict = packet["result"]
                kline_data: list = result["list"]

                if not kline_data:
                    msg = f"No kline history data is received, start time: {start_time}, count: {count}"
                    self.gateway.write_log(msg)
                    break

                for row in kline_data:
                    dt: datetime = generate_datetime(int(row[0]))

                    bar: BarData = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=dt,
                        interval=req.interval,
                        volume=float(row[5]),
                        turnover=float(row[6]),
                        open_price=float(row[1]),
                        high_price=float(row[2]),
                        low_price=float(row[3]),
                        close_price=float(row[4]),
                        gateway_name=self.gateway_name
                    )
                    buf[bar.datetime] = bar

                begin: str = kline_data[-1][0]
                begin_dt: datetime = generate_datetime(int(begin))

                end: str = kline_data[0][0]
                end_dt: datetime = generate_datetime(int(end))

                msg = f"Query kline history finished, {req.symbol} - {req.interval.value}, {begin_dt} - {end_dt}"
                self.gateway.write_log(msg)

                # Break loop if all data received
                if (
                    (req.end and end_dt >= req.end)
                    or end_dt == last_end_dt
                ):
                    break
                last_end_dt = end_dt

                # Update start time
                start_time = int(end)

                # Add small delay to avoid rate limit
                time.sleep(0.01)

        index: list[datetime] = list(buf.keys())
        index.sort()

        history: list[BarData] = [buf[i] for i in index]
        return history

    def on_failed(self, status_code: int, request: Request) -> None:
        """
        General failed callback.

        This function is called when a REST API request returns a non-success
        HTTP status code. It logs the error details for troubleshooting.

        Parameters:
            status_code: HTTP status code
            request: Original request object
        """
        try:
            data: dict = request.response.json()
            error_code: int = data["retCode"]
            error_msg: str = data["retMsg"]
            msg: str = (
                f"Request failed, status code: {status_code},"
                f" error code: {error_code}, message: {error_msg}"
            )
        except Exception:
            msg = (
                f"Request failed, status code: {status_code},"
                f" response: {request.response.text}"
            )

        self.gateway.write_log(msg)

    def on_error(
        self,
        exc: type,
        value: Exception,
        tb: TracebackType,
        request: Request
    ) -> None:
        """
        General error callback.

        This function is called when an exception occurs in REST API requests.
        It logs the exception details for troubleshooting.

        Parameters:
            exc: Type of the exception
            value: Exception instance
            tb: Traceback object
            request: Original request object
        """
        detail: str = self.exception_detail(exc, value, tb, request)
        # Escape curly braces to prevent loguru from interpreting them as format placeholders
        detail = detail.replace("{", "{{").replace("}", "}}")

        msg: str = f"Exception catched by REST API: {detail}"
        self.gateway.write_log(msg)

    def on_query_time(self, packet: dict, request: Request) -> None:
        """
        Callback of server time query.

        This function processes the server time response and calculates
        the time difference between local and server time.

        Parameters:
            packet: Response data from the server
            request: Original request object
        """
        result: dict = packet["result"]

        local_time: int = int(time.time() * 1000)
        server_time: int = int(int(result["timeNano"]) / 1_000_000)
        self.time_offset = local_time - server_time

        self.gateway.write_log(f"Server time updated, local offset: {self.time_offset} ms")

        self.query_contract()
        self.query_order()
        self.query_account()
        self.query_position()

    def on_query_contract(self, data: dict, request: Request) -> None:
        """
        Callback of available contracts query.

        This function processes the exchange info response and
        creates ContractData objects for each trading instrument.

        Parameters:
            data: Response data from the server
            request: Original request object
        """
        result: dict = data["result"]

        category: str = result["category"]
        product: Product = PRODUCT_BYBIT2VT[category]

        for d in result["list"]:
            name: str = d["symbol"]
            actual_product: Product = product

            contract_type: str = d.get("contractType", "")
            if contract_type in ("LinearFutures", "InverseFutures"):
                actual_product = Product.FUTURES

            # Generate VeighNa symbol with product type suffix
            match actual_product:
                case Product.SPOT:
                    symbol: str = name + "_SPOT_BYBIT"
                case Product.SWAP:
                    symbol = name + "_SWAP_BYBIT"
                case Product.FUTURES:
                    symbol = name + "_FUTURES_BYBIT"
                case Product.OPTION:
                    symbol = name + "_OPTION_BYBIT"

            contract: ContractData = ContractData(
                symbol=symbol,
                exchange=Exchange.GLOBAL,
                name=name,
                product=actual_product,
                size=1,
                pricetick=float(d["priceFilter"]["tickSize"]),
                min_volume=float(d["lotSizeFilter"]["minOrderQty"]),
                history_data=True,
                net_position=True,
                gateway_name=self.gateway_name
            )

            # Add extra option contract fields
            if actual_product == Product.OPTION:
                buf: list = name.split("-")

                contract.option_strike = int(buf[2])
                contract.option_underlying = "-".join(buf[:2])
                contract.option_type = OPTION_TYPE_BYBIT2VT[d["optionsType"]]
                contract.option_listed = generate_datetime(int(d["launchTime"]))
                contract.option_expiry = generate_datetime(int(d["deliveryTime"]))
                contract.option_portfolio = buf[0]
                contract.option_index = str(contract.option_strike)

            # Cache symbol-to-category mapping for later use
            symbol_category_map[contract.symbol] = category

            self.gateway.on_contract(contract)

        self.gateway.write_log(f"Available {category} contracts data is received")

        # Continue fetching next page if cursor exists
        cursor: str = result.get("nextPageCursor", "")
        if cursor:
            params: dict = {
                "category": category,
                "limit": 1000,
                "cursor": cursor
            }
            self.add_request(
                "GET",
                "/v5/market/instruments-info",
                self.on_query_contract,
                params=params
            )

    def on_query_order(self, data: dict, request: Request) -> None:
        """
        Callback of open orders query.

        This function processes the open orders response and
        creates OrderData objects for each active order.

        Parameters:
            data: Response data from the server
            request: Original request object
        """
        if data["retCode"]:
            msg: str = f"Query open orders failed, code: {data['retCode']}, message: {data['retMsg']}"
            self.gateway.write_log(msg)
            return

        result: dict = data["result"]
        category: str = result["category"]

        for d in result["list"]:
            contract: ContractData | None = self.gateway.get_contract_by_name(d["symbol"], category)
            if not contract:
                continue

            order: OrderData = OrderData(
                symbol=contract.symbol,
                exchange=Exchange.GLOBAL,
                orderid=d["orderLinkId"],
                type=ORDER_TYPE_BYBIT2VT[d["orderType"]],
                direction=DIRECTION_BYBIT2VT[d["side"]],
                price=float(d["price"]),
                volume=float(d["qty"]),
                traded=float(d["cumExecQty"]),
                status=STATUS_BYBIT2VT[d["orderStatus"]],
                datetime=generate_datetime(int(d["createdTime"])),
                gateway_name=self.gateway_name
            )

            # Set order offset based on reduceOnly flag
            offset: bool = d["reduceOnly"]
            if offset:
                order.offset = Offset.CLOSE
            else:
                order.offset = Offset.OPEN

            self.gateway.on_order(order)

        self.gateway.write_log(f"{category} open orders data is received")

    def on_query_account(self, data: dict, request: Request) -> None:
        """
        Callback of account balance query.

        This function processes the account balance response and creates
        AccountData objects for each coin.

        Parameters:
            data: Response data from the server
            request: Original request object
        """
        if data["retCode"]:
            msg: str = f"Query account balance failed, code: {data['retCode']}, message: {data['retMsg']}"
            self.gateway.write_log(msg)
            return

        result: dict = data["result"]

        for d in result["list"]:
            for coin_data in d["coin"]:
                balance: float = 0
                if coin_data["walletBalance"]:
                    balance = float(coin_data["walletBalance"])

                available: float = 0
                if coin_data["availableToWithdraw"]:
                    available = float(coin_data["availableToWithdraw"])

                account: AccountData = AccountData(
                    accountid=coin_data["coin"],
                    balance=balance,
                    frozen=balance - available,
                    gateway_name=self.gateway_name,
                )
                self.gateway.on_account(account)

    def on_query_position(self, data: dict, request: Request) -> None:
        """
        Callback of holding positions query.

        This function processes the position response and creates
        PositionData objects for each holding position.

        Parameters:
            data: Response data from the server
            request: Original request object
        """
        if data["retCode"]:
            msg: str = f"Query holding position failed, code: {data['retCode']}, message: {data['retMsg']}"
            self.gateway.write_log(msg)
            return

        result: dict = data["result"]
        category: str = result["category"]

        for d in result["list"]:
            contract: ContractData | None = self.gateway.get_contract_by_name(d["symbol"], category)
            if not contract:
                continue

            # Determine position volume and direction from side field
            volume: float = 0
            if d["side"] == "Buy":
                volume = float(d["size"])
            elif d["side"] == "Sell":
                volume = -float(d["size"])

            position: PositionData = PositionData(
                symbol=contract.symbol,
                exchange=Exchange.GLOBAL,
                direction=Direction.NET,
                volume=volume,
                price=float(d["avgPrice"]),
                gateway_name=self.gateway_name
            )
            self.gateway.on_position(position)

    def on_send_order_failed(self, status_code: int, request: Request) -> None:
        """
        Failed callback of send_order.

        This function is called when the order placement HTTP request
        returns a non-success status code.

        Parameters:
            status_code: HTTP status code
            request: Original request object
        """
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        msg: str = f"Send order failed, code: {status_code}"
        self.gateway.write_log(msg)

    def on_send_order_error(
        self,
        exc: type,
        value: Exception,
        tb: TracebackType,
        request: Request
    ) -> None:
        """
        Error callback of send_order.

        This function is called when an exception occurs during order placement.
        It marks the order as rejected and logs the error.

        Parameters:
            exc: Type of the exception
            value: Exception instance
            tb: Traceback object
            request: Original request object
        """
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        msg: str = f"Send order error, exception type: {exc}, exception value: {value}"
        self.gateway.write_log(msg)

        self.on_error(exc, value, tb, request)

    def on_send_order(self, data: dict, request: Request) -> None:
        """
        Successful callback of send order.

        This function processes the order creation response.
        If the return code indicates an error, the order is rejected.

        Parameters:
            data: Response data from the server
            request: Original request object
        """
        if data["retCode"]:
            msg: str = f"Send order failed, code: {data['retCode']}, message: {data['retMsg']}"
            self.gateway.write_log(msg)

            order: OrderData = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)

    def on_cancel_order(self, data: dict, request: Request) -> None:
        """
        Successful callback of cancel order.

        This function processes the order cancellation response.
        If the return code indicates an error, the failure is logged.

        Parameters:
            data: Response data from the server
            request: Original request object
        """
        if data["retCode"]:
            msg: str = f"Cancel order failed, code: {data['retCode']}, message: {data['retMsg']}"
            self.gateway.write_log(msg)


class BybitPublicWebsocketApi:
    """The public websocket API of BybitGateway"""

    def __init__(self, gateway: BybitGateway) -> None:
        """
        The init method of the api.

        Parameters:
            gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BybitGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.server: str = ""
        self.proxy_host: str = ""
        self.proxy_port: int = 0

        self.clients: dict[str, WebsocketClient] = {}
        self.ticks: dict[str, TickData] = {}
        self.subscribed: dict[str, SubscribeRequest] = {}

        self.callbacks: dict[str, Callable] = {
            "ping": self.on_heartbeat,
            "pong": self.on_heartbeat
        }

    def connect(
        self,
        server: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """
        Start server connection.

        This method stores connection parameters for lazy initialization
        of websocket clients when subscriptions are made.

        Parameters:
            server: Server type ("REAL" or "DEMO")
            proxy_host: Proxy server hostname or IP
            proxy_port: Proxy server port
        """
        self.server = server
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port

    def stop(self) -> None:
        """
        Close all websocket connections.

        This method stops all category-specific websocket clients.
        """
        for client in self.clients.values():
            client.stop()

    def get_client(self, category: str) -> WebsocketClient:
        """
        Get the websocket client of specific category.

        This method creates and initializes a new websocket client if one
        does not exist for the given category, otherwise returns the existing one.

        Parameters:
            category: Product category (spot, linear, inverse, option)

        Returns:
            WebsocketClient: The websocket client for the specified category
        """
        client: WebsocketClient | None = self.clients.get(category, None)
        if client:
            return client

        # Create client object
        client = WebsocketClient()
        self.clients[category] = client

        client.is_connected = False

        # Bind callbacks with category parameter using partial
        client.on_connected = partial(self.on_connected, category=category)
        client.on_disconnected = partial(self.on_disconnected, category=category)
        client.on_packet = partial(self.on_packet, category=category)
        client.on_error = partial(self.on_error, category=category)

        # Get host based on server environment
        if self.server == "REAL":
            category_host_map: dict[str, str] = {
                "spot": REAL_SPOT_WEBSOCKET_HOST,
                "linear": REAL_LINEAR_WEBSOCKET_HOST,
                "inverse": REAL_INVERSE_WEBSOCKET_HOST,
                "option": REAL_OPTION_WEBSOCKET_HOST,
            }
        elif self.server == "TESTNET":
            category_host_map = {
                "spot": TESTNET_SPOT_WEBSOCKET_HOST,
                "linear": TESTNET_LINEAR_WEBSOCKET_HOST,
                "inverse": TESTNET_INVERSE_WEBSOCKET_HOST,
                "option": TESTNET_OPTION_WEBSOCKET_HOST,
            }
        else:
            category_host_map = {
                "spot": DEMO_SPOT_WEBSOCKET_HOST,
                "linear": DEMO_LINEAR_WEBSOCKET_HOST,
                "inverse": DEMO_INVERSE_WEBSOCKET_HOST,
                "option": DEMO_OPTION_WEBSOCKET_HOST,
            }

        host: str = category_host_map[category]

        # Start connection
        client.init(host, self.proxy_host, self.proxy_port)
        client.start()

        # Return object
        return client

    def subscribe(self, req: SubscribeRequest) -> None:
        """
        Subscribe to market data.

        This function sends subscription requests for ticker and depth data
        for the specified trading instrument.

        Parameters:
            req: Subscription request object containing symbol information
        """
        # Check if already subscribed
        if req.symbol in self.subscribed:
            return
        self.subscribed[req.symbol] = req

        # Look up contract for exchange name
        contract: ContractData | None = self.gateway.get_contract_by_symbol(req.symbol)
        if not contract:
            return

        # Get websocket client for the symbol's category
        category: str = symbol_category_map.get(req.symbol, "")
        if not category:
            return

        # Create tick object keyed by category:exchange_name
        tick: TickData = TickData(
            symbol=req.symbol,
            exchange=req.exchange,
            datetime=datetime.now(),
            name=contract.name,
            gateway_name=self.gateway_name
        )
        tick_key: str = f"{category}:{contract.name}"
        self.ticks[tick_key] = tick

        client: WebsocketClient = self.get_client(category)

        # Send subscribe request if connected
        if client.is_connected:
            # Option orderbook supports max 100 levels, others support 200
            if category == "option":
                depth: int = 100
            else:
                depth = 200

            self.subscribe_topic(client, f"tickers.{contract.name}", self.on_ticker)
            self.subscribe_topic(client, f"orderbook.{depth}.{contract.name}", self.on_depth)

    def subscribe_topic(
        self,
        client: WebsocketClient,
        topic: str,
        callback: Callable[[dict, str], object]
    ) -> None:
        """
        Subscribe to a topic of the public stream.

        Parameters:
            client: The websocket client to send the subscription through
            topic: The topic string to subscribe to
            callback: Callback function for data updates on this topic
        """
        self.callbacks[topic] = callback

        req: dict = {
            "op": "subscribe",
            "args": [topic],
        }
        client.send_packet(req)

    def send_heartbeat(self) -> None:
        """
        Send heartbeat ping to all connected clients.

        This method sends a ping message to keep the websocket
        connections alive.
        """
        for client in self.clients.values():
            if client.is_connected:
                req: dict = {"op": "ping"}
                client.send_packet(req)

    def on_connected(self, category: str) -> None:
        """
        Callback when server is connected.

        This function is called when a category-specific websocket connection
        is established. It resubscribes to previously subscribed topics.

        Parameters:
            category: The product category that was connected
        """
        client: WebsocketClient = self.clients[category]
        client.is_connected = True

        self.gateway.write_log(f"Public websocket stream of {category} is connected")

        # Resubscribe to topics for this category
        for req in self.subscribed.values():
            if symbol_category_map.get(req.symbol, "") != category:
                continue

            contract: ContractData | None = self.gateway.get_contract_by_symbol(req.symbol)
            if not contract:
                continue

            # Option orderbook supports max 100 levels, others support 200
            if category == "option":
                depth: int = 100
            else:
                depth = 200

            self.subscribe_topic(client, f"tickers.{contract.name}", self.on_ticker)
            self.subscribe_topic(client, f"orderbook.{depth}.{contract.name}", self.on_depth)

    def on_disconnected(self, category: str, status_code: int, msg: str) -> None:
        """
        Callback when server is disconnected.

        Parameters:
            category: The product category that was disconnected
            status_code: WebSocket close status code
            msg: Disconnect message
        """
        client: WebsocketClient = self.get_client(category)
        client.is_connected = False

        self.gateway.write_log(f"Public websocket stream of {category} is disconnected")

    def on_packet(self, packet: dict, category: str) -> None:
        """
        Callback of data update.

        This function dispatches incoming websocket messages to the
        appropriate callback handler based on the topic or op field.

        Parameters:
            packet: Data packet from websocket
            category: The product category of the source client
        """
        if "topic" in packet:
            channel: str = packet["topic"]
            callback: Callable | None = self.callbacks.get(channel, None)
            if callback:
                callback(packet, category)
        elif "op" in packet:
            channel = packet["op"]
            callback = self.callbacks.get(channel, None)
            if callback:
                callback(packet)

    def on_error(self, e: Exception, category: str = "") -> None:
        """
        General error callback.

        Parameters:
            e: Exception instance
            category: The product category of the source client
        """
        detail: str = str(e).replace("{", "{{").replace("}", "}}")
        msg: str = f"Exception catched by public websocket API: {detail}"
        self.gateway.write_log(msg)

    def on_ticker(self, packet: dict, category: str) -> None:
        """
        Callback of ticker update.

        This function processes the ticker data updates and
        updates the corresponding TickData objects using the
        field mapping defined in TICK_FIELD_BYBIT2VT.

        Parameters:
            packet: Ticker data from websocket
            category: The product category of the source client
        """
        topic: str = packet["topic"]
        data: dict = packet["data"]

        # Extract exchange name from topic string (format: "tickers.NAME")
        exchange_name: str = topic.replace("tickers.", "")
        tick_key: str = f"{category}:{exchange_name}"
        tick: TickData | None = self.ticks.get(tick_key)
        if not tick:
            return

        tick.datetime = generate_datetime(packet["ts"])

        # Update tick fields using intersection of available and mapped fields
        data_fields: set[str] = set(data.keys())
        tick_fields: set[str] = set(TICK_FIELD_BYBIT2VT.keys())
        update_fields: set[str] = data_fields.intersection(tick_fields)

        for field in update_fields:
            value: float = float(data[field])
            field_name: str = TICK_FIELD_BYBIT2VT[field]
            setattr(tick, field_name, value)

        self.gateway.on_tick(copy(tick))

    def on_depth(self, packet: dict, category: str) -> None:
        """
        Callback of depth update.

        This function processes the order book depth data updates
        and updates the corresponding TickData objects with bid/ask
        prices and volumes (up to 5 levels).

        Parameters:
            packet: Depth data from websocket
            category: The product category of the source client
        """
        data: dict = packet["data"]
        exchange_name: str = data["s"]
        tick_key: str = f"{category}:{exchange_name}"
        tick: TickData | None = self.ticks.get(tick_key)
        if not tick:
            return

        tick.datetime = generate_datetime(packet["ts"])

        # Update bid prices and volumes
        bid_data: list = data["b"]
        for i in range(min(5, len(bid_data))):
            bp, bv = bid_data[i]
            setattr(tick, f"bid_price_{i + 1}", float(bp))
            setattr(tick, f"bid_volume_{i + 1}", float(bv))

        # Update ask prices and volumes
        ask_data: list = data["a"]
        for i in range(min(5, len(ask_data))):
            ap, av = ask_data[i]
            setattr(tick, f"ask_price_{i + 1}", float(ap))
            setattr(tick, f"ask_volume_{i + 1}", float(av))

        self.gateway.on_tick(copy(tick))

    def on_heartbeat(self, packet: dict) -> None:
        """Callback of heartbeat pong."""
        pass


class BybitPrivateWebsocketApi(WebsocketClient):
    """The private websocket API of BybitGateway"""

    def __init__(self, gateway: BybitGateway) -> None:
        """
        The init method of the api.

        Parameters:
            gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BybitGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""
        self.server: str = ""
        self.proxy_host: str = ""
        self.proxy_port: int = 0

        self.callbacks: dict[str, Callable] = {
            "auth": self.on_login,
            "pong": self.on_heartbeat
        }

        self.is_connected: bool = False

    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """
        Start server connection.

        This method establishes a websocket connection to Bybit private
        data stream for receiving order, trade, position and account updates.

        Parameters:
            key: API Key for authentication
            secret: API Secret for request signing
            server: Server type ("REAL" or "DEMO")
            proxy_host: Proxy server hostname or IP
            proxy_port: Proxy server port
        """
        self.key = key
        self.secret = secret
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.server = server

        # Select websocket host based on server environment
        server_hosts: dict[str, str] = {
            "REAL": REAL_PRIVATE_WEBSOCKET_HOST,
            "DEMO": DEMO_PRIVATE_WEBSOCKET_HOST,
            "TESTNET": TESTNET_PRIVATE_WEBSOCKET_HOST,
        }

        url: str = server_hosts[self.server]
        self.init(url, self.proxy_host, self.proxy_port)
        self.start()

    def login(self) -> None:
        """
        User login.

        This function prepares and sends a login request to authenticate
        with the websocket API using HMAC-SHA256 signature.
        """
        # Generate expiry timestamp (30 seconds from now)
        expires: int = int((time.time() + 30) * 1000)

        # Create HMAC-SHA256 signature for authentication
        signature: str = str(hmac.new(
            bytes(self.secret, "utf-8"),
            bytes(f"GET/realtime{expires}", "utf-8"), digestmod="sha256"
        ).hexdigest())

        req: dict = {
            "op": "auth",
            "args": [self.key, expires, signature]
        }
        self.send_packet(req)

    def subscribe_topic(
        self,
        topic: str,
        callback: Callable[[dict], object]
    ) -> None:
        """
        Subscribe to a websocket stream topic.

        Parameters:
            topic: The topic string to subscribe to
            callback: Callback function for data updates on this topic
        """
        self.callbacks[topic] = callback

        req: dict = {
            "op": "subscribe",
            "args": [topic],
        }
        self.send_packet(req)

    def send_heartbeat(self) -> None:
        """
        Send heartbeat ping to server.

        This method sends a ping message to keep the websocket
        connection alive.
        """
        if self.is_connected:
            req: dict = {"op": "ping"}
            self.send_packet(req)

    def on_connected(self) -> None:
        """
        Callback when server is connected.

        This function is called when the websocket connection to the server
        is successfully established. It logs the connection status and
        initiates the login process.
        """
        self.is_connected = True
        self.gateway.write_log("Private websocket stream is connected")
        self.login()

    def on_disconnected(self, status_code: int, msg: str) -> None:
        """
        Callback when server is disconnected.

        Parameters:
            status_code: WebSocket close status code
            msg: Disconnect message
        """
        self.is_connected = False
        self.gateway.write_log("Private websocket stream is disconnected")

    def on_packet(self, packet: dict) -> None:
        """
        Callback of data update.

        This function dispatches incoming websocket messages to the
        appropriate callback handler based on the topic or op field.c

        Parameters:
            packet: Data packet from websocket
        """
        if "topic" in packet:
            channel: str = packet["topic"]
        elif "op" in packet:
            channel = packet["op"]
        else:
            return

        callback: Callable | None = self.callbacks.get(channel, None)
        if callback:
            callback(packet)

    def on_error(self, e: Exception) -> None:
        """
        General error callback.

        Parameters:
            e: Exception instance
        """
        detail: str = str(e).replace("{", "{{").replace("}", "}}")
        msg: str = f"Exception catched by private websocket API: {detail}"
        self.gateway.write_log(msg)

    def on_login(self, packet: dict) -> None:
        """
        Callback of user login.

        This function processes the login response and subscribes to
        private data channels if login is successful.

        Parameters:
            packet: Login response data from websocket
        """
        success: bool = packet.get("success", False)
        if success:
            self.gateway.write_log("Private websocket stream login successful")

            # Subscribe to private data channels
            self.subscribe_topic("order", self.on_order)
            self.subscribe_topic("execution", self.on_trade)
            self.subscribe_topic("position", self.on_position)
            self.subscribe_topic("wallet", self.on_account)
        else:
            error_msg: str = packet.get("ret_msg", packet.get("retMsg", ""))
            self.gateway.write_log(f"Private websocket stream login failed: {error_msg}")

    def on_account(self, packet: dict) -> None:
        """
        Callback of account balance update.

        This function processes account balance updates and creates
        AccountData objects for each coin.

        Parameters:
            packet: Account update data from websocket
        """
        for d in packet["data"]:
            for coin_data in d["coin"]:
                balance: float = 0
                if coin_data["walletBalance"]:
                    balance = float(coin_data["walletBalance"])

                available: float = 0
                if coin_data["availableToWithdraw"]:
                    available = float(coin_data["availableToWithdraw"])

                account: AccountData = AccountData(
                    accountid=coin_data["coin"],
                    balance=balance,
                    frozen=balance - available,
                    gateway_name=self.gateway_name,
                )
                self.gateway.on_account(account)

    def on_trade(self, packet: dict) -> None:
        """
        Callback of trade update.

        This function processes trade execution updates and creates
        TradeData objects for each fill.

        Parameters:
            packet: Trade update data from websocket
        """
        for d in packet["data"]:
            category: str = d.get("category", "")
            contract: ContractData | None = self.gateway.get_contract_by_name(d["symbol"], category)
            if not contract:
                continue

            trade: TradeData = TradeData(
                symbol=contract.symbol,
                exchange=Exchange.GLOBAL,
                orderid=d["orderLinkId"],
                tradeid=d["execId"],
                direction=DIRECTION_BYBIT2VT[d["side"]],
                price=float(d["execPrice"]),
                volume=float(d["execQty"]),
                datetime=generate_datetime(int(d["execTime"])),
                gateway_name=self.gateway_name,
            )

            self.gateway.on_trade(trade)

    def on_order(self, packet: dict) -> None:
        """
        Callback of order update.

        This function processes order status updates and creates
        OrderData objects for each order change.

        Parameters:
            packet: Order update data from websocket
        """
        for d in packet["data"]:
            category: str = d.get("category", "")
            contract: ContractData | None = self.gateway.get_contract_by_name(d["symbol"], category)
            if not contract:
                continue

            order: OrderData = OrderData(
                symbol=contract.symbol,
                exchange=Exchange.GLOBAL,
                orderid=d["orderLinkId"],
                type=ORDER_TYPE_BYBIT2VT[d["orderType"]],
                direction=DIRECTION_BYBIT2VT[d["side"]],
                price=float(d["price"]),
                volume=float(d["qty"]),
                traded=float(d["cumExecQty"]),
                status=STATUS_BYBIT2VT[d["orderStatus"]],
                datetime=generate_datetime(int(d["createdTime"])),
                gateway_name=self.gateway_name
            )

            # Set order offset based on reduceOnly flag
            if d["reduceOnly"]:
                order.offset = Offset.CLOSE
            else:
                order.offset = Offset.OPEN

            self.gateway.on_order(order)

    def on_position(self, packet: dict) -> None:
        """
        Callback of holding position update.

        This function processes position updates and creates
        PositionData objects for each position change.

        Parameters:
            packet: Position update data from websocket
        """
        for d in packet["data"]:
            category: str = d.get("category", "")
            contract: ContractData | None = self.gateway.get_contract_by_name(d["symbol"], category)
            if not contract:
                continue

            # Determine position volume and direction from side field
            volume: float = 0
            if d["side"] == "Buy":
                volume = float(d["size"])
            elif d["side"] == "Sell":
                volume = -float(d["size"])

            position: PositionData = PositionData(
                symbol=contract.symbol,
                exchange=Exchange.GLOBAL,
                direction=Direction.NET,
                volume=volume,
                price=float(d["entryPrice"]),
                gateway_name=self.gateway_name
            )
            self.gateway.on_position(position)

    def on_heartbeat(self, packet: dict) -> None:
        """Callback of heartbeat pong."""
        pass


class BybitTradeWebsocketApi(WebsocketClient):
    """The trade websocket API of BybitGateway"""

    def __init__(self, gateway: BybitGateway) -> None:
        """
        The init method of the api.

        Parameters:
            gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BybitGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""
        self.server: str = ""
        self.proxy_host: str = ""
        self.proxy_port: int = 0

        self.callbacks: dict[str, Callable] = {
            "auth": self.on_login,
            "pong": self.on_heartbeat
        }

        self.order_callbacks: dict[str, Callable] = {}
        self.reqid_order_map: dict[str, OrderData] = {}
        self.is_connected: bool = False

    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """
        Start server connection.

        This method establishes a websocket connection to Bybit trade stream
        for placing and cancelling orders with low latency.

        Parameters:
            key: API Key for authentication
            secret: API Secret for request signing
            server: Server type ("REAL" or "DEMO")
            proxy_host: Proxy server hostname or IP
            proxy_port: Proxy server port
        """
        self.key = key
        self.secret = secret
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.server = server

        # Select websocket host based on server environment
        server_hosts: dict[str, str] = {
            "REAL": REAL_TRADE_WEBSOCKET_HOST,
            "DEMO": DEMO_TRADE_WEBSOCKET_HOST,
            "TESTNET": TESTNET_TRADE_WEBSOCKET_HOST,
        }

        url: str = server_hosts[self.server]
        self.init(url, self.proxy_host, self.proxy_port)
        self.start()

    def login(self) -> None:
        """
        User login.

        This function prepares and sends a login request to authenticate
        with the trade websocket API using HMAC-SHA256 signature.
        """
        # Generate expiry timestamp (30 seconds from now)
        expires: int = int((time.time() + 30) * 1000)

        # Create HMAC-SHA256 signature for authentication
        signature: str = str(hmac.new(
            bytes(self.secret, "utf-8"),
            bytes(f"GET/realtime{expires}", "utf-8"), digestmod="sha256"
        ).hexdigest())

        req: dict = {
            "op": "auth",
            "args": [self.key, expires, signature]
        }
        self.send_packet(req)

    def send_order(self, req: OrderRequest) -> str:
        """
        Send new order to Bybit via trade websocket.

        This function creates and sends a new order request with
        HMAC-SHA256 signed headers for authentication.

        Parameters:
            req: Order request object containing order details

        Returns:
            str: The VeighNa order ID
        """
        # Generate new order id
        orderid: str = self.gateway.rest_api.new_orderid()

        # Push a submitting order event
        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)

        # Look up contract for exchange name
        contract: ContractData | None = self.gateway.get_contract_by_symbol(req.symbol)
        if not contract:
            self.gateway.write_log(f"Failed to send order, symbol not found: {req.symbol}")
            order.status = Status.REJECTED
            self.gateway.on_order(order)
            return order.vt_orderid

        # Prepare timestamp and receive window for signature
        timestamp: int = int(time.time() * 1000) - self.gateway.rest_api.time_offset
        recv_window: int = 30_000

        # Generate unique request ID for tracking
        reqid: str = str(uuid.uuid4())
        self.reqid_order_map[reqid] = order

        # Create order parameters
        args: dict = {
            "category": symbol_category_map.get(req.symbol, ""),
            "symbol": contract.name,
            "orderType": ORDER_TYPE_VT2BYBIT[req.type],
            "side": DIRECTION_VT2BYBIT[req.direction],
            "qty": str(req.volume),
            "price": str(req.price),
            "orderLinkId": orderid,
            "timeInForce": "GTC"
        }

        # Set reduce-only flag for closing orders
        if req.offset == Offset.CLOSE:
            args["reduceOnly"] = True

        # Create signed request header
        header: dict = {
            "X-BAPI-TIMESTAMP": str(timestamp),
            "X-BAPI-RECV-WINDOW": str(recv_window)
        }

        # Generate HMAC-SHA256 signature
        param_str: str = str(timestamp) + self.key + str(recv_window) + json.dumps(args)
        signature: str = generate_signature(self.secret, param_str)
        header["X-BAPI-SIGN"] = signature

        # Send request
        req_data: dict = {
            "reqId": reqid,
            "header": header,
            "op": "order.create",
            "args": [args]
        }
        self.send_packet(req_data)
        print(f"sending order: {req_data}")

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """
        Cancel existing order on Bybit via trade websocket.

        This function sends a signed cancel request. It determines
        whether to use exchange order ID or client order ID based
        on the format of the order ID (UUID format has 4 dashes).

        Parameters:
            req: Cancel request object containing order details
        """
        # Look up contract for exchange name
        contract: ContractData | None = self.gateway.get_contract_by_symbol(req.symbol)
        if not contract:
            self.gateway.write_log(f"Failed to cancel order, symbol not found: {req.symbol}")
            return

        # Generate unique request ID for tracking
        reqid: str = str(uuid.uuid4())

        # Prepare timestamp and receive window for signature
        timestamp: int = int(time.time() * 1000) - self.gateway.rest_api.time_offset
        recv_window: int = 30_000

        args: dict = {
            "category": symbol_category_map.get(req.symbol, ""),
            "symbol": contract.name
        }

        # Determine the type of order ID to use for cancellation
        # Bybit exchange order IDs are UUIDs with 4 dashes
        dash_count: int = req.orderid.count("-")

        if dash_count == 4:
            args["orderId"] = req.orderid
        else:
            args["orderLinkId"] = req.orderid

        # Create signed request header
        header: dict = {
            "X-BAPI-TIMESTAMP": str(timestamp),
            "X-BAPI-RECV-WINDOW": str(recv_window)
        }

        # Generate HMAC-SHA256 signature
        param_str: str = str(timestamp) + self.key + str(recv_window) + json.dumps(args)
        signature: str = generate_signature(self.secret, param_str)
        header["X-BAPI-SIGN"] = signature

        # Send request
        req_data: dict = {
            "reqId": reqid,
            "header": header,
            "op": "order.cancel",
            "args": [args]
        }
        self.send_packet(req_data)

    def send_heartbeat(self) -> None:
        """
        Send heartbeat ping to server.

        This method sends a ping message to keep the websocket
        connection alive.
        """
        if self.is_connected:
            req: dict = {"op": "ping"}
            self.send_packet(req)

    def on_connected(self) -> None:
        """
        Callback when server is connected.

        This function is called when the websocket connection to the server
        is successfully established. It logs the connection status and
        initiates the login process.
        """
        self.is_connected = True
        self.gateway.write_log("Trade websocket stream is connected")
        self.login()

    def on_disconnected(self, status_code: int, msg: str) -> None:
        """
        Callback when server is disconnected.

        Parameters:
            status_code: WebSocket close status code
            msg: Disconnect message
        """
        self.is_connected = False
        self.gateway.write_log("Trade websocket stream is disconnected")

    def on_packet(self, packet: dict) -> None:
        """
        Callback of data update.

        This function dispatches incoming websocket messages to the
        appropriate callback handler. It handles both operation responses
        (auth, pong) and request-specific responses (order, cancel).

        Parameters:
            packet: Data packet from websocket
        """
        if "op" in packet:
            op: str = packet["op"]
            callback: Callable | None = self.callbacks.get(op, None)
            if callback:
                callback(packet)
                return

        if "reqId" in packet:
            reqid: str = packet["reqId"]
            if reqid in self.reqid_order_map:
                self.on_send_order(packet)
            else:
                self.on_cancel_order(packet)

    def on_error(self, e: Exception) -> None:
        """
        General error callback.

        Parameters:
            e: Exception instance
        """
        detail: str = str(e).replace("{", "{{").replace("}", "}}")
        msg: str = f"Exception catched by trade websocket API: {detail}"
        self.gateway.write_log(msg)

    def on_login(self, packet: dict) -> None:
        """
        Callback of user login.

        This function processes the login response and logs the result.

        Parameters:
            packet: Login response data from websocket
        """
        if packet["retCode"] == 0:
            self.gateway.write_log("Trade websocket stream login successful")
        else:
            self.gateway.write_log(f"Trade websocket stream login failed: {packet['retMsg']}")

    def on_send_order(self, packet: dict) -> None:
        """
        Callback of send order.

        This function processes the response to an order placement request.
        If the return code indicates an error, the order is rejected.

        Parameters:
            packet: Order response data from websocket
        """
        reqid: str = packet["reqId"]
        order: OrderData | None = self.reqid_order_map.pop(reqid, None)

        if packet["retCode"] != 0:
            self.gateway.write_log(f"Send order failed: {packet['retMsg']}")
            print(f"send order error: {packet}")

            if order is not None:
                order.status = Status.REJECTED
                self.gateway.on_order(order)

    def on_cancel_order(self, packet: dict) -> None:
        """
        Callback of cancel order.

        This function processes the response to an order cancellation request.
        If the return code indicates an error, the failure is logged.

        Parameters:
            packet: Cancel response data from websocket
        """
        if packet["retCode"] != 0:
            self.gateway.write_log(f"Cancel order failed: {packet['retMsg']}")

    def on_heartbeat(self, packet: dict) -> None:
        """Callback of heartbeat pong."""
        pass


def generate_signature(secret: str, param_str: str) -> str:
    """
    Generate HMAC-SHA256 signature for API authentication.

    Parameters:
        secret: API secret key
        param_str: Parameter string to be signed

    Returns:
        str: Hex-encoded signature string
    """
    hash: hmac.HMAC = hmac.new(
        bytes(secret, "utf-8"),
        param_str.encode("utf-8"),
        hashlib.sha256,
    )
    return hash.hexdigest()


def generate_datetime(timestamp: int) -> datetime:
    """
    Generate datetime object from millisecond timestamp.

    Parameters:
        timestamp: Unix timestamp in milliseconds

    Returns:
        datetime: Datetime object with Shanghai timezone
    """
    dt: datetime = datetime.fromtimestamp(timestamp / 1000)
    return dt.replace(tzinfo=BYBIT_TZ)


def prepare_payload(method: str, parameters: dict) -> str:
    """
    Prepare the request payload and validate parameter value types.

    For GET requests, parameters are encoded as URL query string.
    For other methods, parameters are serialized as JSON with
    type validation for specific fields.

    Parameters:
        method: HTTP method (GET, POST, etc.)
        parameters: Request parameters dictionary

    Returns:
        str: Encoded payload string
    """
    def cast_values() -> None:
        """Cast parameter values to their expected types."""
        string_params: list[str] = [
            "qty",
            "price",
            "triggerPrice",
            "takeProfit",
            "stopLoss",
        ]
        integer_params: list[str] = ["positionIdx"]
        for key, value in parameters.items():
            if key in string_params:
                if not isinstance(value, str):
                    parameters[key] = str(value)
            elif key in integer_params:
                if not isinstance(value, int):
                    parameters[key] = int(value)

    if method == "GET":
        payload: str = "&".join(
            [
                str(k) + "=" + str(v)
                for k, v in sorted(parameters.items())
                if v is not None
            ]
        )
        return payload
    else:
        cast_values()
        return json.dumps(parameters)
