import hashlib
import hmac
import sys
import time
import json
from copy import copy
from datetime import datetime, timedelta
from typing import Callable
from zoneinfo import ZoneInfo
from functools import partial

from vnpy_evo.event import EventEngine
from vnpy_evo.trader.constant import (
    Direction,
    Exchange,
    Interval,
    OrderType,
    Product,
    Status,
    Offset,
    OptionType
)
from vnpy_evo.trader.gateway import BaseGateway
from vnpy_evo.trader.object import (
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
from vnpy_evo.rest import Request, RestClient
from vnpy_evo.websocket import WebsocketClient


# Timezone
BYBIT_TZ: ZoneInfo = ZoneInfo("Asia/Shanghai")

# Real server hosts
REAL_REST_HOST: str = "https://api.bybit.com"
REAL_PRIVATE_WEBSOCKET_HOST: str = "wss://stream.bybit.com/v5/private"
REAL_SPOT_WEBSOCKET_HOST: str = "wss://stream.bybit.com/v5/public/spot"
REAL_LINEAR_WEBSOCKET_HOST: str = "wss://stream.bybit.com/v5/public/linear"
REAL_INVERSE_WEBSOCKET_HOST: str = "wss://stream.bybit.com/v5/public/inverse"
REAL_OPTION_WEBSOCKET_HOST: str = "wss://stream.bybit.com/v5/public/option"

# Demo server hosts
DEMO_REST_HOST: str = "https://api-demo.bybit.com"
DEMO_PRIVATE_WEBSOCKET_HOST: str = "wss://stream-demo.bybit.com/v5/private"
DEMO_SPOT_WEBSOCKET_HOST: str = "wss://stream-demo.bybit.com/v5/public/spot"
DEMO_LINEAR_WEBSOCKET_HOST: str = "wss://stream-demo.bybit.com/v5/public/linear"
DEMO_INVERSE_WEBSOCKET_HOST: str = "wss://stream-demo.bybit.com/v5/public/inverse"
DEMO_OPTION_WEBSOCKET_HOST: str = "wss://stream-demo.bybit.com/v5/public/option"

# Product type map
PRODUCT_BYBIT2VT: dict[str, Exchange] = {
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
    """

    default_name = "BYBIT"

    default_setting: dict[str, str] = {
        "API Key": "",
        "Secret Key": "",
        "Server": ["REAL", "DEMO"],
        "Proxy Host": "",
        "Proxy Port": "",
    }

    default_name: str = "BYBIT"

    exchanges: list[Exchange] = [Exchange.BYBIT]

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

    def connect(self, setting: dict) -> None:
        """Start server connections"""
        key: str = setting["API Key"]
        secret: str = setting["Secret Key"]
        server: str = setting["Server"]
        proxy_host: str = setting["Proxy Host"]
        proxy_port: str = setting["Proxy Port"]

        if proxy_port.isdigit():
            proxy_port = int(proxy_port)
        else:
            proxy_port = 0

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

    def subscribe(self, req: SubscribeRequest) -> None:
        """Subscribe market data"""
        self.public_ws_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """Send new order"""
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest):
        """Cancel existing order"""
        self.rest_api.cancel_order(req)

    def query_account(self) -> None:
        """Not required since Bybit provides websocket update"""
        pass

    def query_position(self) -> None:
        """Not required since Bybit provides websocket update"""
        return

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """Query kline history data"""
        return self.rest_api.query_history(req)

    def close(self) -> None:
        """Close server connections"""
        self.rest_api.stop()
        self.private_ws_api.stop()
        self.public_ws_api.stop()


class BybitRestApi(RestClient):
    """The REST API of BybitGateway"""

    def __init__(self, gateway: BybitGateway) -> None:
        """
        The init method of the api.

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
        """Standard callback for signing a request"""
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

        # Add headers
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
        """Generate local order id"""
        prefix: str = datetime.now().strftime("%Y%m%d-%H%M%S-")

        self.order_count += 1
        suffix: str = str(self.order_count).rjust(8, "0")

        orderid: str = prefix + suffix
        return orderid

    def check_error(self, name: str, data: dict) -> bool:
        """回报状态检查"""
        if data["ret_code"]:
            error_code: int = data["ret_code"]
            error_msg: str = data["ret_msg"]
            msg = f"{name}失败，错误代码：{error_code}，信息：{error_msg}"
            self.gateway.write_log(msg)
            return True

        return False

    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """Start server connection"""
        self.key = key
        self.secret = secret

        if server == "REAL":
            self.init(REAL_REST_HOST, proxy_host, proxy_port)
        else:
            self.init(DEMO_REST_HOST, proxy_host, proxy_port)

        self.start()
        self.gateway.write_log("REST API started")

        self.query_time()

    def query_time(self) -> None:
        """Query server time"""
        self.add_request(
            "GET",
            "/v5/market/time",
            callback=self.on_query_time
        )

    def query_contract(self) -> None:
        """Query available contract"""
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
        """Query open orders"""
        for category in ["spot", "linear", "inverse", "option"]:
            params: dict = {"category": category}

            if category == "linear":
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
        """Query account balance"""
        for account_type in ["UNIFIED", "CONTRACT"]:
            params: dict = {"accountType": account_type}

            self.add_request(
                "GET",
                "/v5/account/wallet-balance",
                self.on_query_account,
                params=params
            )

    def query_position(self) -> None:
        """Query holding positions"""
        for category in ["linear", "inverse", "option"]:
            params: dict = {
                "category": category,
                "limit": 200
            }

            if category == "linear":
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
        """Send new order"""
        # Generate new order id
        orderid: str = self.new_orderid()

        # Push a submitting order event
        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)

        # Create order parameters
        data: dict = {
            "category": symbol_category_map.get(req.symbol, ""),
            "symbol": req.symbol,
            "orderType": ORDER_TYPE_VT2BYBIT[req.type],
            "side": DIRECTION_VT2BYBIT[req.direction],
            "qty": str(req.volume),
            "price": str(req.price),
            "orderLinkId": orderid
        }

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
        """Cancel existing order"""
        # Create cancel parameters
        data: dict = {
            "category": symbol_category_map.get(req.symbol, ""),
            "symbol": req.symbol
        }

        # Use dash count to check order id type
        dash_count = req.orderid.count("-")

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
        """Query kline history data"""
        count: int = 1000
        start_time: int = int(req.start.timestamp()) * 1000
        category: str = symbol_category_map.get(req.symbol, "")

        buf: dict[datetime, BarData] = {}
        last_end_dt: datetime = None

        while True:
            # Create query params
            params: dict = {
                "category": category,
                "symbol": req.symbol,
                "interval": INTERVAL_VT2BYBIT[req.interval],
                "start": start_time,
                "limit": count
            }

            # Get response from server
            resp = self.request(
                "GET",
                "/v5/market/kline",
                params=params
            )

            # Break loop if request is failed
            if resp.status_code // 100 != 2:
                msg = f"Query kline history failed, status code: {resp.status_code}, message: {resp.text}"
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
                begin_dt = generate_datetime(int(begin))

                end: datetime = kline_data[0][0]
                end_dt = generate_datetime(int(end))

                # Break loop if all data received
                if end_dt == last_end_dt:
                    break
                last_end_dt = end_dt

                msg: str = f"Query kline history finished, {req.symbol} - {req.interval.value}, {begin_dt} - {end_dt}"
                self.gateway.write_log(msg)
                print(msg)

                # Update start time
                start_time: int = end

        index: list[datetime] = list(buf.keys())
        index.sort()

        history: list[BarData] = [buf[i] for i in index]
        return history

    def on_failed(self, status_code: int, request: Request) -> None:
        """General failed callback"""
        data: dict = request.response.json()
        error_msg: str = data["ret_msg"]
        error_code: int = data["ret_code"]

        msg = f"Request failed, status code：{request.status}, error code: {error_code}, message: {error_msg}"
        self.gateway.write_log(msg)

    def on_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb,
        request: Request
    ) -> None:
        """General error callback"""
        msg = f"Exception raised, type: {exception_type}, value: {exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(self.exception_detail(exception_type, exception_value, tb, request))

    def on_query_time(self, packet: dict, request: Request) -> None:
        """Callback of server time query"""
        result: dict = packet["result"]

        local_time: float = int(time.time() * 1000)
        server_time: float = int(int(result["timeNano"]) / 1_000_000)
        self.time_offset = local_time - server_time

        self.gateway.write_log(f"Server time updated, local offset: {self.time_offset} ms")

        self.query_contract()
        self.query_order()
        self.query_account()
        self.query_position()

    def on_query_contract(self, data: dict, request: Request) -> None:
        """Callback of available contracts query"""
        result: dict = data["result"]

        category: str = result["category"]
        product: Product = PRODUCT_BYBIT2VT[category]

        for d in result["list"]:
            contract: ContractData = ContractData(
                symbol=d["symbol"],
                exchange=Exchange.BYBIT,
                name=d["symbol"],
                product=product,
                size=1,
                pricetick=float(d["priceFilter"]["tickSize"]),
                min_volume=float(d["lotSizeFilter"]["minOrderQty"]),
                history_data=True,
                net_position=True,
                gateway_name=self.gateway_name
            )

            # If symbol contains digit, then should be futures
            if product == Product.SWAP and not contract.symbol.isalpha():
                contract.product = Product.FUTURES

            # Add extra option field
            if product == Product.OPTION:
                buf: list = contract.symbol.split("-")

                contract.option_strike = int(buf[2])
                contract.option_underlying = "-".join(buf[:2])
                contract.option_type = OPTION_TYPE_BYBIT2VT[d["optionsType"]]
                contract.option_listed = generate_datetime(float(d["launchTime"]))
                contract.option_expiry = generate_datetime(float(d["deliveryTime"]))
                contract.option_portfolio = buf[0]
                contract.option_index = str(contract.option_strike)

            symbol_category_map[contract.symbol] = category

            self.gateway.on_contract(contract)

        self.gateway.write_log(f"Available {category} contracts data is received")

    def on_query_order(self, data: dict, request: Request):
        """Callback of open orders query"""
        if data["retCode"]:
            msg = f"Query open orders failed, code: {data['retCode']}, message: {data['retMsg']}"
            self.gateway.write_log(msg)
            return

        result: dict = data["result"]
        category: str = result["category"]

        for d in result["list"]:
            order: OrderData = OrderData(
                symbol=d["symbol"],
                exchange=Exchange.BYBIT,
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

            offset: bool = d["reduceOnly"]
            if offset:
                order.offset = Offset.CLOSE
            else:
                order.offset = Offset.OPEN

            self.gateway.on_order(order)

        self.gateway.write_log(f"{category} open orders data is received")

    def on_query_account(self, data: dict, request: Request) -> None:
        """Callback of account balance query"""
        if data["retCode"]:
            msg = f"Query account balance failed, code: {data['retCode']}, message: {data['retMsg']}"
            self.gateway.write_log(msg)
            return

        result: dict = data["result"]

        for d in result["list"]:
            balance: float = 0
            if d["totalWalletBalance"]:
                balance = float(d["totalWalletBalance"])

            available: float = 0
            if d["totalAvailableBalance"]:
                available = float(d["totalAvailableBalance"])

            account: AccountData = AccountData(
                accountid=d["accountType"],
                balance=balance,
                frozen=balance - available,
                gateway_name=self.gateway_name,
            )
            self.gateway.on_account(account)

    def on_query_position(self, data: dict, request: Request) -> None:
        """Callback of holding positions query"""
        if data["retCode"]:
            msg = f"Query holding position failed, code: {data['retCode']}, message: {data['retMsg']}"
            self.gateway.write_log(msg)
            return

        result: dict = data["result"]

        for d in result["list"]:
            volume: float = 0
            if d["side"] == "Buy":
                volume = float(d["size"])
            elif d["side"] == "Sell":
                volume = -float(d["size"])

            position: PositionData = PositionData(
                symbol=d["symbol"],
                exchange=Exchange.BYBIT,
                direction=Direction.NET,
                volume=volume,
                price=float(d["avgPrice"]),
                gateway_name=self.gateway_name
            )
            self.gateway.on_position(position)

    def on_send_order_failed(self, status_code: int, request: Request) -> None:
        """Failed callback of send_order"""
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        msg = f"Send order failed, code: {status_code}"
        self.gateway.write_log(msg)

    def on_send_order_error(self, exception_type: type, exception_value: Exception, tb, request: Request) -> None:
        """Error callback of send_order"""
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        msg: str = f"Send order error, exception type: {exception_type}, exception value: {exception_value}"
        self.gateway.write_log(msg)

        self.on_error(exception_type, exception_value, tb, request)

    def on_send_order(self, data: dict, request: Request) -> None:
        """Successful callback of send order"""
        if data["retCode"]:
            msg = f"Send order failed, code: {data['retCode']}, message: {data['retMsg']}"
            self.gateway.write_log(msg)

            order: OrderData = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)

    def on_cancel_order(self, data: dict, request: Request) -> None:
        """Successful callback of cancel order"""
        if data["retCode"]:
            msg = f"Cancel order failed, code: {data['retCode']}, message: {data['retMsg']}"
            self.gateway.write_log(msg)


class BybitPublicWebsocketApi:
    """The public websocket API of BybitGateway"""

    def __init__(self, gateway: BybitGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BybitGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.clients: dict[str, WebsocketClient] = {}
        self.callbacks: dict[str, Callable] = {}
        self.ticks: dict[str, TickData] = {}
        self.subscribed: dict[str, SubscribeRequest] = {}

    def connect(
        self,
        server: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """Start server connection"""
        self.server = server
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port

    def stop(self) -> None:
        """Close server connection"""
        for client in self.clients.values():
            client.stop()

    def get_client(self, category: str) -> WebsocketClient:
        """Get the websocket client of specific category"""
        client: WebsocketClient = self.clients.get(category, None)
        if client:
            return client

        # Create client object
        client = WebsocketClient()
        self.clients[category] = client

        client.is_connected = False

        client.on_connected = partial(self.on_connected, category=category)
        client.on_disconnected = partial(self.on_disconnected, category=category)
        client.on_packet = partial(self.on_packet, category=category)
        client.on_error = partial(self.on_error, category=category)

        # Get host
        if self.server == "REAL":
            category_host_map: dict = {
                "spot": REAL_SPOT_WEBSOCKET_HOST,
                "linear": REAL_LINEAR_WEBSOCKET_HOST,
                "inverse": REAL_INVERSE_WEBSOCKET_HOST,
                "option": REAL_OPTION_WEBSOCKET_HOST,
            }
        else:
            category_host_map: dict = {
                "spot": DEMO_SPOT_WEBSOCKET_HOST,
                "linear": DEMO_LINEAR_WEBSOCKET_HOST,
                "inverse": DEMO_INVERSE_WEBSOCKET_HOST,
                "option": DEMO_OPTION_WEBSOCKET_HOST,
            }

        category_host_map: dict = {
            "spot": REAL_SPOT_WEBSOCKET_HOST,
            "linear": REAL_LINEAR_WEBSOCKET_HOST,
            "inverse": REAL_INVERSE_WEBSOCKET_HOST,
            "option": REAL_OPTION_WEBSOCKET_HOST,
        }

        host: str = category_host_map[category]

        # Start conection
        client.init(host, self.proxy_host, self.proxy_port)
        client.start()

        # Return object
        return client

    def subscribe(self, req: SubscribeRequest) -> None:
        """Subscribe market data"""
        # Check if already subscribed
        if req.symbol in self.subscribed:
            return
        self.subscribed[req.symbol] = req

        # Create tick object
        tick: TickData = TickData(
            symbol=req.symbol,
            exchange=req.exchange,
            datetime=datetime.now(),
            name=req.symbol,
            gateway_name=self.gateway_name
        )
        self.ticks[req.symbol] = tick

        # Get websocket client
        category: str = symbol_category_map.get(req.symbol, "")
        if not category:
            return
        client: WebsocketClient = self.get_client(category)

        # Send subscribe request
        if client.is_connected:
            if category == "option":
                depth: int = 100
            else:
                depth: int = 200

            self.subscribe_topic(client, f"tickers.{req.symbol}", self.on_ticker)
            self.subscribe_topic(client, f"orderbook.{depth}.{req.symbol}", self.on_depth)

    def subscribe_topic(
        self,
        client: WebsocketClient,
        topic: str,
        callback: Callable[[str, dict], object]
    ) -> None:
        """Subscribe topic of public stream"""
        self.callbacks[topic] = callback

        req: dict = {
            "op": "subscribe",
            "args": [topic],
        }
        client.send_packet(req)

    def on_connected(self, category: str) -> None:
        """Callback when server is connected"""
        client: WebsocketClient = self.clients[category]
        client.is_connected = True

        self.gateway.write_log(f"Public websocket stream of {category} is connected")

        # Send subscribe request
        for req in self.subscribed.values():
            if symbol_category_map.get(req.symbol, "") != category:
                continue

            if category == "option":
                depth: int = 25
            else:
                depth: int = 50

            self.subscribe_topic(client, f"tickers.{req.symbol}", self.on_ticker)
            self.subscribe_topic(client, f"orderbook.{depth}.{req.symbol}", self.on_depth)

    def on_disconnected(self, category: str) -> None:
        """Callback when server is disconnected"""
        client: WebsocketClient = self.get_client(category)
        client.is_connected = False

        self.gateway.write_log(f"Public websocket stream of {category} is disconnected")

    def on_packet(self, packet: dict, category: str) -> None:
        """Callback of data update"""
        topic: str = packet.get("topic", "")
        if not topic:
            return

        callback: callable = self.callbacks[topic]
        callback(packet)

    def on_error(self, e: Exception) -> None:
        """General error callback"""
        msg: str = f"Exception catched by public websocket API: {e}"
        self.gateway.write_log(msg)

    def on_ticker(self, packet: dict) -> None:
        """Callback of ticker update"""
        topic: str = packet["topic"]
        data: dict = packet["data"]

        symbol: str = topic.replace("tickers.", "")
        tick: TickData = self.ticks[symbol]

        tick.datetime = generate_datetime(packet["ts"])

        data_fields: set[str] = set(data.keys())
        tick_fields: set[str] = set(TICK_FIELD_BYBIT2VT.keys())
        update_fields: set[str] = data_fields.intersection(tick_fields)

        for field in update_fields:
            value: float = float(data[field])
            name: str = TICK_FIELD_BYBIT2VT[field]
            setattr(tick, name, value)

        self.gateway.on_tick(copy(tick))

    def on_depth(self, packet: dict) -> None:
        """Callback of depth update"""
        data: dict = packet["data"]
        symbol: str = data["s"]
        tick: TickData = self.ticks[symbol]

        tick.datetime = generate_datetime(packet["ts"])

        bid_data: list = data["b"]
        for i in range(min(5, len(bid_data))):
            bp, bv = bid_data[i]
            setattr(tick, f"bid_price_{i+1}", float(bp))
            setattr(tick, f"bid_volume_{i+1}", float(bv))

        ask_data: list = data["a"]
        for i in range(min(5, len(ask_data))):
            ap, av = ask_data[i]
            setattr(tick, f"ask_price_{i+1}", float(ap))
            setattr(tick, f"ask_volume_{i+1}", float(av))

        self.gateway.on_tick(copy(tick))


class BybitPrivateWebsocketApi(WebsocketClient):
    """The private websocket API of BybitGateway"""

    def __init__(self, gateway: BybitGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BybitGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""
        self.server: str = ""

        self.callbacks: dict[str, Callable] = {}

    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """Start server connection"""
        self.key = key
        self.secret = secret
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.server = server

        if self.server == "REAL":
            url = REAL_PRIVATE_WEBSOCKET_HOST
        else:
            url = DEMO_PRIVATE_WEBSOCKET_HOST

        self.init(url, self.proxy_host, self.proxy_port)
        self.start()

    def login(self) -> None:
        """User login"""
        expires: int = int((time.time() + 30) * 1000)

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
        callback: Callable[[str, dict], object]
    ) -> None:
        """Subscribe websocket stream topic"""
        self.callbacks[topic] = callback

        req: dict = {
            "op": "subscribe",
            "args": [topic],
        }
        self.send_packet(req)

    def on_connected(self) -> None:
        """Callback when server is connected"""
        self.gateway.write_log("Private websocket stream is connected")
        self.login()

    def on_disconnected(self) -> None:
        """Callback when server is disconnected"""
        self.gateway.write_log("Private websocket stream is disconnected")

    def on_packet(self, packet: dict) -> None:
        """Callback of data update"""
        if "topic" not in packet:
            op: str = packet["op"]
            if op == "auth":
                self.on_login(packet)
        else:
            channel: str = packet["topic"]
            callback: callable = self.callbacks[channel]
            callback(packet)

    def on_error(self, e: Exception) -> None:
        """General error callback"""
        msg: str = f"Exception catched by private websocket API: {e}"
        self.gateway.write_log(msg)

    def on_login(self, packet: dict):
        """Callback of user login"""
        success: bool = packet.get("success", False)
        if success:
            self.gateway.write_log("Private websocket stream login successful")

            self.subscribe_topic("order", self.on_order)
            self.subscribe_topic("execution", self.on_trade)
            self.subscribe_topic("position", self.on_position)
            self.subscribe_topic("wallet", self.on_account)
        else:
            self.gateway.write_log(f"Private websocket stream login failed: {packet['ret_msg']}")

    def on_account(self, packet: dict) -> None:
        """Callback of account balance update"""
        for d in packet["data"]:
            account = AccountData(
                accountid=d["accountType"],
                balance=float(d["totalWalletBalance"]),
                frozen=(float(d["totalWalletBalance"]) - float(d["totalAvailableBalance"])),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_account(account)

    def on_trade(self, packet: dict) -> None:
        """Callback of trade update"""
        for d in packet["data"]:
            trade: TradeData = TradeData(
                symbol=d["symbol"],
                exchange=Exchange.BYBIT,
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
        """Callback of order update"""
        for d in packet["data"]:
            order: OrderData = OrderData(
                symbol=d["symbol"],
                exchange=Exchange.BYBIT,
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

            if d["reduceOnly"]:
                order.offset = Offset.CLOSE
            else:
                order.offset = Offset.OPEN

            self.gateway.on_order(order)

    def on_position(self, packet: dict) -> None:
        """Callback of holding position update"""
        for d in packet["data"]:
            volume: float = 0
            if d["side"] == "Buy":
                volume = float(d["size"])
            elif d["side"] == "Sell":
                volume = -float(d["size"])

            position: PositionData = PositionData(
                symbol=d["symbol"],
                exchange=Exchange.BYBIT,
                direction=Direction.NET,
                volume=volume,
                price=float(d["entryPrice"]),
                gateway_name=self.gateway_name
            )
            self.gateway.on_position(position)


def generate_signature(secret: str, param_str: str) -> str:
    """Generate signature for REST API"""
    hash: hmac.HMAC = hmac.new(
        bytes(secret, "utf-8"),
        param_str.encode("utf-8"),
        hashlib.sha256,
    )
    return hash.hexdigest()


def generate_datetime(timestamp: int) -> datetime:
    """Generate datetime object from timestamp"""
    dt: datetime = datetime.fromtimestamp(timestamp / 1000)
    return dt.replace(tzinfo=BYBIT_TZ)


def prepare_payload(method: str, parameters: dict) -> str:
    """
    Prepares the request payload and validates parameter value types.
    """

    def cast_values():
        string_params = [
            "qty",
            "price",
            "triggerPrice",
            "takeProfit",
            "stopLoss",
        ]
        integer_params = ["positionIdx"]
        for key, value in parameters.items():
            if key in string_params:
                if not isinstance(value, str):
                    parameters[key] = str(value)
            elif key in integer_params:
                if not isinstance(value, int):
                    parameters[key] = int(value)

    if method == "GET":
        payload = "&".join(
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
