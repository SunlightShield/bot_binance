import json
import os
import queue
import threading
from collections import deque
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, Set, Tuple

import binance.client
from binance.exceptions import BinanceAPIException, BinanceRequestException

from .config import Config
from .logger import Logger

# ---------------------------------------------------------------------------
# PARCHE 1: esta version reemplaza los websockets (unicorn-binance-websocket-api)
# por consultas REST. Motivo: Binance retiro el endpoint listenKey
# (POST /api/v3/userDataStream responde 410 Gone) y el testnet cambio el host
# de los streams, asi que el stream de ordenes y el de precios ya no funcionan.
#
# PARCHE 2: diario de operaciones (TradeJournal). Cada orden que llega a un
# estado final se registra con precio, cantidad y comision en:
#   data/trades_<env>.jsonl  -> completo, solo agrega lineas, nunca se recorta
#   data/trades_<env>.json   -> solo las ultimas N (200), para leer comodo
# <env> es "testnet" o "real", para no mezclar pruebas con el experimento real.
#
# Se mantiene la interfaz que usa el resto del bot:
#   BinanceOrder, BinanceCache, OrderGuard, BinanceStreamManager
#   (acquire_order_guard, close)
# ---------------------------------------------------------------------------

# Estados terminales o con cambio de saldo: al verlos se invalida la cache de saldos.
BALANCE_CHANGING_STATUSES = {"FILLED", "PARTIALLY_FILLED", "CANCELED", "EXPIRED", "REJECTED"}
# Estados finales: la orden ya no va a cambiar y se registra en el diario.
TERMINAL_STATUSES = {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}


class BinanceOrder:  # pylint: disable=too-few-public-methods
    def __init__(self, report):
        self.event = report
        self.symbol = report["symbol"]
        self.side = report["side"]
        self.order_type = report["order_type"]
        self.id = report["order_id"]
        self.cumulative_quote_qty = float(report["cumulative_quote_asset_transacted_quantity"])
        self.status = report["current_order_status"]
        self.price = float(report["order_price"])
        self.time = report["transaction_time"]

    def __repr__(self):
        return f"<BinanceOrder {self.event}>"


class BinanceCache:  # pylint: disable=too-few-public-methods
    ticker_values: Dict[str, float] = {}
    _balances: Dict[str, float] = {}
    _balances_mutex: threading.Lock = threading.Lock()
    non_existent_tickers: Set[str] = set()
    orders: Dict[str, BinanceOrder] = {}

    @contextmanager
    def open_balances(self):
        with self._balances_mutex:
            yield self._balances


class OrderGuard:
    def __init__(self, pending_orders: Set[Tuple[str, int]], mutex: threading.Lock, on_release=None):
        self.pending_orders = pending_orders
        self.mutex = mutex
        self.on_release = on_release
        # lock immediately because OrderGuard
        # should be entered and put tag that shouldn't be missed
        self.mutex.acquire()
        self._held = True
        self.tag = None

    def set_order(self, origin_symbol: str, target_symbol: str, order_id: int):
        self.tag = (origin_symbol + target_symbol, order_id)

    def release(self):
        # Suelta el candado si todavia lo tiene este guard (error antes de entrar a wait_for_order)
        if self._held:
            self._held = False
            self.mutex.release()

    def __enter__(self):
        try:
            if self.tag is None:
                raise Exception("OrderGuard wasn't properly set")
            self.pending_orders.add(self.tag)
        finally:
            self._held = False
            self.mutex.release()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pending_orders.remove(self.tag)
        # Aviso para hacer una ultima consulta de la orden (estado final) para el diario
        if self.on_release is not None:
            try:
                self.on_release(self.tag)
            except Exception:  # pylint: disable=broad-except
                pass

class TradeJournal:
    """Diario de operaciones: un .jsonl completo y un .json con las ultimas N."""

    def __init__(self, directory: str = "data", env: str = "testnet", keep_last: int = 200):
        self.keep_last = keep_last
        os.makedirs(directory, exist_ok=True)
        self.path_all = os.path.join(directory, f"trades_{env}.jsonl")
        self.path_last = os.path.join(directory, f"trades_{env}.json")
        self._last = deque(maxlen=keep_last)
        self._recorded = set()
        self._load_existing()

    def _load_existing(self):
        """Al reiniciar, recupera lo ya registrado para no duplicar ni perder el historial."""
        if not os.path.exists(self.path_all):
            return
        with open(self.path_all, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except ValueError:
                    continue  # linea corrupta (p. ej. corte a mitad de escritura): se ignora
                self._recorded.add((entry.get("symbol"), entry.get("order_id")))
                self._last.append(entry)

    def has(self, symbol, order_id) -> bool:
        return (symbol, order_id) in self._recorded

    def record(self, entry: dict) -> bool:
        key = (entry["symbol"], entry["order_id"])
        if key in self._recorded:
            return False
        with open(self.path_all, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
            fh.flush()
        self._recorded.add(key)
        self._last.append(entry)
        # escritura atomica: nunca queda un JSON a medias si el bot se corta
        tmp = self.path_last + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(list(self._last), fh, indent=2, ensure_ascii=False)
        os.replace(tmp, self.path_last)
        return True


def _ms_to_utc_iso(ms) -> str:
    return datetime.fromtimestamp(float(ms) / 1000, tz=timezone.utc).isoformat(timespec="seconds")


def _ms_to_local(ms) -> str:
    return datetime.fromtimestamp(float(ms) / 1000).strftime("%Y-%m-%d %H:%M:%S")


class BinanceStreamManager:
    """Misma interfaz que la version con websockets, pero consultando por REST."""

    ORDER_POLL_SECONDS = 1
    ERROR_BACKOFF_SECONDS = 5
    FINAL_CHECK_MAX_ATTEMPTS = 5

    def __init__(
        self,
        cache: BinanceCache,
        config: Config,
        binance_client: binance.client.Client,
        logger: Logger,
        journal_dir: str = "data",
    ):
        self.cache = cache
        self.logger = logger
        self.binance_client = binance_client
        # El precio se refresca como minimo cada 2 s para no gastar cuota de la API
        self.ticker_poll_seconds = max(2, int(config.SCOUT_SLEEP_TIME))

        self.env = "testnet" if bool(getattr(config, "TESTNET", False)) else "real"
        try:
            self.journal = TradeJournal(journal_dir, self.env)
        except Exception as e:  # pylint: disable=broad-except
            # el diario nunca debe impedir que el bot opere
            self.journal = None
            self.logger.error(f"No se pudo iniciar el diario de operaciones: {e}", False)

        self.pending_orders: Set[Tuple[str, int]] = set()
        self.pending_orders_mutex: threading.Lock = threading.Lock()
        self._final_checks: "queue.Queue" = queue.Queue()
        self._stop = threading.Event()

        self._ticker_thread = threading.Thread(target=self._ticker_loop, name="rest-ticker", daemon=True)
        self._orders_thread = threading.Thread(target=self._orders_loop, name="rest-orders", daemon=True)
        self._ticker_thread.start()
        self._orders_thread.start()

    def acquire_order_guard(self):
        return OrderGuard(
            self.pending_orders,
            self.pending_orders_mutex,
            on_release=lambda tag: self._final_checks.put((tag[0], tag[1], 0)),
        )

    def _invalidate_balances(self):
        with self.cache.open_balances() as balances:
            balances.clear()

    # --- precios ----------------------------------------------------------
    def _ticker_loop(self):
        while not self._stop.is_set():
            wait = self.ticker_poll_seconds
            try:
                self.cache.ticker_values.update(
                    {ticker["symbol"]: float(ticker["price"]) for ticker in self.binance_client.get_symbol_ticker()}
                )
            except (BinanceRequestException, BinanceAPIException) as e:
                self.logger.error(f"Error consultando precios por REST: {e}", False)
                wait = self.ERROR_BACKOFF_SECONDS
            except Exception as e:  # pylint: disable=broad-except
                self.logger.error(f"Error inesperado consultando precios: {e}", False)
                wait = self.ERROR_BACKOFF_SECONDS
            self._stop.wait(wait)

    # --- diario -----------------------------------------------------------
    def _journal_order(self, order: dict, final: bool = False):
        """Registra una orden en estado final. Nunca debe romper el trading.

        final=True es la ultima consulta de la orden: si la comision no se pudo leer,
        se registra igual con commission=null (desconocida). En consultas anteriores
        se reintenta en el siguiente sondeo.
        """
        if self.journal is None or self.journal.has(order["symbol"], order["orderId"]):
            return
        try:
            executed = float(order["executedQty"])
            quote = float(order["cummulativeQuoteQty"])

            commission = None  # None = desconocida (la consulta fallo); [] = sin comision
            if executed > 0:
                try:
                    totals: Dict[str, float] = {}
                    for fill in self.binance_client.get_my_trades(symbol=order["symbol"], orderId=order["orderId"]):
                        asset = fill["commissionAsset"]
                        totals[asset] = totals.get(asset, 0.0) + float(fill["commission"])
                    commission = [{"asset": a, "amount": v} for a, v in totals.items()]
                except Exception as e:  # pylint: disable=broad-except
                    self.logger.warning(f"No se pudo leer la comision de la orden {order['orderId']}: {e}", False)
                    if not final:
                        return  # se reintenta en el proximo sondeo
            else:
                commission = []

            finished_ms = order.get("updateTime") or order["time"]
            entry = {
                "ts_utc": _ms_to_utc_iso(finished_ms),
                "ts_local": _ms_to_local(finished_ms),
                "created_utc": _ms_to_utc_iso(order["time"]),
                "env": self.env,
                "order_id": order["orderId"],
                "symbol": order["symbol"],
                "side": order["side"],
                "type": order["type"],
                "status": order["status"],
                "limit_price": float(order["price"]),
                "orig_qty": float(order["origQty"]),
                "executed_qty": executed,
                "quote_cost": quote,
                "avg_fill_price": (quote / executed) if executed > 0 else None,
                "commission": commission,
            }
            if self.journal.record(entry):
                self.logger.debug(f"Diario: orden {order['orderId']} ({order['symbol']}) {order['status']} registrada")
        except Exception as e:  # pylint: disable=broad-except
            self.logger.error(f"Error escribiendo el diario de operaciones: {e}", False)

    # --- ordenes ----------------------------------------------------------
    def _poll_order(self, symbol: str, order_id: int, final: bool = False):
        order = self.binance_client.get_order(symbol=symbol, orderId=order_id)
        report = {
            "symbol": order["symbol"],
            "side": order["side"],
            "order_type": order["type"],
            "order_id": order["orderId"],
            "cumulative_quote_asset_transacted_quantity": float(order["cummulativeQuoteQty"]),
            "current_order_status": order["status"],
            "order_price": float(order["price"]),
            # hora de creacion de la orden: sirve para los timeouts de compra/venta
            "transaction_time": order["time"],
        }
        previous = self.cache.orders.get(report["order_id"])
        self.cache.orders[report["order_id"]] = BinanceOrder(report)

        changed = previous is None or previous.status != report["current_order_status"]
        if changed:
            self.logger.debug(f"Orden {order_id} ({symbol}): {report['current_order_status']}")
        if changed and report["current_order_status"] in BALANCE_CHANGING_STATUSES:
            self._invalidate_balances()
        if report["current_order_status"] in TERMINAL_STATUSES:
            self._journal_order(order, final)

    def _drain_final_checks(self):
        """Ultima consulta de cada orden que el bot ya solto (p. ej. cancelada por timeout)."""
        while True:
            try:
                symbol, order_id, attempts = self._final_checks.get_nowait()
            except queue.Empty:
                return
            try:
                self._poll_order(symbol, order_id, final=True)
            except Exception as e:  # pylint: disable=broad-except
                if attempts + 1 < self.FINAL_CHECK_MAX_ATTEMPTS:
                    self._final_checks.put((symbol, order_id, attempts + 1))
                else:
                    self.logger.error(f"No se pudo consultar el estado final de la orden {order_id}: {e}", False)

    def _orders_loop(self):
        while not self._stop.is_set():
            wait = self.ORDER_POLL_SECONDS
            try:
                with self.pending_orders_mutex:
                    pending = self.pending_orders.copy()
                for symbol, order_id in pending:
                    self._poll_order(symbol, order_id)
                self._drain_final_checks()
            except (BinanceRequestException, BinanceAPIException) as e:
                self.logger.error(f"Error consultando ordenes por REST: {e}", False)
                wait = self.ERROR_BACKOFF_SECONDS
            except Exception as e:  # pylint: disable=broad-except
                self.logger.error(f"Error inesperado consultando ordenes: {e}", False)
                wait = self.ERROR_BACKOFF_SECONDS
            self._stop.wait(wait)

    def close(self):
        self._stop.set()