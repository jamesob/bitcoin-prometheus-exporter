#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import time
import os
import signal
import sys
import socket
from asyncio import ensure_future, Future

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import lru_cache
from queue import Queue
from typing import Any, Tuple, Optional, Callable, Set, Iterable
from typing import Dict
from typing import List
from typing import Union
from urllib.parse import quote

import bitcoin
import riprova

from bitcoin.rpc import InWarmupError, Proxy
from prometheus_client import start_http_server, Gauge, Counter


logger = logging.getLogger("bitcoin-exporter")


# Create Prometheus metrics to track bitcoind stats.
BITCOIN_BLOCKS = Gauge("bitcoin_blocks", "Block height")
BITCOIN_DIFFICULTY = Gauge("bitcoin_difficulty", "Difficulty")
BITCOIN_PEERS = Gauge("bitcoin_peers", "Number of peers")
BITCOIN_HASHPS_NEG1 = Gauge(
    "bitcoin_hashps_neg1", "Estimated network hash rate per second since the last difficulty change"
)
BITCOIN_HASHPS_1 = Gauge(
    "bitcoin_hashps_1", "Estimated network hash rate per second for the last block"
)
BITCOIN_HASHPS = Gauge(
    "bitcoin_hashps", "Estimated network hash rate per second for the last 120 blocks"
)

BITCOIN_ESTIMATED_SMART_FEE_GAUGES: Dict[int, Gauge] = {}

BITCOIN_WARNINGS = Counter("bitcoin_warnings", "Number of network or blockchain warnings detected")
BITCOIN_UPTIME = Gauge("bitcoin_uptime", "Number of seconds the Bitcoin daemon has been running")

BITCOIN_MEMINFO_USED = Gauge("bitcoin_meminfo_used", "Number of bytes used")
BITCOIN_MEMINFO_FREE = Gauge("bitcoin_meminfo_free", "Number of bytes available")
BITCOIN_MEMINFO_TOTAL = Gauge("bitcoin_meminfo_total", "Number of bytes managed")
BITCOIN_MEMINFO_LOCKED = Gauge("bitcoin_meminfo_locked", "Number of bytes locked")
BITCOIN_MEMINFO_CHUNKS_USED = Gauge("bitcoin_meminfo_chunks_used", "Number of allocated chunks")
BITCOIN_MEMINFO_CHUNKS_FREE = Gauge("bitcoin_meminfo_chunks_free", "Number of unused chunks")

BITCOIN_MEMPOOL_BYTES = Gauge("bitcoin_mempool_bytes", "Size of mempool in bytes")
BITCOIN_MEMPOOL_SIZE = Gauge(
    "bitcoin_mempool_size", "Number of unconfirmed transactions in mempool"
)
BITCOIN_MEMPOOL_USAGE = Gauge("bitcoin_mempool_usage", "Total memory usage for the mempool")

BITCOIN_LATEST_BLOCK_HEIGHT = Gauge(
    "bitcoin_latest_block_height", "Height or index of latest block"
)
BITCOIN_LATEST_BLOCK_WEIGHT = Gauge(
    "bitcoin_latest_block_weight", "Weight of latest block according to BIP 141"
)
BITCOIN_LATEST_BLOCK_SIZE = Gauge("bitcoin_latest_block_size", "Size of latest block in bytes")
BITCOIN_LATEST_BLOCK_TXS = Gauge(
    "bitcoin_latest_block_txs", "Number of transactions in latest block"
)

BITCOIN_NUM_CHAINTIPS = Gauge("bitcoin_num_chaintips", "Number of known blockchain branches")

BITCOIN_TOTAL_BYTES_RECV = Gauge("bitcoin_total_bytes_recv", "Total bytes received")
BITCOIN_TOTAL_BYTES_SENT = Gauge("bitcoin_total_bytes_sent", "Total bytes sent")

BITCOIN_LATEST_BLOCK_INPUTS = Gauge(
    "bitcoin_latest_block_inputs", "Number of inputs in transactions of latest block"
)
BITCOIN_LATEST_BLOCK_OUTPUTS = Gauge(
    "bitcoin_latest_block_outputs", "Number of outputs in transactions of latest block"
)
BITCOIN_LATEST_BLOCK_VALUE = Gauge(
    "bitcoin_latest_block_value", "Bitcoin value of all transactions in the latest block"
)

BITCOIN_BAN_CREATED = Gauge(
    "bitcoin_ban_created", "Time the ban was created", labelnames=["address", "reason"]
)
BITCOIN_BANNED_UNTIL = Gauge(
    "bitcoin_banned_until", "Time the ban expires", labelnames=["address", "reason"]
)

BITCOIN_SERVER_VERSION = Gauge("bitcoin_server_version", "The server version")
BITCOIN_PROTOCOL_VERSION = Gauge("bitcoin_protocol_version", "The protocol version of the server")

BITCOIN_SIZE_ON_DISK = Gauge("bitcoin_size_on_disk", "Estimated size of the block and undo files")

BITCOIN_VERIFICATION_PROGRESS = Gauge(
    "bitcoin_verification_progress", "Estimate of verification progress [0..1]"
)

EXPORTER_ERRORS = Counter(
    "bitcoin_exporter_errors", "Number of errors encountered by the exporter", labelnames=["type"]
)
PROCESS_TIME = Counter(
    "bitcoin_exporter_process_time", "Time spent processing metrics from bitcoin node"
)


BITCOIN_RPC_SCHEME = os.environ.get("BITCOIN_RPC_SCHEME", "http")
BITCOIN_RPC_HOST = os.environ.get("BITCOIN_RPC_HOST", "localhost")
BITCOIN_RPC_PORT = os.environ.get("BITCOIN_RPC_PORT", "8332")
BITCOIN_RPC_USER = os.environ.get("BITCOIN_RPC_USER")
BITCOIN_RPC_PASSWORD = os.environ.get("BITCOIN_RPC_PASSWORD")
BITCOIN_CONF_PATH = os.environ.get("BITCOIN_CONF_PATH")
SMART_FEES = [int(f) for f in os.environ.get("SMARTFEE_BLOCKS", "2,3,5,20").split(",")]
REFRESH_SECONDS = float(os.environ.get("REFRESH_SECONDS", "300"))
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8334"))
RETRIES = int(os.environ.get("RETRIES", 5))
TIMEOUT = int(os.environ.get("TIMEOUT", 30))
NUM_THREADS = int(os.environ.get("NUM_THREADS", 5))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")


RETRY_EXCEPTIONS = (InWarmupError, ConnectionError, socket.timeout)

# TODO: Update RpcResult to be more accurate
RpcResult = Union[Dict[str, Any], List[Any], str, int, float, bool, None]
RpcCall = Tuple[Any, ...]
RpcCallback = Callable[[RpcResult], None]
RpcCallError = Tuple[RpcCall, BaseException]
MetricDefinition = Tuple[RpcCall, List[RpcCallback]]
MetricCollection = List[MetricDefinition]


def exception_count(e: BaseException) -> None:
    err_type = type(e)
    exception_name = err_type.__module__ + "." + err_type.__name__
    EXPORTER_ERRORS.labels(**{"type": exception_name}).inc()


def on_retry(err: Exception, next_try: float) -> None:
    # Count all exceptions, even those that will be retried.
    exception_count(err)
    logger.debug("Retry after exception: %s", err)


def error_evaluator(e: Exception) -> bool:
    return isinstance(e, RETRY_EXCEPTIONS)


@lru_cache(maxsize=1)
def rpc_client_factory():
    # Configuration is done in this order of precedence:
    #   - Explicit config file.
    #   - BITCOIN_RPC_USER and BITCOIN_RPC_PASSWORD environment variables.
    #   - Default bitcoin config file (as handled by Proxy.__init__).
    use_conf = (
        (BITCOIN_CONF_PATH is not None)
        or (BITCOIN_RPC_USER is None)
        or (BITCOIN_RPC_PASSWORD is None)
    )

    if use_conf:
        logger.info("Using config file: %s", BITCOIN_CONF_PATH or "<default>")
        return lambda: Proxy(btc_conf_file=BITCOIN_CONF_PATH, timeout=TIMEOUT)
    else:
        host = BITCOIN_RPC_HOST
        host = "%s:%s@%s" % (quote(BITCOIN_RPC_USER), quote(BITCOIN_RPC_PASSWORD), host)
        if BITCOIN_RPC_PORT:
            host = "%s:%s" % (host, BITCOIN_RPC_PORT)
        service_url = "%s://%s" % (BITCOIN_RPC_SCHEME, host)
        logger.info("Using environment configuration")
        return lambda: Proxy(service_url=service_url, timeout=TIMEOUT)


def rpc_client():
    return rpc_client_factory()()


@riprova.retry(
    timeout=TIMEOUT,
    backoff=riprova.ExponentialBackOff(),
    on_retry=on_retry,
    error_evaluator=error_evaluator,
)
def bitcoinrpc(*args) -> RpcResult:
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("RPC call: " + " ".join(str(a) for a in args))
    rpc_result = rpc_client().call(*args)
    return rpc_result


def get_block(block_hash: str) -> Optional[RpcResult]:
    try:
        return bitcoinrpc("getblock", block_hash, 2)
    except Exception:
        logger.exception("Failed to retrieve block " + block_hash + " from bitcoind.")
        return None


def smartfee_gauge(num_blocks: int) -> Gauge:
    """
    Returns the Prometheus gauge for the smart fee associated with a given number of blocks.
    """
    gauge = BITCOIN_ESTIMATED_SMART_FEE_GAUGES.get(num_blocks)
    if gauge is None:
        gauge = Gauge(
            "bitcoin_est_smart_fee_%d" % num_blocks,
            "Estimated smart fee per kilobyte for confirmation in %d blocks" % num_blocks,
        )
        BITCOIN_ESTIMATED_SMART_FEE_GAUGES[num_blocks] = gauge
    return gauge


#
# Callback functions associated with specific RPC calls. See "build_metric_collection" for usage.
#


def set_meminfo_metrics(result: RpcResult) -> None:
    meminfo: Dict[str, float] = result["locked"]  # type: ignore
    BITCOIN_MEMINFO_USED.set(meminfo["used"])
    BITCOIN_MEMINFO_FREE.set(meminfo["free"])
    BITCOIN_MEMINFO_TOTAL.set(meminfo["total"])
    BITCOIN_MEMINFO_LOCKED.set(meminfo["locked"])
    BITCOIN_MEMINFO_CHUNKS_USED.set(meminfo["chunks_used"])
    BITCOIN_MEMINFO_CHUNKS_FREE.set(meminfo["chunks_free"])


def set_mempool_metrics(result: RpcResult) -> None:
    mempool: Dict[str, float] = result  # type: ignore
    BITCOIN_MEMPOOL_BYTES.set(mempool["bytes"])
    BITCOIN_MEMPOOL_SIZE.set(mempool["size"])
    BITCOIN_MEMPOOL_USAGE.set(mempool["usage"])


def set_blockchaininfo_metrics(result: RpcResult) -> None:
    blockchaininfo: Dict[str, float] = result  # type: ignore
    BITCOIN_BLOCKS.set(blockchaininfo["blocks"])
    BITCOIN_DIFFICULTY.set(blockchaininfo["difficulty"])
    BITCOIN_SIZE_ON_DISK.set(blockchaininfo["size_on_disk"])
    BITCOIN_VERIFICATION_PROGRESS.set(blockchaininfo["verificationprogress"])


def set_nettotals_metrics(result: RpcResult) -> None:
    nettotals: Dict[str, float] = result  # type: ignore
    BITCOIN_TOTAL_BYTES_RECV.set(nettotals["totalbytesrecv"])
    BITCOIN_TOTAL_BYTES_SENT.set(nettotals["totalbytessent"])


def set_banned(result: RpcResult) -> None:
    banned: List[Dict[str, Any]] = result  # type: ignore
    for ban in banned:
        BITCOIN_BAN_CREATED.labels(address=ban["address"], reason=ban["ban_reason"]).set(
            ban["ban_created"]
        )
        BITCOIN_BANNED_UNTIL.labels(address=ban["address"], reason=ban["ban_reason"]).set(
            ban["banned_until"]
        )


def set_networkinfo(result: RpcResult) -> None:
    networkinfo: Dict[str, float] = result  # type: ignore
    BITCOIN_PEERS.set(networkinfo["connections"])
    BITCOIN_SERVER_VERSION.set(networkinfo["version"])
    BITCOIN_PROTOCOL_VERSION.set(networkinfo["protocolversion"])
    if networkinfo["warnings"]:
        BITCOIN_WARNINGS.inc()


def set_latest_block_metrics(result: RpcResult) -> None:
    blockchaininfo: Dict[str, Any] = result  # type: ignore
    latest_block: Dict[str, Any] = get_block(str(blockchaininfo["bestblockhash"]))  # type: ignore
    if latest_block is not None:
        BITCOIN_LATEST_BLOCK_SIZE.set(latest_block["size"])
        BITCOIN_LATEST_BLOCK_TXS.set(latest_block["nTx"])
        BITCOIN_LATEST_BLOCK_HEIGHT.set(latest_block["height"])
        BITCOIN_LATEST_BLOCK_WEIGHT.set(latest_block["weight"])
        inputs, outputs = 0, 0
        value = 0
        for tx in latest_block["tx"]:
            i = len(tx["vin"])
            inputs += i
            o = len(tx["vout"])
            outputs += o
            value += sum(o["value"] for o in tx["vout"])

        BITCOIN_LATEST_BLOCK_INPUTS.set(inputs)
        BITCOIN_LATEST_BLOCK_OUTPUTS.set(outputs)
        BITCOIN_LATEST_BLOCK_VALUE.set(value)


def set_uptime(uptime: RpcResult):
    BITCOIN_UPTIME.set(int(uptime))  # type: ignore


def set_chaintips(chaintips: RpcResult):
    BITCOIN_NUM_CHAINTIPS.set(len(chaintips))  # type: ignore


#
# Factory methods for certain callbacks.
#


def make_set_hashps(metric: Gauge) -> RpcCallback:
    def set_hashps(hashps: RpcResult) -> None:
        metric.set(float(hashps))  # type: ignore

    return set_hashps


def make_set_smartfee(num_blocks: int) -> RpcCallback:
    def set_smartfee(smartfee: RpcResult) -> None:
        fee = smartfee.get("feerate")  # type: ignore
        if fee is not None:
            smartfee_gauge(num_blocks).set(fee)

    return set_smartfee


def build_metric_collection() -> MetricCollection:
    """
    Builds the collection of metrics, which is defined as a
    """
    calls: MetricCollection = [
        (("uptime",), [set_uptime]),
        (("getmemoryinfo", "stats"), [set_meminfo_metrics]),
        (("getblockchaininfo",), [set_blockchaininfo_metrics, set_latest_block_metrics]),
        (("getnetworkinfo",), [set_networkinfo]),
        (("getchaintips",), [set_chaintips]),
        (("getmempoolinfo",), [set_mempool_metrics]),
        (("getnettotals",), [set_nettotals_metrics]),
        (("getnetworkhashps", 120), [make_set_hashps(BITCOIN_HASHPS)]),
        (("getnetworkhashps", -1), [make_set_hashps(BITCOIN_HASHPS_NEG1)]),
        (("getnetworkhashps", 1), [make_set_hashps(BITCOIN_HASHPS_1)]),
        (("listbanned",), [set_banned]),
    ]

    # Add the dynamic smart fee metrics.
    for num_blocks in SMART_FEES:
        smartfee_metric = (("estimatesmartfee", num_blocks), [make_set_smartfee(num_blocks)])
        calls.append(smartfee_metric)

    return calls


def add_task_error_collector(errors: Queue, task: Future, call: RpcCall) -> None:
    def collect_error(completed_task: Future) -> None:
        exception = completed_task.exception()
        if exception is not None:
            exception_count(exception)
            errors.put_nowait((call, exception))

    task.add_done_callback(collect_error)


class MetricState:
    """
    Tracks the state of certain RPC calls and whether they should be enabled or not.
    """

    def __init__(self) -> None:
        self.disabled: Set[RpcCall] = set()

    def is_enabled(self, call: RpcCall) -> bool:
        return call not in self.disabled

    def disable_until_complete(self, task: Future, call: RpcCall) -> None:
        # Don't disable if it's already done.
        if task.done():
            return

        self.disabled.add(call)

        def enable_call(task):
            if call in self.disabled:
                self.disabled.remove(call)

        task.add_done_callback(enable_call)


class MetricRunner:
    """
    Runs all the metrics asynchronously in a ThreadPoolExecutor.
    """

    # Ignore all the main riprova exceptions and exceptions thrown from the callback functions.
    IGNORABLE_EXCEPTIONS = (riprova.exceptions.RetryError,)

    def __init__(self, metrics: MetricCollection, num_threads: int) -> None:
        self.metrics = metrics
        self.state = MetricState()
        self.error_queue: "Queue[RpcCallError]" = Queue()

        logger.info("Starting thread pool with %s threads", num_threads)
        self._executor = ThreadPoolExecutor(num_threads)

    async def refresh_all(self, timeout: Optional[float]) -> None:
        """
        Runs all metrics simultaneously, keeping the overall runtime of this method under the
        timeout value.

        Metrics that do not complete their execution in the timeout duration will be disabled on
        subsequent calls to this method until they complete.

        Args:
            timeout: Number of seconds to wait for results.
        """
        # Start the async tasks all together.
        loop = asyncio.get_event_loop()
        tasks = []
        waiting = []
        for call, callbacks in self.metrics:
            if not self.state.is_enabled(call):
                waiting.append(call)
                continue
            task = loop.run_in_executor(self._executor, run_rpc_call, call, callbacks)
            tasks.append(task)
            task_future = ensure_future(task)
            self.state.disable_until_complete(task_future, call)
            add_task_error_collector(self.error_queue, task_future, call)

        if waiting:
            waiting_calls = [" ".join(str(a) for a in call) for call in waiting]
            logger.warning("Waiting on these calls: %s", waiting_calls)

        if not tasks:
            logger.warning("No tasks started. Still waiting on past tasks.")
            return

        if timeout:
            logger.debug("%s tasks created, starting wait for %s seconds", len(tasks), timeout)
        else:
            logger.debug("%s tasks created, waiting for completion")

        done, pending = await asyncio.wait(tasks, timeout=timeout)

        if pending:
            logger.info("Waiting on %s tasks", len(pending))

        # Log and re-raise exceptions for the main method to handle.
        # NOTE: This only throws the first bad exception of potentially many.
        while not self.error_queue.empty():
            call, exception = self.error_queue.get()
            if not isinstance(exception, self.IGNORABLE_EXCEPTIONS):
                logger.error("Error running call [%s]: %s", call, exception)
                raise exception

    def close(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)


def run_rpc_call(call: RpcCall, callbacks: Iterable[RpcCallback]) -> None:
    """
    Perform an RPC call and call the relevant callbacks.
    """
    start = datetime.now()
    result = bitcoinrpc(*call)

    # Try our best to call all actions and avoid throwing errors.
    for callback in callbacks:
        try:
            callback(result)
        except Exception as ex:
            logger.exception("Ignoring error in callback: %s", call)
            exception_count(ex)

    end = datetime.now()
    duration = (end - start).total_seconds()
    logger.debug("RPC and callbacks [took %s sec]: %s", duration, call)


def create_cleanup_signal_handler(metric_runner: MetricRunner):
    def signal_handler(signal, frame) -> None:
        logger.critical("Received SIGTERM/SIGINT. Exiting.")
        metric_runner.close(wait=False)
        logging.shutdown()
        sys.exit(0)

    return signal_handler


async def main():
    # Start up the server to expose the metrics.
    start_http_server(METRICS_PORT)

    # Create the metric callbacks and runner.
    metrics = build_metric_collection()
    metric_runner = MetricRunner(metrics, NUM_THREADS)

    # Handle SIGTERM gracefully.
    signal_handler = create_cleanup_signal_handler(metric_runner)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    while True:
        process_start = datetime.now()

        # Allow unknown exceptions to crash the process, but try to give better error messages for
        # certain known failures.
        #
        # See MetricRunner.IGNORABLE_EXCEPTIONS for a list of ignored exceptions on RPC calls.
        try:
            await metric_runner.refresh_all(timeout=TIMEOUT)
        except (json.decoder.JSONDecodeError, bitcoin.rpc.JSONRPCError) as e:
            # Definitely happens on bad RPC credentials.
            logger.error("RPC call did not return JSON. Bad credentials? " + str(e))
            sys.exit(1)
        except ValueError as e:
            # The Proxy class throws ValueError when credentials and cookie are missing.
            logger.error("Unhandled error: %s", e)
            sys.exit(2)

        # Track the duration of the refreshes as a metric.
        duration = (datetime.now() - process_start).total_seconds()
        PROCESS_TIME.inc(duration)
        remaining = max(REFRESH_SECONDS - duration, 0)

        logger.info("Refresh took %.1f seconds, sleeping for %.1f seconds", duration, remaining)
        await asyncio.sleep(remaining)


if __name__ == "__main__":
    # Set up logging to look similar to bitcoin logs (UTC).
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ"
    )
    logging.Formatter.converter = time.gmtime
    logger.setLevel(LOG_LEVEL)

    main_task = asyncio.run(main())
