# -*- coding: utf-8 -*-
from config import KLINES_CONFIG, MONGO_CLIENT, REDIS_CLIENT, INTERVAL_LENGTH, EXCHANGE_INFO
from utils import get_logger, RateLimiter, PriorityRateLimiter
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError
from motor.motor_asyncio import AsyncIOMotorClient
from redis import StrictRedis, ConnectionPool
import ccxt
import requests
import json
import aiohttp
import time
import asyncio
import traceback
import signal
import platform
import copy

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except Exception as e:
    pass

now = lambda: time.time()
timeout = aiohttp.ClientTimeout(total=600)
OS_TYPE = platform.system().upper()


class Kline:

    start = None
    success_count = 0
    fail_count = 0
    error_count = 0
    coinpair_url = "www.example.com/xxx/xxx"
    reload_list = ["BigOne",'Bitfinex']

    def __init__(self, exchange_code):
        self.name = exchange_code
        self.kline_config = KLINES_CONFIG.pop('exchanges')[exchange_code]
        self.kline_config.update(KLINES_CONFIG)
        self.exchange_config = EXCHANGE_INFO[exchange_code].pop('kline_rate')
        self.exchange_config.update(EXCHANGE_INFO[exchange_code])
        
        self.logging = get_logger(exchange_code)
        for key, value in self.kline_config.items():
            setattr(self, key, value)
        for key, value in self.exchange_config.items():
            setattr(self, key, value) 

        self.sessions = [aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(verify_ssl=False, local_addr=addr)
        ) for addr in self.local_addrs]

        self.wait_interval = (1 / self.rate) / len(self.local_addrs)

        #client = AsyncIOMotorClient(MONGO_CLIENT, maxPoolSize=1000)
        #self.db = client["Klines"]
        self.sr = StrictRedis(connection_pool=ConnectionPool.from_url(REDIS_CLIENT))

        self.datas = self.get_coinpair()
        if self.name in self.reload_list:
            self.reload_market_id()
        self.symbol_id = {_info["pair_name"]: _info["pair_api_name"] for _info in self.datas}

        self.session_distribute()

    def __del__(self):
        for session in self.sessions:
            if session._connector_owner:
                session._connector.close()
                session._connector = None
        self.sessions = []

    async def shutdown(self, signame, loop):
        self.logging.warning('caught {0}'.format(signame))
        tasks = [task for task in asyncio.Task.all_tasks()]  # if task is not asyncio.tasks.Task.current_task()
        list(map(lambda task: task.cancel(), tasks))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        loop.stop()

    def session_distribute(self):
        # 每个ClientSession 平均分配 优先货币对 和 普通货币对
        priority_symbols = []
        common_symbols = []
        for data in self.datas:
            symbol = data["pair_name"]
            base, quote = data["pair_name"].split("/")
            if base in self.priority_coin or quote in self.priority_coin:
                priority_symbols.append(symbol)
            else:
                common_symbols.append(symbol)

        common_distribution = [[] for i in range(len(self.local_addrs))]
        priority_distribution = [[] for i in range(len(self.local_addrs))]
        while common_symbols:
            for index in range(len(self.local_addrs)):
                if not common_symbols:
                    break
                common_distribution[index].append(common_symbols.pop())
        while priority_symbols:
            for index in range(len(self.local_addrs))[::-1]:
                if not priority_symbols:
                    break
                priority_distribution[index].append(priority_symbols.pop())

        self.clients = []
        for index, session in enumerate(self.sessions):
            priority_symbols = priority_distribution[index]
            common_symbols = common_distribution[index]
            limiter_config = {
                "session": session,
                "rate": self.rate,
                "max_tokens": self.max_tokens,
            }
            rate_limiter = RateLimiter(**limiter_config)
            self.clients.append((rate_limiter, common_symbols, priority_symbols))

    def reset_counter(self):
        self.start = now()
        self.success_count = self.fail_count = self.error_count = 0

    def reload_market_id(self):
        exchange = eval("ccxt.{}()".format(self.ccxt_alias))
        exchange.load_markets()
        markets = exchange.markets
        market_alias = exchange.commonCurrencies
        for cp_info in self.datas:
            symbol = cp_info["pair_name"]
            if symbol in markets:
                cp_info["pair_api_name"] = markets[symbol]["id"]
            else:
                base_quote = symbol.split("/")
                base_quote = list(map(lambda x: market_alias[x] if x in market_alias else x, base_quote))
                symbol = "/".join(base_quote)
                if symbol in markets:
                    cp_info["pair_api_name"] = markets[symbol]["id"]
                else:
                    self.logging.exception("Market {} Not Found In CCXT.{}".format(symbol, self.ccxt_alias))

    def get_coinpair(self):
        datas = []
        try:
            r = requests.post(self.coinpair_url, data={'market_code': self.name}, timeout=20)
            datas = json.loads(r.content)["data"]["list"]
            self.logging.debug('Total Coin Pair Count: {}'.format(len(datas)))
            datas = [data for data in datas if data['status'] > 0]
            self.logging.debug('Valid Coin Pair Count: {}'.format(len(datas)))
        except Exception as e:
            self.logging.exception('Exception Occured During Getting Coin Pairs({}): {}'.format(self.name, str(e)))

        if not datas:
            raise Exception('Empty Coin Pairs List Found')
        return datas

    async def worker(self, client, common, priority):
        average = 0
        req_count = 0
        cycle_count = 0
        fail_count = 0
        success_count = 0
        
        total_symbols = common + priority
        local_addr = client.session._connector._local_addr
        addr_index = self.local_addrs.index(local_addr)
        await asyncio.sleep(self.wait_interval * addr_index)

        while True:
            if not cycle_count % 2:
                symbol_tasks = total_symbols
            else:
                symbol_tasks = priority

            for symbol in symbol_tasks:
                intervals = self.get_intervals(cycle_count)
                params_list = self.params_prepare(symbol, intervals)
                for params in params_list:
                    start = now()
                    mongo_key, fetch_kwargs = params
                    response = await self.fetch(client, **fetch_kwargs)

                    if response:
                        self.success_count += 1
                        success_count += 1
                        status = "Success"
                    else:
                        self.fail_count += 1
                        fail_count += 1
                        status = "Fail"
    
                    time_consume = now() - start
                    average = (req_count * average + time_consume) / (req_count + 1)
                    req_count += 1
                    self.logging.info("[{} Get {} {} (Round {}) Spend Time {:.4f}] "
                    "Success: {}, Fail: {}, Average Time Consume: {:.4f} As Yet ".format(
                    local_addr, mongo_key, status, cycle_count + 1, time_consume, success_count, fail_count, average))
                    self.parse_data(mongo_key, response)

            cycle_count += 1

    def params_prepare(self, market, intervals): 
        params_list = []
        for interval in intervals:
            _interval = self.intervals[interval]
            market_id = self.symbol_id[market]
            limit = min(self.max_limit, INTERVAL_LENGTH[interval])

            # 根据特定交易所特定K线的(market_id interval limit)完善请求配置信息
            filled_params = self.params_fill(copy.deepcopy(self.kline_config), market_id, _interval, limit)
            mongo_key = "Kline_{}_{}_{}".format(self.name, market.replace("/", "_"), interval)

            # poloniex 交易所K线请求url需要一个 start 时间戳,从mongo里之前最大时间获得
            if self.name == "Poloniex":
                final_time = self.query_final_time(mongo_key)
                filled_params['url'] = filled_params['url'].format(start = int(float(final_time) / 1000))

            # 根据之前完善好的请求配置信息填写请求表单信息
            fetch_kwargs = {}
            for _property in ("_json", "data", "params", "url"):
                if _property in filled_params:
                    fetch_kwargs[_property] = filled_params[_property]

            params_list.append((mongo_key, fetch_kwargs))
        return params_list

    def get_intervals(self, cycle_count):
        intervals = []
        # Bithumb 不支持5min K线
        if self.name == "Bithumb":
            intervals.append("10min")
        else:
            intervals.append("5min")
        if cycle_count % 5 == 0:
            # Poloniex 不支持1hour K线
            if self.name == "Poloniex":
                intervals.append("30min")
            else:
                intervals.append("1hour")
        if cycle_count % 10 == 0:
            intervals.append("1day")
        return intervals

    async def fetch(self, session, url, data=None, _json=None, params=None):
        try:
            if params:
                get_url = url + "?" + "&".join(map(lambda x: "{}={}".format(x[0], x[1]), params.items()))
            else:
                get_url = url
            session_method = getattr(session, self.method.lower())
            async with await session_method(url, timeout=timeout, ssl=False,
                data=data, json=_json, params=params, headers=self.headers) as response:
                text = await response.text()
                if response.status == 200:
                    self.logging.debug("Success with {} {}:{}...,Total_time: {}"
                        "".format(self.method.upper(), get_url, text[:100], now() - self.start))
                    return json.loads(text)
                else:
                    self.fail_count += 1
                    self.logging.error("Faild with {} {} [Status:{}]:{}, Total_time: {}"
                        "".format(self.method.upper(), get_url, response.status, text, now() - self.start))
                    return False

        except asyncio.TimeoutError:
            self.logging.error("Timeout with {} {} Timeout, Total_time: {}"
                "".format(self.method.upper(), get_url, now() - self.start))
            return False
        except asyncio.CancelledError:
            self.logging.debug("Task with {} {} Canelled".format(self.method.upper(), get_url))
            return False
        except aiohttp.client_exceptions.ClientOSError as e:
            self.logging.error(e)
            self.logging.error("[WinError 10054] 远程主机强迫关闭了一个现有的连接 with {} {} , Total_time: {}"
                "".format(self.method.upper(), get_url, now() - self.start))
            return False
        except Exception as e:
            self.logging.exception('{} with {} {} Exception:{}'.format(self.name, self.method.upper(), get_url, str(e)))
            return False

    def parse_data(self, mongo_key, crude_data):
        try:
            # 提取数据
            data_list = self.get_data(crude_data) if hasattr(self, "get_data") else crude_data

            # 解析数据
            item_list = []
            unique_bar_times = []
            for data in data_list:
                item = {}
                for k, _vk in self.volume_key.items():
                    if isinstance(_vk, type(lambda x: x)):
                        item[k] = _vk(data)
                    else:
                        item[k] = data[_vk]
                item["bar_time"] = int(item["bar_time"])
                item["_id"] = item["bar_time"]
                if item["bar_time"] not in unique_bar_times:
                    unique_bar_times.append(item["bar_time"])
                    item_list.append(item)
            unique_bar_times = sorted(unique_bar_times)
            item_list = sorted(item_list, key=lambda x: x.get("bar_time"))

            # 存储数据
            try:
                redis_key = "_".join(mongo_key.split("_")[:2]).replace("_", ":")
                redis_sub_key = "_".join(mongo_key.split("_")[2:])
                redis_keys = (redis_key, redis_sub_key)
                self.data_save(redis_keys, mongo_key, item_list, unique_bar_times)
            except Exception as sub_err:
                traceback.print_exc()
                self.logging.error('[Kline {} Saving Error]: {}'.format(mongo_key, sub_err))

        except Exception as err:
            traceback.print_exc()
            self.logging.error('[Kline {} Parsing Error]: {}'.format(mongo_key, err))

    def data_save(self, redis_keys, mongo_key, data_list, unique_bar_times):
        redis_key, redis_sub_key = redis_keys
        interval = mongo_key.split("_")[-1]
        max_length = INTERVAL_LENGTH[interval]

        recent_klines = self.sr.hget(*redis_keys)
        recent_klines = eval(recent_klines) if recent_klines else []

        if recent_klines:
            last_redis_time = recent_klines[-1]["bar_time"]
            if last_redis_time in unique_bar_times:
                last_index = unique_bar_times.index(last_redis_time)
                redis_save_list = recent_klines[:-1] + data_list[last_index:]
            else:
                redis_save_list = recent_klines + data_list
        else:
            redis_save_list = data_list
    
        data_length = len(redis_save_list)
        if data_length > max_length:
            redis_save_list = redis_save_list[data_length - max_length:]

        try:
            self.sr.hmset(redis_key, {redis_sub_key: redis_save_list})
            redis_initial = redis_save_list[0]['bar_time']
            self.logging.info("{} Rows Of Data From Timestamp {} Has Been Successfully Saved To Redis:{}:{}"
                "".format(len(redis_save_list), redis_initial, redis_key, redis_sub_key))
        except Exception as err:
            self.logging.error("[Save Data To Redis {}:{}] Error: {}".format(redis_key, redis_sub_key, err))
        
        """
            服务器内存满载，去掉本地mongo存储
        """

        #mongo_col = self.db[mongo_key]
        #mongo_col.create_index([("_id", ASCENDING), ("bar_time", ASCENDING)])

        #recent_bar_times = [data["bar_time"] for data in redis_save_list]

        #not_empty = await mongo_col.find_one()

        #if not_empty:
        #    last_doc_dict = await mongo_col.find({}, {"bar_time": 1, "_id": 0}).sort("$natural", -1).limit(1)[0]
        #    last_mongo_time = last_doc_dict.get('bar_time')
        #    if last_mongo_time in recent_bar_times:
        #        await mongo_col.remove({"bar_time": {"$gte": last_mongo_time}})
        #        mongo_insert_list = redis_save_list[recent_bar_times.index(last_mongo_time):]
        #    else:
        #        mongo_insert_list = redis_save_list
        #else:
        #    mongo_insert_list = redis_save_list

        # try:
        #     await mongo_col.insert_many(mongo_insert_list)
        #     mongo_initial = mongo_insert_list[0]['bar_time']
        #     self.logging.info("{} Rows Of Data From Timestamp {} Has Been Successfully Inserted To Mongo:{}"
        #         "".format(len(mongo_insert_list), mongo_initial, mongo_key))

        # except Exception as err:
        #     self.logging.error("[Save Data To Mongo {}] Error: {}".format(mongo_key, err))

    def get_klines(self):
        self.reset_counter()
        tasks = []
        for _client in self.clients:
            tasks.append(self.worker(*_client))

        loop = asyncio.get_event_loop()
        if OS_TYPE != 'WINDOWS':
            for signame in ('SIGTERM',):  # 'SIGINT',
                loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda: asyncio.ensure_future(self.shutdown(signame, loop))
                )
        try:
            loop.run_until_complete(asyncio.wait(tasks))
        except KeyboardInterrupt as e:
            self.logging.warning('catch KeyboardInterrupt')
            for task in asyncio.Task.all_tasks():
                task.cancel()
            loop.stop()
            loop.run_forever()
        finally:
            loop.close()

        self.logging.info('TIME: {}, Success: {}, Fail: {}, Error: {}'.format(
            now() - self.start, self.success_count, self.fail_count, self.error_count))

    def query_final_time(self, mongo_key):
        # 获取Redis之前最大时间戳
        interval = mongo_key.split("_")[2:][-1]
        if interval == "5min":
            default_range = INTERVAL_LENGTH[interval] * 300
        elif interval == "10min":
            default_range = INTERVAL_LENGTH[interval] * 600
        elif interval == "30min":
            default_range = INTERVAL_LENGTH[interval] * 1800
        elif interval == "1hour":
            default_range = INTERVAL_LENGTH[interval] * 3600
        elif interval == "1day":
            default_range = INTERVAL_LENGTH[interval] * 86400
        default_start = int((time.time() - default_range) * 1000)

        try:
            redis_key = "_".join(mongo_key.split("_")[:2]).replace("_", ":")
            redis_sub_key = "_".join(mongo_key.split("_")[2:])
            redis_keys = (redis_key, redis_sub_key)
            recent_klines = self.sr.hget(*redis_keys)
            if recent_klines:
                recent_klines = eval(recent_klines)
                return recent_klines[-1]["bar_time"]
            else:
                return default_start
        
        except Exception as e:
            self.logging.info("[Function: query_final_time] Error: %s" % e)
            return default_start

    def params_fill(self, params, market, interval, limit):
        # 递归完善请求配置信息
        for k, v in params.items():
            if isinstance(v, dict):
                params[k] = self.params_fill(v, market, interval, limit)
            elif isinstance(v, str):
                if "{limit}" in v:
                    params[k] = params[k].replace("{limit}", str(limit))
                if "{market}" in v:
                    params[k] = params[k].replace("{market}", str(market))
                if "{interval}" in v:
                    params[k] = params[k].replace("{interval}", str(interval))
        return params

if __name__=="__main__":
    kline_exchange = Kline("Bitfinex")
    try:
        kline_exchange.get_klines()
    except Exception as e:
        print(traceback.print_exc())
        kline_exchange.logging.exception('{} get kline exception:{}'.format(kline_exchange.name, str(e)))
    finally:
        del kline_exchange
