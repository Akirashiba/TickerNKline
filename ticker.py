# -*- coding: utf-8 -*-
from config import TICKERS_CONFIG, MONGO_CLIENT, REDIS_CLIENT, EXCHANGE_INFO
from utils import get_logger, RateLimiter
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError
from requests.exceptions import Timeout, HTTPError
from redis import StrictRedis, ConnectionPool
from requests import Session
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
import re

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except Exception as e:
    pass

milliseconds = lambda: int(time.time() * 1000)
seconds = lambda: int(time.time())
async_timeout = aiohttp.ClientTimeout(total=600)
timeout = 60
OS_TYPE = platform.system().upper()
CURRENCY_ALIAS = {
    'usd': ['usd', 'usdt', 'zusd'],
    'cny': ['bnc'],
    'btc': ['btc', 'xbt', 'xxbt']
}
BASE_CURRENCIES = ['usd', 'usdt', 'btc']


def params_url(url, params):
    if not params:
        return url
    return url + "?" + "&".join(map(lambda x: "{}={}".format(x[0], x[1]), params.items()))

class Ticker(object):

    lastRestRequestTime = 0
    success_count = 0
    fail_count = 0
    error_count = 0
    session = Session()

    def __init__(self, exchange_code):
        self.name = exchange_code
        self.ticker_config = TICKERS_CONFIG.pop('exchanges')[exchange_code]
        self.ticker_config.update(TICKERS_CONFIG)
        self.exchange_config = EXCHANGE_INFO[exchange_code].pop('ticker_rate')
        self.exchange_config.update(EXCHANGE_INFO[exchange_code])
        self.logging = get_logger(exchange_code)

        for key, value in self.ticker_config.items():
            setattr(self, key, value)
        for key, value in self.exchange_config.items():
            setattr(self, key, value)

        client = MongoClient(MONGO_CLIENT)
        self.db = client["Tickers"]
        self.sr = StrictRedis(connection_pool=ConnectionPool.from_url(REDIS_CLIENT))
  
        self.datas = self.get_coinpair()
        if hasattr(self, 'get_market_id'):
            self.reload_market_id()

        if hasattr(self, "market_format"):
            self.id_symbol = {self.market_format(_info["pair_name"]): _info["pair_name"] for _info in self.datas}
        else:
            self.id_symbol = {_info["pair_api_name"]: _info["pair_name"] for _info in self.datas}

        self.currencies += BASE_CURRENCIES
        for currency in self.currencies:
            to_cny = float(self.sr.hget('rate:rmb', currency) or '1')
            setattr(self, currency, to_cny)

    def __del__(self):
        if self.session:
            self.session.close()

    def get_coinpair(self):
        datas = []
        try:
            r = requests.post(TICKERS_CONFIG.get(
                    'galaxy_url', "https://galaxy.sandyvip.com/api/coinpair/"
                ), data={'market_code': self.name}, timeout=20)
            datas = json.loads(r.content)["data"]["list"]
            self.logging.debug('Total Coin Pair Count: {}'.format(len(datas)))
            datas = [data for data in datas if data['status'] > 0]
            self.logging.debug('Valid Coin Pair Count: {}'.format(len(datas)))
        except Exception as e:
            self.logging.exception('Exception Occured During Getting Coin Pairs({}): {}'.format(self.name, str(e)))

        if not datas:
            raise Exception('Empty Coin Pairs List Found')
        return datas

    def reset_counter(self):
        self.success_count = self.fail_count = self.error_count = 0

    def reload_market_id(self):
        for cp_info in self.datas:
            symbol = cp_info["pair_name"]
            cp_info["pair_api_name"] = self.get_market_id(symbol)

    def fetch(self, url, headers=None, data=None, params=None):
        try:
            self.throttle()
            self.lastRestRequestTime = seconds()
            response = self.session.request(self.method.upper(), url, timeout=timeout, data=data, params=params, headers=headers)
            text = response.text
            if response.status_code == 200:
                self.logging.debug("Success with {} {}:{}...,Total_time: {}"
                    "".format(self.method.upper(), params_url(url, params), text[:100], seconds() - self.lastRestRequestTime))
            else:
                self.logging.error("Faild with {} {} [Status:{}]:{}, Total_time: {}"
                    "".format(self.method.upper(), params_url(url, params), response.status_code, text, seconds() - self.lastRestRequestTime))
            if self.name == "ZB":
                pattern = re.search(r'{.*}', text)
                text = pattern.group()
            return json.loads(text)

        except Timeout:
            self.logging.error("[{} Ticker Fetching Error] Timeout with {} {} Timeout, Total_time: {}"
                "".format(self.name, self.method.upper(), params_url(url, params), seconds() - self.lastRestRequestTime))

        except Exception as e:
            self.logging.exception('[{} Ticker Fetching Error] {} {} :{}'.format(self.name, self.method.upper(), params_url(url, params), str(e)))

    def parse_data(self, crude_data):
        if "timestamp" not in self.volume_key:
            timestamp = milliseconds()
        try:
            # 提取数据
            if hasattr(self, "get_data"):
                processed_data = self.get_data(crude_data)
            else:
                processed_data = crude_data

            if isinstance(processed_data, dict):
                if 'date' in processed_data:
                    timestamp = processed_data.pop('date')
                processed_data = [(k, v) for k, v in processed_data.items() if isinstance(v, dict)]
            elif not isinstance(processed_data, list):
                raise Exception("Invalid Crude Data Found")

            for data in processed_data:
                # 解析数据
                item = {}
                for k, _vk in self.volume_key.items():
                    if isinstance(_vk, type(lambda x: x)):
                        item[k] = _vk(data)
                    else:
                        item[k] = data[_vk]

                if "timestamp" not in self.volume_key:
                    item["timestamp"] = timestamp

                market_id = item.pop("symbol")
                if market_id in self.id_symbol:
                    symbol = self.id_symbol[market_id]
                elif market_id.upper() in self.id_symbol:
                    symbol = self.id_symbol[market_id.upper()]
                elif market_id.lower() in self.id_symbol:
                    symbol = self.id_symbol[market_id.lower()]
                else:
                    self.logging.exception("MarketId: {} Not Found".format(market_id))
                    self.error_count += 1
                    continue
                kname = 'Market_{}_{}'.format(self.name, symbol.replace("/", "_"))

                if "CNY_price" not in item:
                    quote = symbol.split("/")[-1].lower()
                    item = self.update_item(item, quote)

                # 保存数据
                # try:
                #     self.sr.hmset(kname, item)
                #     self.db[kname].insert_one(item)
                # except Exception as sub_err:
                #     self.logging.exception('[Ticker {} Saving Error]: {}'.foramt(kname, sub_err))

                self.success_count += 1

        except Exception as err:
            self.logging.exception('[Ticker {} Parsing Error]: {}'.format(kname, err))
            self.fail_count += 1

    def get_tickers(self):
        self.reset_counter()
        markets = ','.join([data['pair_api_name'] for data in self.datas])

        _config = self.params_fill(copy.deepcopy(self.ticker_config), markets)
        _url = _config['url']

        # 根据之前完善好的请求配置信息填写请求表单信息
        fetch_kwargs = {"headers": self.headers}
        for _property in ("data", "params"):
            if _property in _config:
                fetch_kwargs[_property] = _config[_property]

        crude_data = self.fetch(_url, **fetch_kwargs)
        self.parse_data(crude_data)

        self.logging.info('TIME: {}, Success: {}, Fail: {}, Error: {}'.format(
            seconds() - self.lastRestRequestTime, self.success_count, self.fail_count, self.error_count))

    def update_item(self, item, quote):
        if quote in self.currencies or self.deep_contain(CURRENCY_ALIAS, quote):
            to_cny = getattr(self, quote)
            usd_to_cny = getattr(self, "usd")
            btc_to_cny = getattr(self, "btc")
            temp = {}
            if quote in CURRENCY_ALIAS['cny']:
                temp["CNY_price"] = item['price']
                temp["USD_price"] = float(item['price']) / usd_to_cny
                temp["BTC_price"] = float(item['price']) / btc_to_cny
                temp['CNY_highest_price'] = item['highest_price']
                temp['CNY_lowest_price'] = item['lowest_price']

            elif quote in CURRENCY_ALIAS['usd']:
                temp["CNY_price"] = float(item['price']) * usd_to_cny
                temp["USD_price"] = item['price']
                temp["BTC_price"] = float(item['price']) * (usd_to_cny / btc_to_cny)
                temp['CNY_highest_price'] = float(item['highest_price']) * usd_to_cny
                temp['CNY_lowest_price'] = float(item['lowest_price']) * usd_to_cny

            elif quote in CURRENCY_ALIAS['btc']:
                temp["CNY_price"] = float(item['price']) * btc_to_cny
                temp["USD_price"] = float(item['price']) * (btc_to_cny / usd_to_cny)
                temp["BTC_price"] = item['price']
                temp['CNY_highest_price'] = float(item['highest_price']) * btc_to_cny
                temp['CNY_lowest_price'] = float(item['lowest_price']) * btc_to_cny

            else:
                temp["CNY_price"] = float(item['price']) * to_cny
                temp["USD_price"] = float(item['price']) * (to_cny / usd_to_cny)
                temp["BTC_price"] = float(item['price']) * (to_cny / btc_to_cny)
                temp['CNY_highest_price'] = float(item['highest_price']) * to_cny
                temp['CNY_lowest_price'] = float(item['lowest_price']) * to_cny

            for k in temp.keys():
                temp[k] = "{:.8f}".format(float(temp[k]))
            item.update(temp)
        return item

    def throttle(self):
        now = float(seconds())
        elapsed = now - self.lastRestRequestTime
        if elapsed < self.rate:
            delay = self.rate - elapsed
            time.sleep(delay)

    def params_fill(self, params, market):
        # 递归完善请求配置信息
        for k, v in params.items():
            if isinstance(v, dict):
                params[k] = self.params_fill(v, market)
            elif isinstance(v, str):
                if "{market}" in v:
                    params[k] = params[k].replace("{market}", str(market))
        return params

    def deep_contain(self, obj, abt):
        #递归查找
        if isinstance(obj, dict):
            for k, v in obj.items():
                if self.deep_contain(v, abt):
                    return True
        elif isinstance(obj, list):
            if abt in obj:
                return True
        return False


class AsyncTicker(Ticker):

    def __init__(self, exchange_code):

        super(AsyncTicker, self).__init__(exchange_code)
        super(AsyncTicker, self).__del__()

        self.sessions = [aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False, local_addr=addr)) for addr in self.local_addrs]
        self.clients = [RateLimiter(session, self.rate, self.max_tokens) for session in self.sessions]

    def __del__(self):
        for session in self.sessions:
            if session._connector_owner:
                session._connector.close()
                session._connector = None
        self.sessions = []

    async def fetch(self, session, url, kname, headers=None, data=None, params=None):
        try:
            session_method = getattr(session, self.method.lower())
            async with await session_method(url, timeout=async_timeout, ssl=False, data=data, params=params, headers=headers) as response:
                text = await response.text()
                if response.status == 200:
                    self.logging.debug("Success with {} {}:{}...,Total_time: {}"
                        "".format(self.method.upper(), params_url(url, params), text[:100], seconds() - self.lastRestRequestTime))
                else:
                    self.fail_count += 1
                    self.logging.error("Faild with {} {} [Status:{}]:{}, Total_time: {}"
                        "".format(self.method.upper(), params_url(url, params), response.status, text, seconds() - self.lastRestRequestTime))
                if self.name == "ZB":
                    pattern = re.search(r'{.*}', text)
                    text = pattern.group()
                self.parse_data(kname, json.loads(text))

        except asyncio.TimeoutError:
            self.logging.error("Timeout with {} {} Timeout, Total_time: {}"
                "".format(self.method.upper(), params_url(url, params), seconds() - self.lastRestRequestTime))
            self.error_count += 1
        except asyncio.CancelledError:
            self.logging.debug("Task with {} {} Canelled".format(self.method.upper(), params_url(url, params)))
            self.error_count += 1
        except aiohttp.client_exceptions.ClientOSError:
            self.logging.error("[WinError 10054] 远程主机强迫关闭了一个现有的连接 with {} {} , Total_time: {}"
                "".format(self.method.upper(), params_url(url, params), seconds() - self.lastRestRequestTime))
            self.error_count += 1
        except Exception as e:
            self.logging.exception('{} with {} {} Exception:{}'.format(self.name, self.method.upper(), params_url(url, params), str(e)))
            self.error_count += 1

    def parse_data(self, kname, crude_data):
        try:
            # 提取数据
            if hasattr(self, "get_data"):
                processed_data = self.get_data(crude_data)
            else:
                processed_data = crude_data

            # 解析数据
            item = {}
            for k, _vk in self.volume_key.items():
                if isinstance(_vk, type(lambda x: x)):
                    item[k] = _vk(processed_data)
                else:
                    item[k] = processed_data[_vk]

            if "timestamp" not in self.volume_key:
                item['timestamp'] = milliseconds()

            if "CNY_price" not in item:
                quote = kname.split("_")[-1].lower()
                item = self.update_item(item, quote)

            # 保存数据
            # try:
            #     self.sr.hmset(kname, item)
            #     self.db[kname].insert_one(item)
            # except Exception as sub_err:
            #     self.logging.exception('[Ticker {} Saving Error]: {}'.foramt(kname, sub_err))
            self.success_count += 1

        except Exception as err:
            traceback.print_exc()
            self.logging.error("[Function: parse_data] error: {}".format(err))

    def get_tickers(self):
        self.reset_counter()
        tasks = []
        for i, data in enumerate(self.datas):
            _market = data['pair_name']
            _market_id = data['pair_api_name']
            _market = _market.replace("/", "_")

            _config = self.params_fill(copy.deepcopy(self.ticker_config), _market_id)
            _url = _config['url']

            # 根据之前完善好的请求配置信息填写请求表单信息
            fetch_kwargs = {"headers": self.headers}
            for _property in ("data", "params"):
                if _property in _config:
                    fetch_kwargs[_property] = _config[_property]

            kname = "Market_{}_{}".format(self.name, _market)
            tasks.append(self.fetch(self.clients[i % len(self.clients)], _url, kname, **fetch_kwargs))

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
            seconds() - self.lastRestRequestTime, self.success_count, self.fail_count, self.error_count))


def choose_ticker(exchange_code):
    ticker_rate = EXCHANGE_INFO[exchange_code]['ticker_rate']
    if "max_tokens" in ticker_rate:
        return AsyncTicker(exchange_code)
    else:
        return Ticker(exchange_code)


if __name__=="__main__":
    ticker_exchange = choose_ticker("BigOne")
    try:
        for i in range(1, 5):
            ticker_exchange.get_tickers()
    except Exception as e:
        traceback.print_exc()
        ticker_exchange.logging.exception('{} get tickers exception:{}'.format(ticker_exchange.name, str(e)))
    finally:
        del ticker_exchange