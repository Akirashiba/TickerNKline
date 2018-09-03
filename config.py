# -*- coding: utf-8 -*- 
import time

REDIS_CLIENT = "redis://127.0.0.1:6379/0"

MONGO_CLIENT = "mongodb://127.0.0.1:27017/klines"

INTERVAL_LENGTH = {
    "5min": 3000,
    "10min": 3000,
    "30min": 2000,
    "1hour": 2000,
    "1day": 1000
}

EXCHANGE_INFO = {
    'Binance':{
        # 每分钟 1200 次请求
        'ccxt_alias': 'binance',
        'kline_rate': {
            'cmp_rate': 2,
            'rate': 8,
            'max_tokens': 8,
        },
        'ticker_rate': {
            'rate': 1,
        },
    },
    'Bit-Z':{
        # 每分钟 1000 次请求
        'ccxt_alias': 'bitz',
        'kline_rate': {
            'cmp_rate': 2,
            'rate': 5,
            'max_tokens': 5,
        },
        'ticker_rate': {
            'rate': 2,
            'max_tokens': 5,
        },
        'headers': {
            'User-Agent': "Mozilla/5.0 \(Windows NT 6.1; WOW64\) AppleWebKit/537.36 \(KHTML, like Gecko\) Chrome/39.0.2171.71 Safari/537.36"
        },
    },
    'BigOne':{
        # 每5秒钟 500 次请求
        'ccxt_alias': 'bigone',
        'kline_rate': {
            'cmp_rate': 2,
            'rate': 8,
            'max_tokens': 8,
        },
        'ticker_rate': {
            'rate': 1,
        },
    },
    'Bitfinex':{
        'ccxt_alias': 'bitfinex2',
        'kline_rate': {
            'cmp_rate': 2,
            'rate': 0.3,
            'max_tokens': 2,
            'second_limit': 2,
        },
        'ticker_rate':{
            'rate': 1,
        },
    },
    'Bithumb':{
        # 每秒钟 20 次请求
        'ccxt_alias': 'bithumb',
        'kline_rate': {
            'cmp_rate': 2,
            'rate': 20,
            'max_tokens': 20,
        },
        'ticker_rate':{
            'rate': 1,
        },
    },
    'FCOIN':{
        # 每10秒钟 100 次请求
        'ccxt_alias': 'fcoin',
        'kline_rate': {
            'cmp_rate': 2,
            'rate': 10,
            'max_tokens': 10,
        },
        'ticker_rate': {
            'rate': 10,
            'max_tokens': 10,
        },
    },
    'Kraken':{
        # 初始Counter 15,每次请求减1, 每3秒钟加1
        # 时间足够长的话近似 3秒钟 1次请求
        'ccxt_alias': 'kraken',
        'kline_rate': {
            'cmp_rate': 2,
            'rate': 0.8,
            'max_tokens': 2,
            'second_limit': 3,
        },
        'ticker_rate': {
            'rate': 10,
            'max_tokens': 10,
        },
    },
    'OKCOIN':{
        # 每五分钟 3000 次请求
        'ccxt_alias': 'okcoinusd',
        'kline_rate': {
            'cmp_rate': 2,
            'rate': 8,
            'max_tokens': 8,
        },
        'ticker_rate': {
            'rate': 2,
        },
        'headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded'
        },
    },
    'HitBTC':{
        # 每秒钟 100 次请求
        'ccxt_alias': 'hitbtc2',
        'kline_rate': {
            'cmp_rate': 2,
            'rate': 20,
            'max_tokens': 20,
        },
        'ticker_rate': {
            'rate': 2,
        },
    },
    'Huobi':{
        # 行情API不限制
        'ccxt_alias': 'huobipro',
        'kline_rate': {
            'cmp_rate': 2,
            'rate': 2,
            'max_tokens': 3,
        },
        'ticker_rate': {
            'rate': 2,
            'max_tokens': 3,
        },
    },
    'OKEX':{
        'ccxt_alias': 'okex',
        'kline_rate': {
            'cmp_rate': 2,
            'rate': 8,
            'max_tokens': 8,
        },
        'ticker_rate': {
            'rate': 2,
        },
        'headers':{
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36',
        }
    },
    'Poloniex':{
        # 每秒钟 6 次请求
        'ccxt_alias': 'poloniex',
        'kline_rate': {
            'cmp_rate': 2,
            'rate': 6,
            'max_tokens': 6,
        },
        'ticker_rate': {
            'rate': 2,
        },
    },
    'ZB':{
        # 一分钟 1000 次请求，K线每秒一次
        'ccxt_alias': 'bitkk',
        'kline_rate': {
            'cmp_rate': 2,
            'rate': 8,
            'max_tokens': 8,
        },
        'ticker_rate': {
            'rate': 20,
        }
    }
}

KLINES_CONFIG={
    #本地可用IP列表
    'local_addrs':[("198.2.196.{}".format(index), 0) for index in range(225,230)],
    'cycles':2,
    
    #交易所配置
    'exchanges':{
        #币安配置
        'Binance':{
            'url': "https://api.binance.com/api/v1/klines",
            'priority_coin': ['BTC'],
            'method': "GET",
            'params': {
                'interval': "{interval}",
                'symbol': "{market}",
                'limit': "{limit}"
            },
            'intervals': {
                '5min': "5m",
                '30min': "30m",
                '1hour': "1h",
                '1day': "1d"
            },
            'volume_key': {
                'bar_time': 6,
                'open': 1,
                'close': 4,
                'high': 2,
                'low': 3,
                'volume': 5,
            },
            "max_limit": 1000,
        },
        'Bit-Z':{
            'url': "https://apiv2.bitz.com/Market/kline",
            'priority_coin': ['BTC'],
            'method': 'GET',
            'params': {
                'size': "{limit}",
                'symbol': "{market}",
                'resolution': "{interval}"
            },
            'intervals': {
                '5min': "5min",
                '30min': "30min",
                '1hour': "60min",
                '1day': "1day"
            },
            'get_data': lambda x: x['data']['bars'],
            'volume_key': {
                'bar_time': "time",
                'open': 'open',
                'close': 'close',
                'high': 'high',
                'low': 'low',
                'volume': 'volume',
            },
            'max_limit': 300,
        },
        'BigOne':{
            'url': "https://b1.run/api/graphql",
            'priority_coin': ['BTC'],
            'method': "POST",
            '_json': {
                "operationName": None,
                "variables": {
                    "marketId": "{market}",
                    "period": "{interval}",
                    "startTime": "2015-01-01T00:00:00Z",
                    "endTime": time.strftime("%Y-%m-%dT%H:%M:%SZ")
                },
                "query": "query ($marketId: String!, $period: BarPeriod!, $startTime: DateTime, $endTime: DateTime) {bars(marketUuid: $marketId, period: $period, startTime: $startTime, endTime: $endTime, order: DESC, limit: {limit}) {time open high low close volume __typename}}"
            },
            'intervals': {
                '5min': "MIN5",
                '30min': "MIN30",
                '1hour': "HOUR1",
                '1day': "DAY1"
            },
            'get_data': lambda x: x["data"]["bars"],
            'volume_key': {
                'bar_time': lambda x: (mk_stamp(x['time'].split('.')[0].replace('T', ' ')) + 8 * 3600) * 1000,
                'open': 'open',
                'close': 'close',
                'high': 'high',
                'low': 'low',
                'volume': 'volume',
            },
            'max_limit': 1000,
        },
        'Bitfinex':{
            'url': "https://api.bitfinex.com/v2/candles/trade:{interval}:{market}/hist",
            'priority_coin': ['BTC'],
            'method': "GET",
            'intervals': {
                '5min': "5m",
                '30min': "30m",
                '1hour': "1h",
                '1day': "1D"
            },
            'volume_key': {
                'bar_time': 0,
                'open': 1,
                'close': 2,
                'high': 3,
                'low': 4,
                'volume': 5,
            },
            'max_limit': 99999
        },
        'Bithumb':{
            'url': "https://www.bithumb.com/resources/chart/{market}_xcoinTrade_{interval}.json",
            'priority_coin': ['BTC'],
            'method': "GET",
            'intervals': {
                '10min': "10M",
                '30min': "30M",
                '1hour': "01H",
                '1day': "24H"
            },
            'volume_key': {
                'bar_time': 0,
                'open': 1,
                'close': 2,
                'high': 3,
                'low': 4,
                'volume': 5,
            },
            'max_limit': 99999,
        },
        'FCOIN':{
            'url': "https://api.fcoin.com/v2/market/candles/{interval}/{market}?limit={limit}",
            'priority_coin': ['BTC'],
            'method': "GET",
            'intervals': {
                '5min': "M5",
                '30min': "M30",
                '1hour': "H1",
                '1day': "D1"
            },
            'get_data': lambda x: x["data"],
            'volume_key': {
                'bar_time': lambda x: x['id'] * 1000,
                'open': 'open',
                'close': 'close',
                'high': 'high',
                'low': 'low',
                'volume': 'base_vol',
            },
            'max_limit': 2000,
        },
        'Kraken':{
            'url': "https://api.kraken.com/0/public/OHLC",
            'priority_coin': ['BTC'],
            'method': "POST",
            'data':{
                "pair": "{market}",
                'interval': "{interval}",
            },
            'intervals': {
                '5min': "5",
                '30min': "30",
                '1hour': "60",
                '1day': "1440"
            },
            'get_data': lambda x: list(filter(lambda y: y[0] != "last", list(x['result'].items())))[0][1],
            'volume_key': {
                'bar_time': lambda x: x[0] * 1000,
                'open': 1,
                'close': 4,
                'high': 2,
                'low': 3,
                'volume': 6,
            },
            'max_limit': 720,
        },
        'OKCOIN':{
            'url': "https://www.okcoin.com/api/v1/kline.do?size={limit}&symbol={market}&type={interval}",
            'priority_coin': ['BTC'],
            'method': "GET",
            'intervals': {
                '5min': "5min",
                '30min': "30min",
                '1hour': "1hour",
                '1day': "1day"
            },
            'volume_key': {
                'bar_time': 0,
                'open': 1,
                'close': 4,
                'high': 2,
                'low': 3,
                'volume': 5,
            },
            'max_limit': 2000,
        },
        'HitBTC':{
            'url': "https://api.hitbtc.com/api/2/public/candles/{market}?period={interval}&limit={limit}",
            'priority_coin': ['BTC'],
            'method': "GET",
            'intervals': {
                '5min': "M5",
                '30min': "M30",
                '1hour': "H1",
                '1day': "D1"
            },
            'volume_key': {
                'bar_time': lambda x: (mk_stamp1(x['timestamp'].replace('T', ' ')[:-5]) + 8 * 3600) * 1000,
                'open': 'open',
                'close': 'close',
                'high': 'max',
                'low': 'min',
                'volume': 'volume',
            },
            'max_limit': 1000,
        },
        'Huobi':{
            'url': "https://api.huobipro.com/market/history/kline?period={interval}&size={limit}&symbol={market}",
            'priority_coin': ['BTC'],
            'method': "GET",
            'intervals': {
                '5min': "5min",
                '30min': "30min",
                '1hour': "60min",
                '1day': "1day"
            },
            'get_data': lambda x: x["data"],
            'volume_key': {
                'bar_time': lambda x: int(x['id']) * 1000,
                'open': 'open',
                'close': 'close',
                'high': 'high',
                'low': 'low',
                'volume': 'vol',
            },
            'max_limit': 2000,
        },
        'OKEX':{
            'url': "https://www.okex.cn/v2/spot/markets/kline?type={interval}&limit={limit}&symbol={market}",
            'priority_coin': ['BTC'],
            'method': "GET",
            'intervals': {
                '5min': "5min",
                '30min': "30min",
                '1hour': "1hour",
                '1day': "day"
            },
            'get_data': lambda x: x["data"],
            'volume_key': {
                'bar_time': 'createdDate',
                'open': 'open',
                'close': 'close',
                'high': 'high',
                'low': 'low',
                'volume': 'volume',
            },
            'max_limit': 2000,
        },
        'Poloniex':{
            'url': "https://poloniex.com/public?command=returnChartData&currencyPair={market}&start={start}&end=9999999999&period={interval}",
            'priority_coin': ['BTC'],
            'method': "GET",
            'intervals': {
                '5min': "300",
                '30min': "1800",
                '1hour': "3600",
                '1day': "86400"
            },
            'volume_key': {
                'bar_time': lambda x: x['date'],
                'open': 'open',
                'close': 'close',
                'high': 'high',
                'low': 'low',
                'volume': 'quoteVolume',
            },
            'max_limit': 99999,
        },
        'ZB':{
            'url': "http://api.bitkk.com/data/v1/kline?market={market}&type={interval}",
            'priority_coin': ['BTC'],
            'method': "GET",
            'intervals': {
                '5min': "5min",
                '30min': "30min",
                '1hour': "1hour",
                '1day': "1day"
            },
            'get_data': lambda x: x["data"],
            'volume_key': {
                'bar_time': 0,
                'open': 1,
                'close': 4,
                'high': 2,
                'low': 3,
                'volume': 5,
            },
            'max_limit': 1000,
        }
    },

    'headers':{
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36'
    }
}

TICKERS_CONFIG={
    #本地可用IP列表
    'local_addrs':[("198.2.196.{}".format(index), 0) for index in range(225,230)],
    'cycles':2000,
    
    #交易所配置
    'exchanges':{
        #币安配置
        'Binance':{
            'url': "https://api.binance.com/api/v1/ticker/24hr",
            'method': "GET",
            'volume_key': {
                'symbol': 'symbol',
                'price': 'lastPrice',
                'volume': 'quoteVolume',
                'highest_price': 'highPrice',
                'lowest_price': 'lowPrice',
                'increase': lambda x: float(x['priceChangePercent']) / 100,
                'timestamp': 'closeTime',
            },
            'currencies': ['eth', 'bnb'],
        },
        'Bit-Z':{
            'url': "https://apiv2.bitz.com/Market/ticker?symbol={market}",
            'method': 'GET',
            'volume_key': {
                'price': lambda x: x['data']['now'],
                'volume': lambda x: x['data']['volume'],
                'highest_price': lambda x: x['data']['high'],
                'lowest_price': lambda x: x['data']['low'],
                'increase': lambda x: x['data']['priceChange24h'],
                'CNY_price': lambda x: x['data']['cny'],
                'USD_price': lambda x: x['data']['usd'],
                'CNY_highest_price': lambda x: float(x['data']['high']) / float(x['data']['now']) * float(x['data']['cny']),
                'CNY_lowest_price': lambda x: float(x['data']['low']) / float(x['data']['now']) * float(x['data']['cny']) ,
                'timestamp': lambda x: float(x['time']) * 1000
            },
            'currencies': [],
        },
        'BigOne':{
            'url': "https://big.one/api/v2/tickers",
            'method': "GET",
            'get_data': lambda x: x["data"],
            'volume_key': {
                'symbol': 'market_id',
                'price': lambda x: x['close'] if x['close'] else 0,
                'volume': lambda x: x['volume'] if x['volume'] else 0,
                'highest_price': lambda x: x['high'] if x['high'] else 0,
                'lowest_price': lambda x: x['low'] if x['low'] else 0,
                'increase': lambda x: float(x['daily_change_perc']) / 100,
            },
            'currencies': ['eos', 'eth', 'qtum'],
        },
        'Bitfinex':{
            'url': "https://api.bitfinex.com/v2/tickers",
            'method': "GET",
            'get_market_id': lambda x: "t" + x.replace("/", "").upper(),
            'params': {"symbols": "{market}"},
            'volume_key': {
                'symbol': 0,
                'price': 7,
                'volume': 8,
                'highest_price': 9,
                'lowest_price': 10,
                'increase': 6,
            },
            'currencies': ['eur', 'eth', 'jpy', 'gbp', 'eos'],
        },
        'Bithumb':{
            'url': "https://api.bithumb.com/public/ticker/ALL",
            'method': "GET",
            'get_data': lambda x: x['data'],
            'volume_key': {
                'symbol': lambda x: x[0],
                'price': lambda x: x[1]['closing_price'],
                'volume': lambda x: x[1]['units_traded'],
                'highest_price': lambda x: x[1]['max_price'],
                'lowest_price': lambda x: x[1]['min_price'],
                'increase': lambda x: "{:.8f}".format((float(x[1]['closing_price']) - float(x[1]['opening_price'])) / float(x[1]['opening_price'])),
            },
            'currencies': ['krw']
        },
        'FCOIN':{
            'url': 'https://api.fcoin.com/v2/market/ticker/{market}',
            'method': "GET",
            'get_data': lambda x: x['data']['ticker'],
            'volume_key': {
                'price': lambda x: x[0] if x[0] else 0,
                'volume': 9,
                'highest_price': 7,
                'lowest_price': 8,
                'increase': lambda x: x[0] / x[6] - 1 if x[6] and x[0] else 0,
            },
            'currencies': ['eth']
        },
        'Kraken':{
            'url': "https://api.kraken.com/0/public/Ticker",
            'method': "POST",
            'data':{
                "pair": "{market}",
            },
            'get_data': lambda x: list(x['result'].items())[0][1],
            'volume_key': {
                'price': lambda x: "{:.8f}".format(float(x['c'][0])),
                'volume': lambda x: "{:.8f}".format(float(x['v'][1])),
                'highest_price': lambda x: "{:.8f}".format(float(x['h'][1])),
                'lowest_price': lambda x: "{:.8f}".format(float(x['l'][1])),
                'increase': lambda x: "{:.8f}".format((float(x['c'][0]) - float(x['o'])) / float(x['o'])),
            },
            'currencies': ['eur', 'eth', 'jpy', 'cad', 'gbp'],
        },
        'OKCOIN':{
            'url': "https://www.okcoin.com/v2/spot/markets/tickers",
            'method': "GET",
            'get_data': lambda x: x['data'],
            'volume_key': {
                'symbol': lambda x: x['symbol'].upper(),
                'price': 'last',
                'volume': 'volume',
                'highest_price': 'high',
                'lowest_price': 'low',
                'increase': lambda x: float(x['changePercentage'].replace('%', '').replace('+', '')) / 100,
                'timestamp': 'createdDate',
            },
            'currencies': [],
        },
        'HitBTC':{
            'url': "https://api.hitbtc.com/api/2/public/ticker",
            'method': "GET",
            'volume_key': {
                'symbol': 'symbol',
                'price': 'last',
                'volume': 'volume',
                'highest_price': lambda x: x['high'] if x['high'] else 0,
                'lowest_price': lambda x: x['low'] if x['low'] else 0,
                'increase': lambda x: (float(x['last']) - float(x['open'])) / float(x['open']) if x['open'] else 0,
                'timestamp': lambda x: mk_stamp1(x['timestamp'].replace('T', ' ')[:-5]) * 1000,
            },
            'currencies': ['eth', 'eur'],
        },
        'Huobi':{
            'url': "https://api.huobipro.com/market/detail/merged?symbol={market}",
            'method': "GET",
            'volume_key': {
                'price': lambda x: x['tick']['close'],
                'volume': lambda x: x['tick']['amount'],
                'highest_price': lambda x: x['tick']['high'],
                'lowest_price': lambda x: x['tick']['low'],
                'increase': lambda x: (float(x['tick']['close']) - float(x['tick']['open'])) / float(x['tick']['open']) if x['tick']['open'] else 0,
                'timestamp': lambda x: x['ts'],
            },
            'currencies': ['eth', 'ht'],
        },
        'OKEX':{
            'url': "https://www.okex.cn/v2/spot/markets/tickers",
            'method': "GET",
            'get_data': lambda x: x["data"],
            'volume_key': {
                'symbol': lambda x: x['symbol'].upper(),
                'price': 'last',
                'volume': 'volume',
                'highest_price': 'dayHigh',
                'lowest_price': 'dayLow',
                'increase': lambda x: float(x['changePercentage'].replace('%', '').replace('+', '')) / 100,
                'timestamp': 'createdDate',
            },
            'currencies': ['eth', 'bch'],
        },
        'Poloniex':{
            'url': "https://poloniex.com/public?command=returnTicker",
            'method': "GET",
            'get_data': lambda x: list(x.items()),
            'volume_key': {
                'symbol': lambda x: x[0],
                'price': lambda x: x[1]['last'],
                'volume': lambda x: x[1]['quoteVolume'],
                'highest_price': lambda x: x[1]['high24hr'],
                'lowest_price': lambda x: x[1]['low24hr'],
                'increase': lambda x: x[1]['percentChange'],
            },
            'currencies': ['eth', 'xmr'],
        },
        'ZB':{
            'url': 'https://trans.bitkk.com/line/topall?jsoncallback=jQuery19104507301240911039_%d&_=%d' % (int(time.time()), int(time.time())+1),
            'method': "GET",
            'get_data': lambda x: x["datas"],
            'volume_key': {
                'symbol': lambda x: x['market'].replace('/', '_'),
                'price': 'lastPrice',
                'volume': 'vol',
                'highest_price': 'hightPrice',
                'lowest_price': 'lowPrice',
                'increase': lambda x: float(x['riseRate']) / 100,
            },
            'currencies': [],
        }
    },

    'headers':{
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36'
    }
}

def mk_stamp(dt):
    """convert to timestamp"""
    timeArr = time.localtime(int(time.time()))

    tm_list = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d %H", "%Y-%m-%d"]
    for tm in tm_list:
        try:
            timeArr = time.strptime(dt, tm)
        except:
            continue

    timestamp = int(time.mktime(timeArr))
    return timestamp


def mk_stamp1(dt):
    """convert to timestamp"""
    if len(dt) < 16:
        timeArr = time.strptime(dt, "%Y-%m-%d %H:%M")
    else:
        timeArr = time.strptime(dt, "%Y-%m-%d %H:%M:%S")
    timestamp = int(time.mktime(timeArr))
    return timestamp
