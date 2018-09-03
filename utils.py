# -*- coding: utf-8 -*- 
import os
import logging
from logging.handlers import RotatingFileHandler
import sys
import time
import asyncio

# logging config
BASE_DIR = (os.path.dirname(os.path.abspath(__file__)))

def get_logger(splider_name):
    '''
      日志路径在爬虫同目录的logs目录下
      每个日志文件最多10M
      错误日志单独提取一份
    '''
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    if logger.handlers:
        print("=====loger has handlers=====")
        return logger
    
    formatter = logging.Formatter('%(asctime)s [%(levelname)-8s]: %(message)s')
    
    #handler_file_warning = logging.FileHandler(os.path.join(BASE_DIR,"logs/%s.error.log"%(".".join(splider_name.split('.')[:-1]))))
    handler_file_warning = RotatingFileHandler(os.path.join(BASE_DIR,"logs/{}.error.log".format(splider_name)), 
                                     mode='a', maxBytes=10*1024*1024, backupCount=2, encoding=None, delay=0)
    handler_file_warning.setLevel(logging.WARNING)
    handler_file_warning.setFormatter(formatter)
    logger.addHandler(handler_file_warning)
    
    #handler_file_normal = logging.FileHandler(os.path.join(BASE_DIR,"logs/%s.log"%(".".join(splider_name.split('.')[:-1]))))
    handler_file_normal = RotatingFileHandler(os.path.join(BASE_DIR,"logs/{}.log".format(splider_name)), 
                                     mode='a', maxBytes=10*1024*1024, backupCount=5, encoding=None, delay=0)
    handler_file_normal.setLevel(logging.DEBUG)
    handler_file_normal.setFormatter(formatter)
    logger.addHandler(handler_file_normal)
    
    handler_console = logging.StreamHandler(sys.stdout)
    handler_console.formatter = formatter
    logger.addHandler(handler_console)
    
    return logger


monotonic = lambda : time.monotonic()
class RateLimiter:
    '''
    给异步任务限速
    限制同时最大的任务数和间隔
    '''
    
    def __init__(self,session,rate,max_tokens):
        self.session = session
        self.tokens = self.max_tokens = max_tokens
        self.rate = rate
        self.update_at = monotonic()
        
    async def post(self,*args,**kwargs):
        await self.wait_for_token()
        return self.session.post(*args,**kwargs)
    
    async def get(self,*args,**kwargs):
        await self.wait_for_token()
        return self.session.get(*args,**kwargs)

    async def wait_for_token(self):
        while self.tokens < 1:
            self.add_new_tokens()
            await asyncio.sleep(1 / self.rate) #1.0/self.rate
        self.tokens -= 1
        
    def add_new_tokens(self):
        now = monotonic()
        time_since_update = now - self.update_at
        new_tokens = time_since_update*self.rate
        if new_tokens > 1:
            self.tokens = min(self.tokens + new_tokens,self.max_tokens)
            self.update_at = now


class Tier:

    def __init__(self, rate=0, max_tokens=0, symbols=[]):
        self.tokens = max_tokens
        self.max_tokens = max_tokens
        self.rate = rate
        self.symbols = symbols
        self.update_at = monotonic()


class PriorityRateLimiter:
    '''
    给异步任务限速
    限制同时最大的任务数和间隔
    '''
    update_at = monotonic()
    
    def __init__(self, session, rate, max_tokens, cmp_rate, priority, common, second_limit=0):
        self.session = session
        self.max_tokens = max_tokens
        self.max_rate = rate
        self.cmp_rate = cmp_rate
        self.total_symbols = priority + common
        self.priority_symbols = priority
        self.common_symbols = common
        self.second_limit = second_limit
        self.tokens_distribute()

    def tokens_distribute(self):
        _p = len(self.priority_symbols)
        _t = len(self.total_symbols)
        _c = self.cmp_rate
        _m = self.max_tokens
        priority_tokens = "{:.2f}".format((_m * _p * _c) / (_p * _c - _p + _t))
        priority_rate = "{:.2f}".format((_c / (1 + _c)) * self.max_rate)
        common_tokens = self.max_tokens - float(priority_tokens)
        common_rate = self.max_rate - float(priority_rate)

        self.tiers = {
            'priority': Tier(float(priority_rate), float(priority_tokens), self.priority_symbols),
            'common': Tier(common_rate, common_tokens, self.common_symbols)
        }

    async def second_throttle(self, level):
        if self.second_limit:
            tier = self.tiers[level]
            elape = monotonic() - self.update_at
            while elape < self.second_limit:
                print("await asyncio.sleep({})".format(self.second_limit - elape))
                await asyncio.sleep(self.second_limit - elape)
                self.add_new_tokens(level)
                elape = monotonic() - self.update_at
            self.update_at = monotonic()
            tier.update_at = monotonic()

    async def post(self,*args,**kwargs):
        symbol = kwargs.pop("symbol")
        if symbol in self.priority_symbols:
            level = "priority"
        elif symbol in self.common_symbols:
            level = "common"
        await self.wait_for_token(level)
        return self.session.post(*args,**kwargs)

    async def get(self,*args,**kwargs):
        symbol = kwargs.pop("symbol")
        if symbol in self.priority_symbols:
            level = "priority"
        elif symbol in self.common_symbols:
            level = "common"
        await self.wait_for_token(level)
        return self.session.get(*args,**kwargs)

    async def wait_for_token(self, level):
        tier = self.tiers[level]
        while tier.tokens < 1:
            self.add_new_tokens(level)
            await asyncio.sleep(1 / tier.rate) #1.0/self.rate
        tier.tokens -= 1
        await self.second_throttle(level)

    def add_new_tokens(self, level):
        tier = self.tiers[level]
        now = monotonic()
        time_since_update = now - tier.update_at
        new_tokens = time_since_update * tier.rate
        if tier.tokens + new_tokens >= 1:
            tier.tokens = min(tier.tokens + new_tokens, self.max_tokens)
            tier.update_at = now


if __name__ == "__main__":
    import aiohttp
    async def main(client):
            
            # Watch out for the extra 'await' here!
            async with  client.get('https://httpbin.org/uuid') as resp:
                print(await resp.json())
    
    _client=aiohttp.ClientSession()    
    client = RateLimiter(_client,10,10)    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(client))
    _client._connector.close()