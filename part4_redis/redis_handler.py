import redis
from time import time, sleep
import json
from datetime import datetime
from dateutil.parser import parse
import re

class RedisHandler(object):

    def __init__(self, exp_duration=3600*24*7):
        self.client = redis.Redis(host='localhost', port=6379, db=0)
        self.exp_duration = exp_duration
        self._set_mappings()

    def _set_mappings(self):
        day_diffs = list(range(-29, 31))
        day_strs = ['%02d' % n for n in list(range(1, 31))]*2
        self.day_mapping = dict(zip(day_diffs, day_strs))
        hour_diffs = list(range(-24, 24))
        hour_strs = ['%02d' % n for n in list(range(0, 24))] * 2
        self.hour_mapping = dict(zip(hour_diffs, hour_strs))
        month_diffs = list(range(-11, 13))
        month_strs = ['%02d' % n for n in list(range(1, 13))] * 2
        self.month_mapping = dict(zip(month_diffs, month_strs))

    def set_toList(self, key, value, max_len):
        self.client.lpush(key, *value)
        len_key = self.client.llen(key)
        for _ in range(len_key-max_len):
            last = self.client.rpop(key)

    def set_singleKey(self, key, value, key_time, tdate):
        key = self.set_name(key, key_time, tdate)
        exists = self.client.exists(key)
        if exists:
            self.client.incr(key, value)
        else:
            self.client.set(key, value)
            exp_time = self.set_expTime(key_time, tdate)
            self.client.expire(key, exp_time)

    def set_withTime(self, key_list, value, timestamp, key_times, hashtag_in=False, symbol_in=False):
        tdate = parse(timestamp).replace(tzinfo=None)
        for key in key_list:
            for key_time in key_times:
                self.set_singleKey(key, value, key_time, tdate)
                if hashtag_in:
                    self.set_singleKey('totalHashtag', value, key_time, tdate)
                if symbol_in:
                    self.set_singleKey('totalSymbols', value, key_time, tdate)
        for key_time in key_times:
            totalPost_key = self.set_name('totalPosts', key_time, tdate)
            if self.client.exists(totalPost_key):
                self.client.incr(totalPost_key, value)
            else:
                self.client.set(totalPost_key, value)
                exp_time = self.set_expTime(key_time, tdate)
                self.client.expire(totalPost_key, exp_time)

    def set_withoutTime(self, key, value, exp_duration):
        self.client.set(key, value, ex=exp_duration)

    def set_key(self, key, value, set_type, timestamp, **kwargs):
        if set_type == 'withtime':
            times = ['year', 'month', 'day', 'hour']
            if 'key_times' in kwargs:
                key_times = []
                for t in kwargs['key_times']:
                    if t in times:
                        key_times.append(t)
            else:
                key_times = times
            if 'hashtag_in' in kwargs:
                self.set_withTime(key, value, timestamp, key_times, hashtag_in=kwargs['hashtag_in'])
            if 'symbol_in' in kwargs:
                self.set_withTime(key, value, timestamp, key_times, symbol_in=kwargs['symbol_in'])
            else:
                self.set_withTime(key, value, timestamp, key_times)
        else:
            self.set_withoutTime(key, value, self.exp_duration)

    @staticmethod
    def set_expTime(key_time, tdate):
        day, month = tdate.day, tdate.month
        now = datetime.now()
        delta_seconds = (now-tdate).total_seconds()
        sday = 3600 * 24
        smonth = sday * 30
        year_thresh = 5
        if key_time == 'year':
            temp_exp = (year_thresh-1)*12*smonth + (12-month)*smonth + (30-day)*sday - delta_seconds
        elif key_time == 'month':
            temp_exp = 11*smonth + (30-day)*sday - delta_seconds
        elif key_time == 'day':
            temp_exp = 29*sday + (24-tdate.hour)*3600 + (60-tdate.minute)*60 - delta_seconds
        elif key_time == 'hour':
            temp_exp = 23*3600 + (60-tdate.minute)*60 + 60-tdate.second - delta_seconds
        else:
            exp_time = 1
        if temp_exp > 0:
            exp_time = int(temp_exp)
        else:
            exp_time = 1
        return exp_time

    @staticmethod
    def set_name(prefix, key_time, tdate):
        if key_time == 'year':
            key = f'{prefix}:year:{"%02d" % tdate.year}'
        elif key_time == 'month':
            key = f'{prefix}:month:{"%02d" % tdate.month}'
        elif key_time == 'day':
            key = f'{prefix}:day:{"%02d" % tdate.day}'
        elif key_time == 'hour':
            key = f'{prefix}:hour:{"%02d" % tdate.hour}'
        else:
            key = prefix
        return key

    def get_mapping(self, ttype):
        return self.__getattribute__(f'{ttype}_mapping')

    def get_ntimes(self, n_times, prefix, ttype):
        ctime = datetime.now().__getattribute__(ttype)
        last_ntimes = [self.get_mapping(ttype)[n] for n in range(ctime-n_times+1, ctime+1)]
        # print(last_ntimes)
        key_results = []
        for t in last_ntimes:
            key_results += [e for e in self.client.scan_iter(match=f'{prefix}:{ttype}:{t}')]
        results = [e.decode() for e in self.client.mget(key_results)]
        return results

    def get_list(self, key, lower, upper):
        return [e.decode() for e in self.client.lrange(key, lower, upper)]


def add_toRedis(post, redis_cli, value, **kwargs):
    if post['text']:
        redis_cli.set_key(
            post['keywords']+[post['channel_id'], 'NotEmptyPosts'],
            value, 'withtime',
            post['timestampISO'],
            **kwargs
        )
        redis_cli.set_key(
            post['hashtags'],
            value, 'withtime',
            post['timestampISO'],
            hashtag_in=True,
            **kwargs
        )
        redis_cli.set_key(
            post['symbols'],
            value, 'withtime',
            post['timestampISO'],
            symbol_in=True,
            **kwargs
        )
        redis_cli.set_toList(
            'posts', [post['text']], 100
        )
        if post['hashtags']:
            redis_cli.set_toList(
                'hashtags', post['hashtags'], 1000
            )
        if post['symbols']:
            redis_cli.set_toList(
                'symbols', post['symbols'], 1000
            )
        print('New post added.')
    else:
        redis_cli.set_key(
            ['EmptyPosts'],
            value, 'withtime',
            post['timestampISO']
        )
        print('Its an empty post')


if __name__ == '__main__':
    with open('test_crawl.json') as f:
        messages = json.load(f)['messages']
    redis_client = RedisHandler(exp_duration=3600*24*7)
    value = 1
    post = messages[0]
    key_times = ['year', 'month', 'day', 'hour']
    add_toRedis(post, redis_client, value, key_times=key_times)
    outdays = redis_client.get_ntimes(27, 'bourseprofile', 'day')
