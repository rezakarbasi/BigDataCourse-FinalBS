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

    def set_withTime(self, key_list, value, timestamp):
        key_times = ['year', 'month', 'day', 'hour']
        tdate = parse(timestamp)
        for key in key_list:
            for key_time in key_times:
                self.set_singleKey(key, value, key_time, tdate)
        for key_time in key_times:
            totalPost_key = self.set_name('totalPosts', key_time, tdate)
            totalHashtag_key = self.set_name('totalHashtag', key_time, tdate)
            if self.client.exists(totalPost_key):
                self.client.incr(totalPost_key, value)
            else:
                self.client.set(totalPost_key, value)
                exp_time = self.set_expTime(key_time, tdate)
                self.client.expire(totalPost_key, exp_time)
            if self.client.exists(totalHashtag_key):
                self.client.incr(totalHashtag_key, value)
            else:
                self.client.set(totalHashtag_key, value)
                exp_time = self.set_expTime(key_time, tdate)
                self.client.expire(totalHashtag_key, exp_time)

    def set_withoutTime(self, key, value, exp_duration):
        self.client.set(key, value, ex=exp_duration)

    def set_key(self, key, value, set_type, timestamp):
        if set_type == 'withtime':
            self.set_withTime(key, value, timestamp)
        else:
            self.set_withoutTime(key, value, self.exp_duration)

    @staticmethod
    def set_expTime(key_time, tdate):
        day, month = tdate.day, tdate.month
        sday = 3600 * 24
        smonth = sday * 30
        year_thresh = 5
        if key_time == 'year':
            exp_time = (year_thresh-1)*12*smonth + (12-month)*smonth + (30-day)*sday
        elif key_time == 'month':
            exp_time = 11*smonth + (30-day)*sday
        elif key_time == 'day':
            exp_time = 29*sday + (24-tdate.hour)*3600 + (60-tdate.minute)*60
        elif key_time == 'hour':
            exp_time = 23*3600 + (60-tdate.minute)*60 + 60-tdate.second
        else:
            exp_time = -1
        return exp_time

    @staticmethod
    def set_name(prefix, key_time, tdate):
        if key_time == 'year':
            key = f'{prefix}:year:{tdate.year}'
        elif key_time == 'month':
            key = f'{prefix}:month:{tdate.month}'
        elif key_time == 'day':
            key = f'{prefix}:day:{tdate.day}'
        elif key_time == 'hour':
            key = f'{prefix}:hour:{tdate.hour}'
        else:
            key = prefix
        return key

    def get_mapping(self, ttype):
        return self.__getattribute__(f'{ttype}_mapping')

    def get_ntimes(self, n_times, prefix, ttype):
        ctime = datetime.now().__getattribute__(ttype)
        last_ntimes = [self.get_mapping(ttype)[n] for n in range(ctime-n_times+1, ctime+1)]
        key_results = []
        for t in last_ntimes:
            key_results += [e for e in self.client.scan_iter(match=f'{prefix}:{ttype}:{t}')]
        results = [e.decode() for e in self.client.mget(key_results)]
        return results

    def get_list(self, key, lower, upper):
        return [e.decode() for e in self.client.lrange(key, lower, upper)]


def add_toRedis(post, redis_cli, value):
    if post['emptytext']:
        redis_cli.set_key(
            post['frequency_word']+[post['channel_id'], 'NonEmptyPosts'],
            value, 'withtime',
            post['timestampISO']
        )
        redis_cli.set_toList(
            'posts', [post['emptytext']], 100
        )
        redis_cli.set_toList(
            'hashtags', post['frequency_word'], 1000
        )
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
    add_toRedis(post, redis_client, value)
    outdays = redis_client.get_ntimes(27, 'bourseprofile', 'day')
