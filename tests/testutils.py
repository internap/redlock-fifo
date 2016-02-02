# Copyright 2016 Internap
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from fakeredis import FakeRedis
import redis
from redlock import Redlock
from redlock_fifo.extendable_redlock import ExtendableRedlock
from time import time
import threading


class FakeRedisCustom(FakeRedis):
    def __init__(self, db=0, charset='utf-8', errors='strict', **kwargs):
        super(FakeRedisCustom, self).__init__(db, charset, errors, **kwargs)

        self.fail_on_communicate = False
        if 'host' in kwargs and kwargs['host'].endswith('.inactive'):
            self.fail_on_communicate = True

    def set(self, name, value, ex=None, px=None, nx=False, xx=False):
        if self.fail_on_communicate:
            raise redis.exceptions.ConnectionError

        return super(FakeRedisCustom, self).set(name, value, ex, px, nx, xx)

    def eval(self, script, nb_of_args, *args):
        if self.fail_on_communicate:
            raise redis.exceptions.ConnectionError

        if script == Redlock.unlock_script:
            if self.get(args[0]) == args[1]:
                return self.delete(args[0])
            else:
                return 0
        elif script == ExtendableRedlock.extend_script:
            if self.get(args[0]) == args[1]:
                return self.pexpire(args[0], args[2])

    def pexpire(self, key, new_expiry_ms):
        return self.expire(key, ms_to_seconds(new_expiry_ms))


def get_servers_pool(active, inactive):
    redis_servers = []

    for i in range(inactive):
        server_name = "server%s.inactive" % i
        redis_servers.append({"host": server_name, "port": 6379, 'db': server_name})

    for i in range(active):
        server_name = "server%s.active" % i
        redis_servers.append({"host": server_name, "port": 6379, 'db': server_name})

    return redis_servers


class ThreadCollection(object):
    def __init__(self):
        self.threads = []

    def start(self, target, *args):
        thread = threading.Thread(target=target, args=args)
        thread.start()
        self.threads.append(thread)

    def join(self):
        for t in self.threads:
            t.join()


class TestTimer(object):
    def __init__(self):
        self.elapsed = time()

    def get_elapsed(self):
        return time()-self.elapsed

def ms_to_seconds(ms):
    return float(ms)/1000


def seconds_to_ms(seconds):
    return seconds*1000
