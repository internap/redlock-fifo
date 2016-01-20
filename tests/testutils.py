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
from redlock_fifo.extensible_redlock import ExtensibleRedlock


class FakeRedisCustom(FakeRedis):
    def __init__(self, db=0, charset='utf-8', errors='strict', **kwargs):
        self.fail_on_communicate = False
        if 'host' in kwargs and kwargs['host'].endswith('.inactive'):
            self.fail_on_communicate = True

        super(FakeRedisCustom, self).__init__(db, charset, errors, **kwargs)

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
        elif script == ExtensibleRedlock.extend_script:
            if self.get(args[0]) == args[1]:
                return self.expire(args[0], args[2])
