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

from contextlib import contextmanager
import logging
from qthread.stoppable_thread import StoppableThread
from redlock import Redlock
import time


class ExtendableRedlock(Redlock):
    extend_script = """
    if redis.call("get",KEYS[1]) == ARGV[1] then
        return redis.call("pexpire",KEYS[1],ARGV[2])
    else
        return 0
    end"""

    def __init__(self, connection_list, retry_count=None, retry_delay=None):
        super(ExtendableRedlock, self).__init__(connection_list, retry_count, retry_delay)
        self.logger = logging.getLogger(__name__)
        self._autoextend_threads = {}

    def extend_instance(self, server, resource, key, new_ttl):
        try:
            return server.eval(self.extend_script, 1, resource, key, new_ttl)
        except:
            return False

    def extend(self, lock, new_ttl):
        return len(
            [s for s in self.servers if self.extend_instance(s, lock.resource, lock.key, new_ttl)]) >= self.quorum

    def is_valid(self, lock):
        return len([s for s in self.servers if s.get(lock.resource) == lock.key]) >= self.quorum

    @contextmanager
    def autoextend(self, lock, every_ms, new_ttl):
        self.start_autoextend(lock, every_ms, new_ttl)
        yield
        self.stop_autoextend(lock)

    def start_autoextend(self, lock, every_ms, new_ttl):
        self.logger.debug('[{resource}] Autoextending every {every_ms}ms with a ttl of {new_ttl}ms'.format(
            resource=lock.resource, every_ms=every_ms, new_ttl=new_ttl))
        if lock in self._autoextend_threads:
            raise LockAutoextendAlreadyRunning()
        self._autoextend_threads[lock] = AutoExtendableLockThread(self, lock, every_ms, new_ttl)
        self._autoextend_threads[lock].start()

    def stop_autoextend(self, lock):
        self._autoextend_threads[lock].stop()
        self._autoextend_threads[lock].join()
        del self._autoextend_threads[lock]
        self.logger.debug('[{resource}] Stopped autoextending'.format(resource=lock.resource))


class AutoExtendableLockThread(StoppableThread):
    def __init__(self, redlock, lock, every_ms, new_ttl):
        super(AutoExtendableLockThread, self).__init__()
        self.lock = lock
        self.redlock = redlock
        self.every_ms = every_ms
        self.new_ttl = new_ttl

    def mainloop(self):
        self.redlock.extend(self.lock, self.new_ttl)
        time.sleep(float(self.every_ms) / 1000)

class LockAutoextendAlreadyRunning(Exception):
    pass