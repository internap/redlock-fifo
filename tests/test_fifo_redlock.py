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

import threading
from time import sleep

from mock import patch
import redlock
from redlock_fifo.fifo_redlock import FIFORedlock
import random
import logging
from tests import test_extensible_redlock
from tests.testutils import FakeRedisCustom, get_servers_pool


class FIFORedlockTest(test_extensible_redlock.ExtensibleRedlockTest):
    @patch('redis.StrictRedis', new=FakeRedisCustom)
    def setUp(self):
        self.redlock = FIFORedlock(get_servers_pool(active=1, inactive=0))
        self.redlock_with_51_servers_up_49_down = FIFORedlock(get_servers_pool(active=51, inactive=49))
        self.redlock_with_50_servers_up_50_down = FIFORedlock(get_servers_pool(active=50, inactive=50))

    def test_calls_order_works_multiple_times(self):
        """
        This test ensures that the lock is given in right order even under different threading conditions.
        It can be run manually to ensure the order is respected even with thread locks.
        """
        self.skipTest('Test too long to run ( > 800s)')
        for i in range(0, 100):
            logging.debug("Test run {}".format(i))
            self.calls_are_handled_in_order()

    def test_calls_order_works_one_time(self):
        self.calls_are_handled_in_order()

    @patch('redis.StrictRedis', new=FakeRedisCustom)
    def calls_are_handled_in_order(self):
        threads_that_got_the_lock = []
        threads = []
        thread_names = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

        def get_lock_and_register(thread_name, lock_source, resource_name, output, delay_before_releasing_lock):
            print('%s - %s' % (threading.current_thread(), thread_name))
            lock = lock_source.lock(resource_name, 10000)
            if lock:
                output.append(thread_name)
                sleep(delay_before_releasing_lock * random.uniform(0.1, 1.0))
                lock_source.unlock(lock)
        connector = FIFORedlock(get_servers_pool(active=1, inactive=0))
        for t in thread_names:
            simulate_work_delay = 0.02
            thread = threading.Thread(target=get_lock_and_register, args=(t, connector, 'pants', threads_that_got_the_lock,
                                      simulate_work_delay))
            sleep(0.01)
            thread.start()
            threads.append(thread)

        for t in threads:
            t.join()

        self.assertEqual(''.join(threads_that_got_the_lock), thread_names)

    @patch('redis.StrictRedis', new=FakeRedisCustom)
    def test_locks_are_released_when_position0_could_not_be_reached(self):
        connector = FIFORedlock([{'host': 'localhost', 'db': 'mytest'}],
                                fifo_retry_delay=0)

        lock_A = connector.lock('pants', 10000)
        self.assertIsInstance(lock_A, redlock.Lock)
        lock_B = connector.lock('pants', 10000)
        self.assertEqual(lock_B, False)
        connector.unlock(lock_A)

        for server in connector.servers:
            self.assertEqual(server.keys(), [])

