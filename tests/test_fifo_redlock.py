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
import mock
import redlock
from redlock_fifo.fifo_redlock import FIFORedlock
from tests import test_extendable_redlock
from tests.testutils import FakeRedisCustom, get_servers_pool, TestTimer, ThreadCollection


class FIFORedlockTest(test_extendable_redlock.ExtendableRedlockTest):
    @mock.patch('redis.StrictRedis', new=FakeRedisCustom)
    def setUp(self):
        self.redlock = FIFORedlock(get_servers_pool(active=1, inactive=0))
        self.redlock_with_51_servers_up_49_down = FIFORedlock(get_servers_pool(active=51, inactive=49))
        self.redlock_with_50_servers_up_50_down = FIFORedlock(get_servers_pool(active=50, inactive=50))

    @mock.patch('redis.StrictRedis', new=FakeRedisCustom)
    def test_call_order_orchestrated(self,
                                     critical_section=lambda lock_source, lock: None,
                                     default_ttl=100):
        connector = FIFORedlock(get_servers_pool(active=1, inactive=0),
                                fifo_queue_length=3,
                                fifo_retry_count=10,
                                fifo_retry_delay=0,
                                retry_delay=.1)

        test_timer = TestTimer()
        shared_memory = []

        def thread_function(name, lock_source):
            lock = lock_source.lock('test_call_order_orchestrated', ttl=default_ttl)
            self.assertTrue(lock)
            shared_memory.append((name, test_timer.get_elapsed()))
            critical_section(lock_source, lock)

        thread_collection = ThreadCollection()
        thread_collection.start(thread_function, 'Thread A', connector)
        sleep(0.05)
        thread_collection.start(thread_function, 'Thread B', connector)
        sleep(0.051)
        thread_collection.start(thread_function, 'Thread C', connector)
        thread_collection.join()

        actual_order = [entry[0] for entry in shared_memory]
        actual_times = [entry[1] for entry in shared_memory]
        self.assertEquals(['Thread A', 'Thread B', 'Thread C'], actual_order)
        self.assertAlmostEqual(0, actual_times[0], delta=0.03)
        self.assertAlmostEqual(0.15, actual_times[1], delta=0.03)
        self.assertAlmostEqual(0.3, actual_times[2], delta=0.03)

    def test_call_order_orchestrated_with_unlock(self):
        def critical_section(lock_source, lock):
            sleep(0.1)
            lock_source.unlock(lock)
        self.test_call_order_orchestrated(critical_section=critical_section, default_ttl=1000)

    @mock.patch('redis.StrictRedis', new=FakeRedisCustom)
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

    @mock.patch('redis.StrictRedis', new=FakeRedisCustom)
    def test_ephemeral_locks_use_the_ephemeral_ttl_while_regular_locks_have_requested_ttl(self):
        """
            pants
            -----
            A       # A locks
            A B     # B locks, now second in queue
            A B C   # C locks, now third in queue
            A B C D # D locks, now fourth in queue. D is retrying much faster than anyone else and with unlimited retries.
            A B C D # After 1 second, the situation is still the same.
            A   C D # B dies unexpectedly, redis removes it due to short TTL
            A C D   # C and D advance one spot
              C D   # A unlocks
            C D     # Within 1 second, C becomes first place
              D     # C unlocks
            D       # Within 1 second, D becomes first place

                    # If D got first place, that means C hasn't correctly secured it's old position while trying to get a new one

        """
        fifo_ephemeral_ttl_ms = 500
        connector = FIFORedlock([{'host': 'localhost'}],
                                fifo_ephemeral_ttl_ms=fifo_ephemeral_ttl_ms)

        locks = dict()

        # A locks
        locks['A'] = connector.lock('pants', 15000)
        self.assertIsInstance(locks['A'], redlock.Lock)
        for server in connector.servers:
            self.assertAlmostEqual(server.pttl('pants'), 15000, 500)

        # B locks, now second in queue
        def get_lock_b(connector):
            try:
                locks['B'] = connector.lock('pants', 20000)
            except:
                pass
        connector2 = FIFORedlock([{'host': 'localhost'}],
                                 fifo_ephemeral_ttl_ms=fifo_ephemeral_ttl_ms)

        thread_B = threading.Thread(target=get_lock_b, args=(connector2, ))
        thread_B.start()

        # C locks, now third in queue
        def get_lock_c(connector):
            locks['C'] = connector.lock('pants', 30000)
        thread_C = threading.Thread(target=get_lock_c, args=(connector, ))
        thread_C.start()

        # D locks, now fourth in queue. D is retrying much faster than anyone else.
        def get_lock_d(connector):
            locks['D'] = connector.lock('pants', 25000)
        connector3 = FIFORedlock([{'host': 'localhost'}],
                                 fifo_retry_delay=0,
                                 fifo_retry_count=2000,
                                 fifo_ephemeral_ttl_ms=1001)

        thread_D = threading.Thread(target=get_lock_d, args=(connector3, ))
        thread_D.start()


        # B dies unexpectedly, redis removes it due to short TTL
        connector2.lock_instance = mock.Mock()
        connector2.lock_instance.side_effect = Exception

        # C and D advance one spot

        # A unlocks
        connector.unlock(locks['A'])

        # Within 2 second, C becomes first place
        sleep(2)
        for server in connector.servers:
            self.assertIn('C', locks)
            self.assertEqual(server.get('pants'), locks['C'].key)

        # C unlocks
        connector.unlock(locks['C'])

        # Within 2 second, D becomes first place
        sleep(2)
        for server in connector.servers:
            self.assertIn('D', locks)
            self.assertEqual(server.get('pants'), locks['D'].key)

        thread_B.join()
        thread_C.join()
        thread_D.join()

