import threading
from time import sleep

from mock import patch
import redlock
from redlock_fifo.fifo_redlock import FIFORedlock
import test_extensible_redlock
from testutils import FakeRedisCustom, get_servers_pool


class FIFORedlockTest(test_extensible_redlock.ExtensibleRedlockTest):
    @patch('redis.StrictRedis', new=FakeRedisCustom)
    def setUp(self):
        self.redlock = FIFORedlock(get_servers_pool(active=1, inactive=0))
        self.redlock_with_51_servers_up_49_down = FIFORedlock(get_servers_pool(active=51, inactive=49))
        self.redlock_with_50_servers_up_50_down = FIFORedlock(get_servers_pool(active=50, inactive=50))

    @patch('redis.StrictRedis', new=FakeRedisCustom)
    def test_calls_are_handled_in_order(self):
        threads_that_got_the_lock = []
        threads = []
        thread_names = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

        def get_lock_and_register(thread_name, lock_source, resource_name, output, delay_before_releasing_lock):
            print '%s - %s' % (threading.current_thread(), thread_name)
            lock = lock_source.lock(resource_name, 10000)
            if lock:
                output.append(thread_name)
                sleep(delay_before_releasing_lock)
                lock_source.unlock(lock)

        for t in thread_names:
            connector = FIFORedlock(get_servers_pool(active=1, inactive=0))

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

