from time import sleep

from mock import patch
import redlock
from redlock_fifo.fifo_redlock import FIFORedlock
import test_extensible_redlock
from testutils import FakeRedisCustom, get_servers_pool, TestTimer, ThreadCollection


class FIFORedlockTest(test_extensible_redlock.ExtensibleRedlockTest):
    @patch('redis.StrictRedis', new=FakeRedisCustom)
    def setUp(self):
        self.redlock = FIFORedlock(get_servers_pool(active=1, inactive=0))
        self.redlock_with_51_servers_up_49_down = FIFORedlock(get_servers_pool(active=51, inactive=49))
        self.redlock_with_50_servers_up_50_down = FIFORedlock(get_servers_pool(active=50, inactive=50))

    @patch('redis.StrictRedis', new=FakeRedisCustom)
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

