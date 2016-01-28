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
import unittest
import fakeredis
from hamcrest import assert_that, is_

from mock import patch
from redlock import Redlock, Lock
from redlock_fifo.extendable_redlock import ExtendableRedlock, LockAutoextendAlreadyRunning

from testutils import FakeRedisCustom, get_servers_pool, seconds_to_ms, ms_to_seconds


class ExtendableRedlockTest(unittest.TestCase):
    @patch('redis.StrictRedis', new=FakeRedisCustom)
    def setUp(self):
        self.redlock = ExtendableRedlock(get_servers_pool(active=1, inactive=0))
        self.redlock_with_51_servers_up_49_down = ExtendableRedlock(get_servers_pool(active=51, inactive=49))
        self.redlock_with_50_servers_up_50_down = ExtendableRedlock(get_servers_pool(active=50, inactive=50))

    def tearDown(self):
        fakeredis.DATABASES = {}

    def test_bad_connection_info(self):
        with self.assertRaises(Warning):
            Redlock([{"cat": "hog"}])

    def test_should_be_able_to_lock_a_resource_after_it_has_been_unlocked(self):
        lock = self.redlock_with_51_servers_up_49_down.lock("shorts", 10)
        self.assertIsInstance(lock, Lock)
        self.redlock_with_51_servers_up_49_down.unlock(lock)
        lock = self.redlock_with_51_servers_up_49_down.lock("shorts", 10)
        self.assertIsInstance(lock, Lock)

    def test_safety_property_mutual_exclusion(self):
        """
            At any given moment, only one client can hold a lock.
        """
        lock = self.redlock_with_51_servers_up_49_down.lock("shorts", 100000)
        self.assertIsInstance(lock, Lock)
        bad = self.redlock_with_51_servers_up_49_down.lock("shorts", 10)
        self.assertFalse(bad)

    def test_liveness_property_A_deadlocks_free(self):
        """
            Eventually it is always possible to acquire a lock,
            even if the client that locked a resource crashed or gets partitioned.
        """
        lock_A = self.redlock_with_51_servers_up_49_down.lock("shorts", 500)
        self.assertIsInstance(lock_A, Lock)
        sleep(1)
        lock_B = self.redlock_with_51_servers_up_49_down.lock("shorts", 1000)
        self.assertIsInstance(lock_B, Lock)

    def test_liveness_property_B_fault_tolerance(self):
        """
            As long as the majority of Redis nodes are up, clients are able to acquire and release locks.
        """
        lock_with_majority = self.redlock_with_51_servers_up_49_down.lock("shorts", 100000)
        self.assertIsInstance(lock_with_majority, Lock)

        lock_without_majority = self.redlock_with_50_servers_up_50_down.lock("shorts", 100000)
        self.assertEqual(lock_without_majority, False)

    def test_locks_are_released_when_majority_is_not_reached(self):
        """
            [...] clients that fail to acquire the majority of locks,
            to release the (partially) acquired locks ASAP [...]
        """
        lock = self.redlock_with_50_servers_up_50_down.lock("shorts", 10000)
        self.assertEqual(lock, False)

        for server in self.redlock_with_50_servers_up_50_down.servers:
            self.assertEqual(server.get('shorts'), None)

    def test_avoid_removing_locks_created_by_other_clients(self):
        """
            [...] avoid removing a lock that was created by another client.
        """
        lock_A = self.redlock.lock("shorts", 100000)
        self.assertIsInstance(lock_A, Lock)

        lock_B = Lock(validity=9000, resource='shorts', key='abcde')
        self.redlock.unlock(lock_B)

        for server in self.redlock.servers:
            self.assertEqual(server.get('shorts'), lock_A.key)

    def test_two_at_the_same_time_only_one_gets_it(self):
        threads = []
        threads_that_got_the_lock = []

        def get_lock_and_register(thread_name, redlock, resource, output):
            lock = redlock.lock(resource, 100000)
            if lock:
                output.append(thread_name)

        for i in range(2):
            thread = threading.Thread(
                target=get_lock_and_register, args=(i, self.redlock, 'shorts', threads_that_got_the_lock)
            )
            thread.start()
            threads.append(thread)

        for t in threads:
            t.join()

        self.assertEqual(len(threads_that_got_the_lock), 1)

    def test_a_lock_can_be_extended(self):
        lock = self.redlock_with_51_servers_up_49_down.lock("shorts", 500)

        extended = self.redlock_with_51_servers_up_49_down.extend(lock, 1000)
        self.assertEqual(extended, True)

        sleep(0.6)
        assert_that(self.redlock_with_51_servers_up_49_down.is_valid(lock), is_(True))

    def test_a_lock_cannot_be_extended_if_it_is_expired(self):
        lock = self.redlock_with_51_servers_up_49_down.lock("shorts", 500)
        sleep(0.75)
        extended = self.redlock_with_51_servers_up_49_down.extend(lock, 1000)

        self.assertEqual(extended, False)
        assert_that(self.redlock_with_51_servers_up_49_down.is_valid(lock), is_(False))

    def test_locks_have_a_close_to_ttl_validity(self):
        requested_ttl_ms = 10000
        how_close_ms = 500
        lock_A = self.redlock.lock('pants', requested_ttl_ms)
        self.assertIsInstance(lock_A, Lock)

        self.assertGreaterEqual(lock_A.validity, requested_ttl_ms - how_close_ms)

    def test_a_lock_is_always_valid_within_its_validity_time(self):
        lock = self.redlock.lock('test_regular_lock', seconds_to_ms(30))
        assert_that(self.redlock.is_valid(lock), is_(True))

    def test_a_lock_goes_invalid_after_validity_time(self):
        lock = self.redlock_with_51_servers_up_49_down.lock('test_goes_invalid', 350)
        assert_that(self.redlock_with_51_servers_up_49_down.is_valid(lock), is_(True))

        sleep(ms_to_seconds(lock.validity + 100))
        assert_that(self.redlock_with_51_servers_up_49_down.is_valid(lock), is_(False))

    def test_a_lock_goes_invalid_if_majority_not_attained(self):
        lock = self.redlock_with_51_servers_up_49_down.lock('test_goes_invalid', seconds_to_ms(30))
        assert_that(self.redlock_with_51_servers_up_49_down.is_valid(lock), is_(True))

        twenty_five_servers_where_lock_is_valid = [s for s in self.redlock_with_51_servers_up_49_down.servers if
                                                   s.get(lock.resource) == lock.key][:25]

        for server in twenty_five_servers_where_lock_is_valid:
            server.delete(lock.resource)

        assert_that(self.redlock_with_51_servers_up_49_down.is_valid(lock), is_(False))

    def test_autoextend_automatically_extends_the_lock_expiry(self):
        lock = self.redlock_with_51_servers_up_49_down.lock('test_autoextend', 500)

        with self.redlock_with_51_servers_up_49_down.autoextend(lock, every_ms=200, new_ttl=500):
            sleep(1)

            assert_that(self.redlock_with_51_servers_up_49_down.is_valid(lock), is_(True))

    def test_autoextend_automatically_extends_the_lock_expiry_explicit_start_stop(self):
        lock = self.redlock_with_51_servers_up_49_down.lock('test_autoextend', 500)
        try:
            self.redlock_with_51_servers_up_49_down.start_autoextend(lock, every_ms=200, new_ttl=500)
            sleep(1)

            assert_that(self.redlock_with_51_servers_up_49_down.is_valid(lock), is_(True))
        finally:
            self.redlock_with_51_servers_up_49_down.stop_autoextend(lock)

    def test_autoextending_lock_unable_to_renew(self):
        lock = self.redlock_with_51_servers_up_49_down.lock('test_unable_to_renew', 500)
        with self.redlock_with_51_servers_up_49_down.autoextend(lock, every_ms=100, new_ttl=500):
            for server in self.redlock.servers:
                server.flushall()

            sleep(1)

            assert_that(self.redlock_with_51_servers_up_49_down.is_valid(lock), is_(False))

    def test_autoextend_is_not_valid_if_not_refreshed_fast_enough(self):
        lock = self.redlock.lock('test_should_raise', 150)
        with self.redlock_with_51_servers_up_49_down.autoextend(lock, every_ms=250, new_ttl=150):
            sleep(1)
            assert_that(self.redlock_with_51_servers_up_49_down.is_valid(lock), is_(False))

    def test_autoextend_twice_is_an_error(self):
        lock = self.redlock_with_51_servers_up_49_down.lock('test_autoextend', 500)

        with self.redlock_with_51_servers_up_49_down.autoextend(lock, every_ms=200, new_ttl=500):
            with self.assertRaises(LockAutoextendAlreadyRunning):
                self.redlock_with_51_servers_up_49_down.start_autoextend(lock, every_ms=200, new_ttl=500)

    def test_autoextend_start_stop_start_stop(self):
        lock = self.redlock_with_51_servers_up_49_down.lock('test_autoextend', 500)
        with self.redlock_with_51_servers_up_49_down.autoextend(lock, every_ms=200, new_ttl=500):
            pass

        with self.redlock_with_51_servers_up_49_down.autoextend(lock, every_ms=200, new_ttl=500):
            pass
