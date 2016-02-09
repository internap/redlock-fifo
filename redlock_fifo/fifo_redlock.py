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

import logging
from time import sleep

from redlock_fifo.extendable_redlock import ExtendableRedlock


class FIFORedlock(ExtendableRedlock):
    def __init__(self, connection_list, retry_count=1, retry_delay=0.2,
                 fifo_retry_count=30, fifo_retry_delay=0.2, fifo_queue_length=64,
                 fifo_ephemeral_ttl_ms=5000):
        super(FIFORedlock, self).__init__(connection_list, retry_count, retry_delay)
        self.fifo_retry_count = fifo_retry_count
        self.fifo_retry_delay = fifo_retry_delay
        self.fifo_queue_length = fifo_queue_length
        self.fifo_ephemeral_ttl_ms = fifo_ephemeral_ttl_ms
        self.logger = logging.getLogger(__name__)

    def lock(self, resource, ttl):
        self.logger.info('[{resource}] Locking with ttl {ttl}ms'.format(resource=resource, ttl=ttl))

        def get_resource_name_with_position(resource, position):
            if position == 0:
                return resource
            else:
                return "{0}__{1}".format(resource, position)

        current_position = None
        lock = None
        retries = 0

        while current_position is not 0 and retries < self.fifo_retry_count:
            if current_position is not None:
                next_position = current_position - 1
            else:
                next_position = self.fifo_queue_length

            if lock is not None:
                super(FIFORedlock, self).extend(lock, self.fifo_ephemeral_ttl_ms)
            next_lock_ttl = ttl if next_position is 0 else self.fifo_ephemeral_ttl_ms
            next_lock = super(FIFORedlock, self).lock(get_resource_name_with_position(resource, next_position), next_lock_ttl)

            if next_lock:
                retries = 0
                if lock is not None:
                    super(FIFORedlock, self).unlock(lock)
                current_position = next_position
                lock = next_lock
            else:
                retries += 1
                sleep(self.fifo_retry_delay)

        if current_position == 0:
            self.logger.info('[{resource}] Lock acquired with validity {validity}ms'.format(resource=resource, validity=lock.validity))
            return lock
        else:
            self.logger.error('[{resource}] Could not acquire lock after {tries} tries'.format(resource=resource, tries=retries))
            if lock is not None:
                super(FIFORedlock, self).unlock(lock)
            return False
