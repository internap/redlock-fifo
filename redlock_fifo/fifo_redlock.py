import logging
from time import sleep

from redlock_fifo.extensible_redlock import ExtensibleRedlock


class FIFORedlock(ExtensibleRedlock):
    def __init__(self, connection_list, retry_count=1, retry_delay=0.2, fifo_retry_count=30, fifo_retry_delay=0.2, fifo_queue_length=64):
        super(FIFORedlock, self).__init__(connection_list, retry_count, retry_delay)
        self.fifo_retry_count = fifo_retry_count
        self.fifo_retry_delay = fifo_retry_delay
        self.fifo_queue_length = fifo_queue_length
        self.logger = logging.getLogger(__name__)

    def lock(self, resource, ttl):
        self.logger.debug('Locking resource {resource} with ttl {ttl}'.format(resource=resource, ttl=ttl))

        def get_resource_name_with_position(resource, position):
            if position == 0:
                return resource
            else:
                return "{}__{}".format(resource, position)

        current_position = None
        lock = None
        retries = 0

        while current_position is not 0 and retries < self.fifo_retry_count:
            if current_position is not None:
                next_position = current_position - 1
            else:
                next_position = self.fifo_queue_length

            self.logger.debug('Trying to acquire resource {resource} position {pos}, try #{retry}'.format(resource=resource, pos=next_position, retry=retries))
            next_lock = super(FIFORedlock, self).lock(get_resource_name_with_position(resource, next_position), ttl)

            if next_lock:
                retries = 0
                self.logger.debug('Resource {resource} position {pos} acquired.'.format(resource=resource, pos=next_position))
                if lock is not None:
                    self.logger.debug('Releasing previous lock: {lock}'.format(lock=str(lock)))
                    super(FIFORedlock, self).unlock(lock)
                current_position = next_position
                lock = next_lock
            else:
                retries += 1
                sleep(self.fifo_retry_delay)

        if current_position == 0:
            return lock
        else:
            self.logger.debug('Could not get lock on {resource} (position 0) after {tries} tries.'.format(resource=resource, tries=retries))
            if lock is not None:
                self.logger.debug('Releasing previous lock: {lock}'.format(lock=str(lock)))
                super(FIFORedlock, self).unlock(lock)
            return False
