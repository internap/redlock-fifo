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

import time
from redlock import Redlock, Lock


class ExtensibleRedlock(Redlock):
        extend_script = """
    if redis.call("get",KEYS[1]) == ARGV[1] then
        return redis.call("pexpire",KEYS[1],ARGV[2])
    else
        return 0
    end"""

        def extend_instance(self, server, resource, val, new_ttl):
            try:
                return server.eval(self.extend_script, 1, resource, val, new_ttl)
            except:
                return False

        def extend(self, lock, new_ttl):
            return self.act_on_majority_or_unlock(self.extend_instance, lock.resource, lock.key, new_ttl)

        def act_on_majority_or_unlock(self, action, resource, val, ttl):
            """ Copied from Redlock's lock() as it's not extensible """
            retry = 0
            # Add 2 milliseconds to the drift to account for Redis expires
            # precision, which is 1 millisecond, plus 1 millisecond min
            # drift for small TTLs.
            drift = int(ttl * self.clock_drift_factor) + 2
            while retry < self.retry_count:
                n = 0
                start_time = int(time.time() * 1000)
                for server in self.servers:
                    if action(server, resource, val, ttl):
                        n += 1
                elapsed_time = int(time.time() * 1000) - start_time
                validity = int(ttl - elapsed_time - drift)
                if validity > 0 and n >= self.quorum:
                    return Lock(validity, resource, val)
                else:
                    for server in self.servers:
                        self.unlock_instance(server, resource, val)
                    retry += 1
                    time.sleep(self.retry_delay)
            return False
