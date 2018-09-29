#!/usr/bin/env python
# coding=utf-8

import collections
import socket
import random
import datetime
import msgpack


class AbstractClient:

    """Abstract client. Contains primitives for implementing functioning clients."""

    def _request(self, message):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.server_address)
        sock.send(msgpack.packb(message, use_bin_type=True))

        buff = bytes()
        while True:
            block = sock.recv(128)
            if not block:
                break
            buff += block
        resp = msgpack.unpackb(buff, encoding='utf-8')
        sock.close()
        if 'type' in resp and resp['type'] == 'redirect':
            self.server_address = tuple(resp['leader'])
            resp = self._request(message)
        return resp

    def _get_state(self):
        """Retrive remote state machine."""
        self.server_address = tuple(random.choice(self.data['cluster']))
        return self._request({'type': 'get'})

    def _append_log(self, payload):
        """Append to remote log."""
        return self._request({'type': 'append', 'data': payload})

    @property
    def diagnostic(self):
        return self._request({'type': 'diagnostic'})

    def config_cluster(self, action, address, port):
        return self._request({'type': 'config', 'action': action, 'address': address, 'port': port})


class RefreshPolicyAlways:

    """Policy to establish when a DistributedDict should update its content.
    This policy requires the client to update at every read.
    This is the default policy.
    """

    def can_update(self):
        return True


class RefreshPolicyLock:

    """Policy to establish when a DistributedDict should update its content.
    This policy requires the client to update whenever the lock is set.
    """

    def __init__(self, status=True):
        self.lock = status

    def can_update(self):
        return self.lock


class RefreshPolicyCount:

    """Policy to establish when a DistributedDict should update its content.
    This policy requires the client to update every `maximum` iterations.
    """

    def __init__(self, maximum=10):
        self.counter = 0
        self.maximum = maximum

    def can_update(self):
        self.counter += 1
        if self.counter == self.maximum:
            self.counter = 0
            return True
        else:
            return False


class RefreshPolicyTime:

    """Policy to establish when a DistributedDict should update its content.
    This policy requires the client to update every `delta` time interval.
    """

    def __init__(self, delta=datetime.timedelta(minutes=1)):
        self.delta = delta()
        self.last_refresh = None

    def can_update(self):
        if self.last_refresh is None or datetime.datetime.now() - self.last_refresh > self.delta:
            self.last_refresh = datetime.datetime.now()
            return True
        else:
            return False


class DistributedDict(collections.UserDict, AbstractClient):

    """Client for zatt instances with dictionary based state machines."""

    def __init__(self, addr, port, append_retry_attempts=3, refresh_policy=RefreshPolicyAlways()):
        super().__init__()
        self.data['cluster'] = [(addr, port)]
        self.append_retry_attempts = append_retry_attempts
        self.refresh_policy = refresh_policy
        self.refresh(force=True)

    def __getitem__(self, key):
        self.refresh()
        return self.data[key]

    def __setitem__(self, key, value):
        self._append_log({'action': 'change', 'key': key, 'value': value})

    def __delitem__(self, key):
        self.refresh(force=True)
        del self.data[self.__keytransform__(key)]
        self._append_log({'action': 'delete', 'key': key})

    def __keytransform__(self, key):
        return key

    def __repr__(self):
        self.refresh()
        return super().__repr__()

    def refresh(self, force=False):
        if force or self.refresh_policy.can_update():
            self.data = self._get_state()

    def _append_log(self, payload):
        for attempt in range(self.append_retry_attempts):
            response = super()._append_log(payload)
            if response['success']:
                break
        # TODO: logging
        return response


if __name__ == '__main__':
    import sys

    if len(sys.argv) == 3:
        d = DistributedDict('localhost', 5255)
        d[sys.argv[1]] = sys.argv[2]
