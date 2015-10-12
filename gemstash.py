#!/usr/bin/env python3

# Copyright 2015 Tracy Poff. See LICENSE for details.

"""
Server-free memcached replacement.

Gemstash provides a simple cache similar to memcached, appropriate for use in
standalone applications which do not have access to a memcached server. Its
usage should be familiar to users of python-memcached.

Usage:

    >>> import gemstash
    >>> gs = gemstash.Client(gemstash.Stash())

Each stash stores its own independent set of keys and values. Two clients
accessing the same stash will get the same values.

    >>> gs.set("foo", "bar", time=0)
    True
    >>> gs.get("foo")
    'bar'
    >>> gs.set("spam", "eggs", 300)
    True
    >>> gs.get("spam")
    'eggs'
    # five minutes later
    >>> print(gs.get("spam"))
    None

Values can be stored with an expiry time, specified in seconds, after which they
will be deleted from the cache (and not returned). Numbers greater than
60*60*24*30 (thirty days) will be interpreted instead as an absolute timestamp
in seconds since January 1, 1970 (epoch time). If the time parameter is set to
0 or omitted, the item will never expire.

"""

import sys
import collections
import datetime
import threading
import uuid

SERVER_MAX_KEY_LENGTH = 250
SERVER_MAX_VALUE_LENGTH = 1024*1024
_DEAD_RETRY = 30  # number of seconds before retrying a dead server.
_SOCKET_TIMEOUT = 3  #  number of seconds before sockets timeout.

class Stash(collections.MutableMapping):
    """A cache, taking place of a memcached server for a gemstash Client."""

    CachedItem = collections.namedtuple('CachedItem', ['value', 'expires', 'cas_id'])

    def __init__(self, *args, **kwargs):
        """Create a new Stash."""
        self.cache = dict()
        self.write_lock = threading.RLock()

    def __getitem__(self, key):
        try:
            item = self.cache[key]
        except KeyError:
            return None
        if item.expires and item.expires < datetime.datetime.now():
            del self.cache[key]
            return None
        else:
            return item.value, item.cas_id

    def __setitem__(self, key, value):
        raise NotImplementedError("Add items to the stash using the set method.")

    def __delitem__(self, key):
        with self.write_lock:
            try:
                del self.cache[key]
            except KeyError:
                pass

    def __iter__(self):
        return iter(self.cache)

    def __len__(self):
        return len(self.cache)

    def incr(self, key, delta):
        with self.write_lock:
            try:
                value, _ = self[key]
            except TypeError:
                value = None
            if not value:
                return None
            if isinstance(value, str):
                value = str(int(value) + delta)
            elif isinstance(value, int):
                value = value + delta
            else:
                # not a str or int, can't increment
                raise ValueError("cannot increment or decrement non-numeric value")
            self.update(key, value)
            return int(value)

    def update(self, key, value, time=None):
        with self.write_lock:
            if key not in self.cache:
                return False
            else:
                return self.set(key, value, time)

    def set(self, key, value, time):
        with self.write_lock:
            now = datetime.datetime.now()
            if time and time > 60*60*24*30:
                expires = datetime.datetime.utcfromtimestamp(time)
            elif (not time) or time == 0:
                expires = None
            else:
                expires = now + datetime.timedelta(seconds=time)
            self.cache[key] = self.CachedItem(value, expires, uuid.uuid4())
            return True

    def flush(self):
        with self.write_lock:
            self.cache = dict()

    def append(self, key, value, time):
        with self.write_lock:
            try:
                original, _ = self[key]
            except TypeError:
                original = None
            if not original:
                return False
            if isinstance(original, str):
                value = original + str(value)
            elif isinstance(original, int):
                try:
                    value = int(str(original) + str(value))
                except ValueError as e:
                    raise ValueError("cannot append non-numeric value to int") from e
            elif isinstance(original, float):
                try:
                    value = float(str(original) + str(value))
                except ValueError as e:
                    raise ValueError("cannot append non-numeric value to float") from e
            else:
                return False

            return self.set(key, value, time)

    def prepend(self, key, value, time):
        with self.write_lock:
            try:
                original, _ = self[key]
            except TypeError:
                original = None
            if not original:
                return False
            if isinstance(original, str):
                value = str(value) + original
            elif isinstance(original, int):
                try:
                    value = int(str(value) + str(original))
                except ValueError as e:
                    raise ValueError("cannot prepend non-numeric value to int") from e
            elif isinstance(original, float):
                try:
                    value = float(str(value) + str(original))
                except ValueError as e:
                    raise ValueError("cannot prepend non-numeric value to float") from e
            else:
                return False

            return self.set(key, value, time)

    def cas(self, key, value, time, cas_id):
        with self.write_lock:
            if key not in self.cache or not cas_id:
                return self.set(key, value, time)
            else:
                if cas_id == self.cache[key].cas_id:
                    return self.set(key, value, time)
                else:
                    return 0


class MimicStash(collections.MutableMapping):
    """
    A cache, mimicking a memcached server for a gemstash Client.

    This Stash behaves more like python-memcached + memcached. Use when closer
    correspondence between gemstash and memcache behvior is required.

    """

    CachedItem = collections.namedtuple('CachedItem', ['value', 'expires', 'parse', 'cas_id'])

    def __init__(self, mimic=True, *args, **kwargs):
        """Create a new Stash."""
        self.cache = dict()
        self.write_lock = threading.RLock()
        self.mimic = mimic

    def __getitem__(self, key):
        try:
            item = self.cache[key]
        except KeyError:
            return None
        if item.expires and item.expires < datetime.datetime.now():
            del self.cache[key]
            return None
        else:
            return item.parse(item.value), item.cas_id

    def __setitem__(self, key, value):
        raise NotImplementedError("Add items to the stash using the set method.")

    def __delitem__(self, key):
        with self.write_lock:
            try:
                del self.cache[key]
            except KeyError:
                pass

    def __iter__(self):
        return iter(self.cache)

    def __len__(self):
        return len(self.cache)

    def incr(self, key, delta):
        with self.write_lock:
            try:
                value, _ = self[key]
            except TypeError:
                value= None
            if not value:
                return None
            if isinstance(value, str):
                value = str(int(value) + delta)
            elif isinstance(value, int):
                value = value + delta
            else:
                # not a str or int, can't increment
                raise ValueError("cannot increment or decrement non-numeric value")
            self.update(key, value)
            return int(value)

    def update(self, key, value, time=None):
        with self.write_lock:
            if key not in self.cache:
                return False
            else:
                return self.set(key, value, time)

    def set(self, key, value, time):
        with self.write_lock:
            expires = self._expires(time)
            if isinstance(value, int):
                parse = lambda x: int(x.decode("utf_8"))
            elif isinstance(value, float):
                parse = lambda x: float(x.decode("utf_8"))
            else:
                parse = lambda x: x.decode("utf_8")
            value = str(value).encode("utf_8")
            self.cache[key] = self.CachedItem(value, expires, parse, uuid.uuid4())
            return True

    def flush(self):
        with self.write_lock:
            self.cache = dict()

    def append(self, key, value, time):
        with self.write_lock:
            try:
                original, _, parse, cas_id = self.cache[key]
            except KeyError:
                return False
            if isinstance(parse(original), float):
                return True
            value = original + str(value).encode("utf_8")
            self.cache[key] = self.CachedItem(value, self._expires(time), parse, uuid.uuid4())
            return True

    def prepend(self, key, value, time):
        with self.write_lock:
            try:
                original, _, parse, _ = self.cache[key]
            except KeyError:
                return False
            if isinstance(parse(original), float):
                return True
            value = str(value).encode("utf_8") + original
            self.cache[key] = self.CachedItem(value, self._expires(time), parse, uuid.uuid4())
            return True

    def cas(self, key, value, time, cas_id):
        with self.write_lock:
            if key not in self.cache or not cas_id:
                return self.set(key, value, time)
            else:
                if cas_id == self.cache[key].cas_id:
                    return self.set(key, value, time)
                else:
                    return 0

    @staticmethod
    def _expires(time):
        if time and time > 60*60*24*30:
            expires = datetime.datetime.utcfromtimestamp(time)
        elif (not time) or time == 0:
            expires = None
        else:
            expires = datetime.datetime.now() + datetime.timedelta(seconds=time)
        return expires

class Client(object):
    """Client mimicking a memcached client."""

    class MemcachedKeyError(Exception):
        pass
    class MemcachedKeyLengthError(MemcachedKeyError):
        pass
    class MemcachedKeyCharacterError(MemcachedKeyError):
        pass
    class MemcachedKeyNoneError(MemcachedKeyError):
        pass
    class MemcachedKeyTypeError(MemcachedKeyError):
        pass

    def __init__(self, servers, debug=0, pickleProtocol=0,
                 pickler=None, unpickler=None,
                 pload=None, pid=None,
                 server_max_key_length=SERVER_MAX_KEY_LENGTH,
                 server_max_value_length=SERVER_MAX_VALUE_LENGTH,
                 dead_retry=_DEAD_RETRY, socket_timeout=_SOCKET_TIMEOUT,
                 cache_cas = False, flush_on_reconnect=0, check_keys=True):
        """Create a new Client attached to a specified Stash."""
        self.stash = servers
        self.debug = debug
        self.server_max_key_length = server_max_key_length
        self.cache_cas = cache_cas
        self.cas_cache = {}


    def flush_all(self):
        """
        Expires all data in the connected Stash, including data with no expiry
        time.

        """
        self.stash.flush()

    def debuglog(self, str):
        """
        Write a log entry to stderr.

        This method is provided purely for compatibility with python-memcached.

        """

        if self.debug:
            sys.stderr.write("MemCached: {}\n".format(str))

    def delete_multi(self, keys, time=0, key_prefix=''):
        """
        Delete multiple keys from the connected Stash.

        If a key_prefix is specified, it is prepended to each of the keys in the
        list. So:

            delete_multi(['bar', 'baz'], key_prefix='foo')

        is equivalent to:

            delete('foobar')
            delete('foobaz')

        The full operation IS NOT atomic.

        """
        # TODO: the time param does nothing
        for key in keys:
            self.delete(key_prefix + key, time)

    def delete(self, key, time=0):
        """Delete a key from the connected Stash."""
        # TODO: the time param does nothing
        del self.stash[key]

    def incr(self, key, delta=1):
        """
        Increment the value assigned to key by delta.

        This operation is performed by the stash with atomicity guaranteed.

        """
        return self.stash.incr(key, delta)

    def decr(self, key, delta=1):
        """
        Decrement the value assigned to key by delta.

        This operation is performed by the stash with atomicity guaranteed.

        """
        return self.stash.incr(key, 0 - delta)

    def add(self, key, val, time = 0, min_compress_len = 0):
        """Add a new key only if that key does not exist in the stash."""
        # min_compress_len is ignored
        item = self.stash[key]
        if item:
            return False
        else:
            return self.stash.set(key, val, time)

    def append(self, key, val, time=0, min_compress_len=0):
        """
        Append the given val to the existing key's value.

        If the key does not exist in the stash, or if appending to the value
        otherwise fails, do nothing and return False.

        Integers (as either int or str) may be appended to integers, and
        int or str may be appended to a str. Appending with other types may have
        unexpected results.

        """
        # min_compress_len is ignored
        return self.stash.append(key, val, time)

    def prepend(self, key, val, time=0, min_compress_len=0):
        """
        Prepend the given val to the existing key's value.

        If the key does not exist in the stash, or if prepending to the value
        otherwise fails, do nothing and return False.

        Integers (as either int or str) may be prepended to integers, and
        int or str may be prepended to a str. Prepending with other types may
        have unexpected results.

        """
        # min_compress_len is ignored
        return self.stash.prepend(key, val, time)

    def replace(self, key, val, time=0, min_compress_len=0):
        """
        Replace an existing key's value with val.

        Does nothing and returns False if the key does not exist in the stash.

        """
        return self.stash.update(key, val, time)

    def set(self, key, val, time=0, min_compress_len=0):
        """
        Assign val to key in the connected stash.

        This with replace the current assignment, if one exists, or else assign
        the value to a new key in the stash.

        """
        return self.stash.set(key, val, time)

    def set_multi(self, mapping, time=0, key_prefix='', min_compress_len=0):
        """
        Set multiple keys in the connected Stash.

        If a key_prefix is specified, it is prepended to each of the keys in the
        mapping. So:

            set_multi({'bar' : 'barvalue', 'baz' : 'bazvalue'}, key_prefix='foo')

        is equivalent to:

            set('foobar', 'barvalue')
            set('foobaz', 'bazvalue')

        This method returns a list of any keys which could not be set. So:

            failures = set_multi(...)
            if failures:
                # do something
            else:
                # all keys were set successfully

        The full operation IS NOT atomic.

        """
        failures = []
        for key in mapping:
            item = self.stash.set(key_prefix + key, mapping[key], time)
            if not item:
                # at the moment, set always returns True, so this can't happen
                failures.append(key)
        return failures

    def get(self, key):
        """Retrieve the value of a key from the connected Stash."""
        try:
            result, cas_id = self.stash[key]
        except TypeError:
            result = None
        if result and self.cache_cas:
            self.cas_cache[key] = cas_id
        return result

    def get_multi(self, keys, key_prefix=''):
        """
        Retrieve the values of multiple keys from the connected Stash.

        The results are returned as a dictionary. If a key prefix was specified,
        the keys in the result dictionary WILL NOT include the prefix.

        The full operation IS NOT atomic.

        """
        results = {}
        for key in keys:
            try:
                result, cas_id = self.stash[key_prefix + key]
            except TypeError:
                result = None
            if result:
                if self.cache_cas:
                    self.cas_cache[key] = cas_id
                results[key] = result
        return results

    def check_key(self, key, key_extra_len=0):
        """Check whether a given key is valid."""
        if not key:
            raise Client.MemcachedKeyNoneError("Key is None")
        if not isinstance(key, str):
            raise Client.MemcachedKeyTypeError("Key must be str()'s")
        if isinstance(key, bytes):
            # this could happen in python 2, but not python 3
            keylen = len(key)
        else:
            keylen = len(key.encode("utf_8"))
        if (self.server_max_key_length != 0 and
            keylen + key_extra_len > self.server_max_key_length):
            raise Client.MemcachedKeyLengthError(
                "Key length is > {}".format(self.server_max_key_length)
            )
        invalid_key_characters = ''.join(map(chr, list(range(33)) + [127]))
        after_translate = key.translate(key.maketrans('', '', invalid_key_characters))
        if len(key) != len(after_translate):
            raise Client.MemcachedKeyCharacterError("Control characters not allowed")

    def cas(self, key, val, time=0, min_compress_len=0):
        """Set a key only if it has not been changed since last fetched."""
        return self.stash.cas(key, val, time, self.cas_cache.get(key))

    def reset_cas(self):
        """Reset the cas cache."""
        self.cas_cache = {}

    def gets(self, key):
        """Get a key.

        This method is included for completeness, but doesn't seem to be used by
        python-memcached.

        """
        return self.get(key)

    # Dummy methods

    def set_servers(self, servers):
        pass

    def get_stats(self, stat_args = None):
        pass

    def get_slabs(self):
        pass

    def forget_dead_hosts(self):
        pass

    def disconnect_all(self):
        pass
