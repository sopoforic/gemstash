# Gemstash - a server-free memcached replacement

Gemstash provides a simple cache similar to memcached, appropriate for use in
standalone applications which do not have access to a memcached server. Its
usage should be familiar to users of python-memcached.

## Usage

```
>>> import gemstash
>>> gs = gemstash.Client(gemstash.Stash())
```

Each stash stores its own independent set of keys and values. Two clients
accessing the same stash will get the same values.

```
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
```

Values can be stored with an expiry time, specified in seconds, after which they
will be deleted from the cache (and not returned). Numbers greater than
60*60*24*30 (thirty days) will be interpreted instead as an absolute timestamp
in seconds since January 1, 1970 (epoch time). If the time parameter is set to
0 or omitted, the item will never expire.

## Mimicking memcache

If it is necessary to mimic python-memcached more closely (e.g. testing locally
a program that will use python-memcached in production), the gemstash Client can
instead use the MimicStash:

```
>>> import gemstash
>>> gs = gemstash.Client(gemstash.MimicStash())
```

The MimicStash is intended to more closely mimic the behavior of
python-memcached with a memcache server. It is not perfect, and is not
recommended for use unless absolutely necessary, since it replicates some
probably-undesirable features of python-memcached:

```
import gemstash
>>> gs = gemstash.Client(gemstash.Stash())
>>> gs_mimic = gemstash.Client(gemstash.MimicStash())
>>> gs.set("float", 12.34)
True
>>> gs_mimic.set("float", 12.34)
True
>>> gs.append("float", "cows")
...
ValueError: cannot append non-numeric value to float
>>> gs_mimic.append("float", "cows")
True
>>> gs_mimic.get("float")
12.34
```

## Credits

Gemstash is available under the MIT license. See LICENSE for details.
