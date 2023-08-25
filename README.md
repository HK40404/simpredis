# simpredis
![license](https://img.shields.io/github/license/HK40404/simpredis) ![example workflow](https://github.com/hk40404/simpredis/actions/workflows/coverall.yml/badge.svg?branch=main) [![Coverage Status](https://coveralls.io/repos/github/HK40404/simpredis/badge.svg?branch=main)](https://coveralls.io/github/HK40404/simpredis?branch=main) [![Go Report Card](https://goreportcard.com/badge/github.com/HK40404/simpredis)](https://goreportcard.com/report/github.com/HK40404/simpredis)

Simpredis is a high performance In-Memory Database written by golang.

## Features
- RESP(REdis Serialization Protocol) implemented, support interaction with any standard redis-client
- Support string, list, set, hash, bitmap data structure
- Time To Live(TTL), based on timewheel
- Atomic operations for some command, e.g., mset, incr, incrbyfloat.
- Command function as same as redis
- Concurrent execution
- Connection logs

## Supported Commands
| string      | list      | set         | hash         | key      | connection |
| ----------- | --------- | ----------- | ------------ | -------- | ---------- |
| set         | lpush     | sadd        | hget         | ttl      | ping       |
| setex       | lpop      | scard       | hset         | expire   | echo       |
| setnx       | rpush     | smembers    | hlen         | expireat |            |
| getset      | rpop      | srem        | hkeys        | persist  |            |
| get         | lindex    | sismember   | hvals        | del      |            |
| mset        | lrange    | sinter      | hgetall      | exists   |            |
| mget        | llen      | sinterstore | hmset        | rename   |            |
| msetnx      | lset      | spop        | hmget        | renamenx |            |
| incr        | lpushx    | srandmember | hexists      | type     |            |
| incrby      | rpushx    | sdiff       | hdel         |          |            |
| incrbyfloat | rpoplpush | sdiffstore  | hsetnx       |          |            |
| decr        | linsert   | smove       | hincrby      |          |            |
| decrby      | lrem      | sunion      | hincrbyfloat |          |            |
| strlen      | ltrim     | sunionstore |              |          |            |
| append      |           |             |              |          |            |
| setbit      |           |             |              |          |            |
| getbit      |           |             |              |          |            |
| bitcount    |           |             |              |          |            |
| bitop       |           |             |              |          |            |
| setrange    |           |             |              |          |            |
| getrange    |           |             |              |          |            |

## Performance
**environment**
- OS: Ubuntu 20.04.2 VM, Linux 5.15.0
- CPU: i9-12900K \* 8, 3.2 GHz
- RAM: 6GB in VM

**benchmark**
- redis-benchmark
```shell
# simpredis
PING_INLINE: 244498.77 requests per second
PING_BULK: 253164.55 requests per second
SET: 255754.47 requests per second
GET: 255754.47 requests per second
INCR: 255754.47 requests per second
LPUSH: 246305.42 requests per second
RPUSH: 251889.16 requests per second
LPOP: 248756.22 requests per second
RPOP: 248138.95 requests per second
SADD: 254452.92 requests per second
HSET: 243309.00 requests per second
SPOP: 256410.27 requests per second
LPUSH (needed to benchmark LRANGE): 244498.77 requests per second
LRANGE_100 (first 100 elements): 142247.52 requests per second
LRANGE_300 (first 300 elements): 62853.55 requests per second
LRANGE_500 (first 450 elements): 47080.98 requests per second
LRANGE_600 (first 600 elements): 35536.61 requests per second
MSET (10 keys): 229357.80 requests per second

# redis
PING_INLINE: 240963.86 requests per second
PING_BULK: 238663.48 requests per second
SET: 238663.48 requests per second
GET: 263157.91 requests per second
INCR: 244498.77 requests per second
LPUSH: 247524.75 requests per second
RPUSH: 244498.77 requests per second
LPOP: 245098.05 requests per second
RPOP: 236966.83 requests per second
SADD: 243309.00 requests per second
HSET: 244498.77 requests per second
SPOP: 239808.16 requests per second
LPUSH (needed to benchmark LRANGE): 245098.05 requests per second
LRANGE_100 (first 100 elements): 178890.88 requests per second
LRANGE_300 (first 300 elements): 72674.41 requests per second
LRANGE_500 (first 450 elements): 51413.88 requests per second
LRANGE_600 (first 600 elements): 42698.55 requests per second
MSET (10 keys): 267379.66 requests per second
```

## Usage
```shell
go build
./simpredis
```
Server will listen at port 7000. Config can be modified in `simpredis.conf`.

### Interaction with standard redis-cli
```shell
$ redis-cli -p 7000
127.0.0.1:7000> set k v
OK
127.0.0.1:7000> get k
"v"
127.0.0.1:7000>
```
