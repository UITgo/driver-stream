-- KEYS: [deadline, claimed, candidates, presence]
-- ARGV: [now_ms, driverId]
local ttl = redis.call('PTTL', KEYS[1])
if ttl <= 0 then return redis.error_reply('EXPIRED') end

local claimed = redis.call('GET', KEYS[2])
if claimed and claimed ~= "0" then return redis.error_reply('ALREADY_CLAIMED') end

local isCand = redis.call('SISMEMBER', KEYS[3], ARGV[2])
if isCand == 0 then return redis.error_reply('NOT_CANDIDATE') end

local status = redis.call('HGET', KEYS[4], 'status')
if status ~= 'ONLINE' then return redis.error_reply('DRIVER_OFFLINE') end

redis.call('SET', KEYS[2], ARGV[2])
redis.call('PEXPIRE', KEYS[2], 600000)
return 'OK'
