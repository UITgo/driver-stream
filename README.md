# UITGo Driver Stream Service

Service quản lý real-time driver presence, location, và trip assignment, được viết bằng **Go** để đảm bảo high performance và low latency.

## Tổng quan

Driver Stream Service chịu trách nhiệm:
- **Driver Presence**: Quản lý driver status (ONLINE/OFFLINE)
- **Location Tracking**: Lưu trữ và query driver locations với Redis Geo
- **Nearby Drivers**: Tìm tài xế gần nhất dựa trên location
- **Trip Assignment**: Quản lý assignment state với atomic operations (Lua scripts)
- **Event Streaming**: Publish driver location events lên Kafka

## Kiến trúc

Driver Stream sử dụng:
- **Redis Geo**: Lưu trữ driver locations và query nearby drivers
- **Redis Hash**: Lưu driver presence state (`presence:driver:{id}`)
- **Kafka**: Publish events (`driver.location.{region}`) cho real-time streaming
- **Lua Scripts**: Atomic operations cho trip assignment (tránh race conditions)
- **gRPC**: Internal service communication (legacy, có thể deprecated)
- **HTTP REST**: External API cho clients

## Endpoints

### Driver Status

- `POST /v1/drivers/:id/status` - Update driver status
  - Body: `{ status: "ONLINE" | "OFFLINE" }`
  - Flow:
    1. Update Redis hash `presence:driver:{id}` với status và timestamp
    2. Nếu ONLINE: Add driver location vào Redis Geo set `geo:drivers`
    3. Nếu OFFLINE: Remove driver khỏi Redis Geo set
    4. Return: `{ status: "ok" }`

### Driver Location

- `PUT /v1/drivers/:id/location` - Update driver location
  - Body: 
    ```json
    {
      "lat": 10.762622,
      "lng": 106.660172,
      "speed": 50.5,      // Optional
      "heading": 90,      // Optional
      "ts": 1234567890    // Optional timestamp
    }
    ```
  - Flow:
    1. Update Redis Geo set `geo:drivers` với new location
    2. Update Redis hash `presence:driver:{id}` với location metadata
    3. Publish event lên Kafka topic `driver.location.{region}` (HCM hoặc HN)
    4. Return: `{ status: "ok" }`
  - **Performance**: <50ms latency (p95) với k6 load test

### Nearby Drivers

- `GET /v1/drivers/nearby` - Tìm tài xế gần nhất
  - Query params:
    - `lat`: Latitude (required)
    - `lng`: Longitude (required)
    - `radius`: Radius in meters (default: 2000)
    - `limit`: Max number of drivers (default: 20)
  - Returns: Array of drivers
    ```json
    [
      {
        "id": "driver_123",
        "lat": 10.762622,
        "lng": 106.660172,
        "distance": 500.5,  // meters
        "status": "ONLINE"
      }
    ]
    ```
  - **Implementation**: Sử dụng Redis `GEORADIUS` command
  - **Performance**: <100ms latency (p95) với 1000+ drivers

### Trip Assignment

- `POST /v1/assign/prepare` - Prepare trip assignment
  - Body:
    ```json
    {
      "tripId": "trip_123",
      "candidates": ["driver_1", "driver_2", "driver_3"],
      "ttlSeconds": 15
    }
    ```
  - Flow:
    1. Store assignment state trong Redis với TTL
    2. Notify candidates qua SSE (Server-Sent Events)
    3. Return: `{ status: "ok" }`

- `POST /v1/assign/claim` - Driver claim trip (atomic)
  - Body: `{ tripId: "trip_123", driverId: "driver_1" }`
  - Flow:
    1. Execute Lua script để atomic claim (tránh race condition)
    2. Nếu success: Update assignment state, return `{ status: "CLAIMED" }`
    3. Nếu failed: Return `{ status: "ALREADY_CLAIMED" }`
  - **Implementation**: Lua script đảm bảo atomicity

- `DELETE /v1/assign/:tripId` - Cancel assignment
  - Flow: Remove assignment state từ Redis

### SSE Events

- `GET /v1/drivers/:id/events` - Server-Sent Events stream
  - Returns: SSE stream với assignment events
  - Events:
    - `assignment:prepared` - Trip assignment prepared
    - `assignment:claimed` - Trip claimed by driver
    - `assignment:cancelled` - Assignment cancelled

## Region Sharding

Driver Stream được shard theo region để scale:

- **HCM Region**: `driver-stream-hcm` (port 8081, Redis DB 0, topic `driver.location.hcm`)
- **HN Region**: `driver-stream-hn` (port 8082, Redis DB 1, topic `driver.location.hn`)

Mỗi shard:
- Có Redis DB index riêng
- Có Kafka topic riêng
- Có gRPC port riêng (50052 cho HCM, 50053 cho HN)

## Redis Data Structures

### Redis Geo Set
- **Key**: `geo:drivers`
- **Type**: GEO (sorted set)
- **Value**: Driver locations (lat, lng)
- **Commands**: `GEOADD`, `GEORADIUS`, `GEOREM`

### Redis Hash
- **Key**: `presence:driver:{id}`
- **Type**: HASH
- **Fields**:
  - `status`: "ONLINE" | "OFFLINE"
  - `lat`: Latitude
  - `lng`: Longitude
  - `speed`: Speed (optional)
  - `heading`: Heading (optional)
  - `updatedAt`: Timestamp

### Redis String (Assignment State)
- **Key**: `assign:trip:{tripId}`
- **Type**: STRING
- **Value**: JSON string với assignment state
- **TTL**: Set khi prepare assignment

## Lua Scripts

### Claim Script (`internal/assign/claim.lua`)

Atomic operation để claim trip:
```lua
local tripKey = KEYS[1]
local driverId = ARGV[1]
local current = redis.call('GET', tripKey)

if current == nil then
  return {status = 'NOT_FOUND'}
end

local data = cjson.decode(current)
if data.claimedBy ~= nil then
  return {status = 'ALREADY_CLAIMED', claimedBy = data.claimedBy}
end

data.claimedBy = driverId
data.claimedAt = ARGV[2]
redis.call('SET', tripKey, cjson.encode(data))
return {status = 'CLAIMED'}
```

## Environment Variables

```bash
# Redis
REDIS_ADDR=redis:6379
REDIS_URL=redis://redis:6379/0  # DB index (0 for HCM, 1 for HN)

# Kafka
KAFKA_BROKERS=kafka:9092
KAFKA_TOPIC_LOCATION=driver.location.hcm  # Region-specific topic

# OSRM (optional, for distance calculation)
OSRM_BASE_URL=http://osrm:5000

# Server
HTTP_ADDR=:8080
GRPC_ADDR=:50052  # 50052 for HCM, 50053 for HN
```

## Development

```bash
# Install dependencies
go mod download

# Run in development mode
go run cmd/server/main.go

# Build
go build -o driver-stream cmd/server/main.go

# Run binary
./driver-stream
```

## Docker

```bash
# Build image
docker build -t uitgo-driver-stream .

# Run container (HCM region)
docker run -p 8081:8080 \
  -e REDIS_ADDR=redis:6379 \
  -e REDIS_URL=redis://redis:6379/0 \
  -e KAFKA_BROKERS=kafka:9092 \
  -e KAFKA_TOPIC_LOCATION=driver.location.hcm \
  -e HTTP_ADDR=:8080 \
  -e GRPC_ADDR=:50052 \
  uitgo-driver-stream

# Run container (HN region)
docker run -p 8082:8080 \
  -e REDIS_ADDR=redis:6379 \
  -e REDIS_URL=redis://redis:6379/1 \
  -e KAFKA_BROKERS=kafka:9092 \
  -e KAFKA_TOPIC_LOCATION=driver.location.hn \
  -e HTTP_ADDR=:8080 \
  -e GRPC_ADDR=:50053 \
  uitgo-driver-stream
```

## Health Check

- `GET /healthz` - Health check endpoint
  - Returns: `{ status: "ok" }`
  - Checks: Redis connection

## Performance

### Load Test Results (k6)

- **Update Location**: p95 <50ms với 50 VUs
- **Find Nearby**: p95 <100ms với 30 VUs, 1000+ drivers
- **Throughput**: ~1000 requests/second

### Optimization Strategies

1. **Redis Geo**: Sử dụng native Geo commands (GEOADD, GEORADIUS) thay vì manual calculation
2. **Lua Scripts**: Atomic operations để tránh race conditions
3. **Kafka Async**: Publish events asynchronously để không block requests
4. **Connection Pooling**: Reuse Redis và Kafka connections

## Kafka Events

Driver Stream publish events lên Kafka topic `driver.location.{region}`:

```json
{
  "driverId": "driver_123",
  "lat": 10.762622,
  "lng": 106.660172,
  "speed": 50.5,
  "heading": 90,
  "timestamp": 1234567890,
  "region": "HCM"
}
```

Consumers có thể subscribe để:
- Real-time analytics
- Location history (có thể store vào DynamoDB)
- Notifications

## Error Handling

- **400 Bad Request**: Invalid input, missing required fields
- **404 Not Found**: Driver không tồn tại
- **500 Internal Server Error**: Redis error, Kafka error

## Security Considerations

1. **Authentication**: Gateway verify JWT và inject `X-User-Id` header
2. **Authorization**: Driver chỉ có thể update location/status của chính mình
3. **Rate Limiting**: Có thể thêm rate limiting cho location updates (tránh spam)

## Future Improvements

1. **DynamoDB History**: Store location history vào DynamoDB (time-series data)
2. **Geofencing**: Implement geofencing để detect khi driver vào/ra khỏi zones
3. **Route Optimization**: Tích hợp với OSRM để optimize routes
4. **Multi-region**: Deploy shards lên multiple AWS regions

## Xem thêm

- **Kiến trúc tổng thể**: Xem [`../architecture/README.md`](../architecture/README.md) để hiểu toàn bộ hệ thống UITGo
- **ARCHITECTURE.md**: [`../architecture/ARCHITECTURE.md`](../architecture/ARCHITECTURE.md) - Kiến trúc chi tiết
- **REPORT.md**: [`../architecture/REPORT.md`](../architecture/REPORT.md) - Báo cáo Module A
