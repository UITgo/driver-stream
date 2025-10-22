# driver-stream (Go)
Realtime driver presence & location service for UIT-Go.

## Endpoints
- POST /v1/drivers/{id}/status {ONLINE|OFFLINE}
- PUT  /v1/drivers/{id}/location {lat,lng,speed?,heading?,ts?}
- GET  /v1/drivers/nearby?lat=&lng=&radius=2000&limit=20
- POST /v1/assign/prepare {tripId,candidates[],ttlSeconds}
- POST /v1/assign/claim {tripId,driverId}
- DELETE /v1/assign/{tripId}

## Env
REDIS_ADDR=redis:6379  
KAFKA_BROKERS=kafka:9092  
KAFKA_TOPIC_LOCATION=driver.location  
HTTP_ADDR=:8080
