(use redis-client)

(pp (redis-connect "127.0.0.1" 6379))
(pp (redis-lpush "test-queue" "Hello!"))
(pp (redis-rpop  "test-queue"))

