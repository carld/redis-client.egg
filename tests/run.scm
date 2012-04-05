(require-library redis-client)

(define (test a b)
  (pp a)
  (if (not (equal? a b))
      (error (sprintf "Failed test: ~A" a))))

(redis-connect "127.0.0.1" 6379)
(test (redis-ping) 
      '("PONG"))
(test (redis-lpush "scheme-test" "1234") 
      '("1"))
(test (redis-rpop "scheme-test") 
      '("1234"))


