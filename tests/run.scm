(include "../redis-client.scm")

(define (test a b)
  (pp a)
  (if (not (equal? a b))
      (error (sprintf "Failed test: ~A" a))))

(define client (make-redis-client "127.0.0.1" 6379))

(test (client 'ping) 
      '("+PONG"))
(test (client 'lpush "scheme-test" "1234") 
      '(":1"))
(test (client 'rpop "scheme-test") 
      '("$4" "1234"))

(client 'close)

