; Usage:
;   csi -s subscriber.scm my-channel
(use srfi-1 redis-client)

(redis-connect "127.0.0.1" 6379)
(redis-subscribe (last (command-line-arguments)))

(define (run-loop thunk)
  (let ((response (redis-read-response (*redis-in-port*))))
     (thunk response))
  (run-loop thunk))

(run-loop 
  (lambda(r)
    (printf "->~S~%" r)))

