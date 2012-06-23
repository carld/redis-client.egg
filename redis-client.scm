; chicken-scheme redis-client
; Copyright (C) 2011 A. Carl Douglas
(module redis-client *
(import chicken scheme extras)
(use socket)
(begin-for-syntax
  (import chicken))

(define *redis-in-port* (make-parameter #f))
(define *redis-out-port* (make-parameter #f))
(define *redis-socket* '())

(define (redis-write-command port command args)
  (fprintf port "*~A\r\n$~A\r\n~A\r\n~A~!" 
            (+ 1 (length args))
            (string-length command)
            command
            (apply string-append 
              (map (lambda(arg)
                     (sprintf "$~A\r\n~A\r\n" (string-length arg) arg)) args))))

(define (redis-read-response port)
  (let parse ((argc 1) (args '()))
       (if (= argc 0)
          args
          (let ((ch (read-char port)))
               (case ch
                 ((#\+) (list (read-line port)))
                 ((#\*) (parse (string->number (read-line port)) args))
                 ((#\$) (parse (- argc 1)
                          (append args 
                            (list (read-string (string->number (read-line port)) port)))))
                 ((#\:)       (list (read-line port)))
                 ((#\return)  (parse argc args))
                 ((#\newline) (parse argc args))
                 (else (error "unrecognised prefix" ch (read-line port))))))))

(define-syntax map-make-redis-parameter-function
  (ir-macro-transformer
    (lambda (x i c)
      `(begin
         ,@(map (lambda(f)
              `(define (,f . a)
                 (redis-write-command (*redis-out-port*)
                                       ,(string-upcase (cadr (string-split (symbol->string (i f)) "-"))) a)
                 (redis-read-response (*redis-in-port*))))
             (cadr x))))))

(map-make-redis-parameter-function
  (
    redis-ping
    redis-echo
    redis-strlen
    redis-quit
    redis-auth
    redis-exists
    redis-del
    redis-type
    redis-keys
    redis-randomkey
    redis-rename
    redis-renamenx
    redis-dbsize
    redis-expire
    redis-persist
    redis-ttl
    redis-select
    redis-move
    redis-flushdb
    redis-flushall
    redis-set
    redis-get
    redis-getset
    redis-setnx
    redis-setex
    redis-setbit
    redis-mset
    redis-msetnx
    redis-mget
    redis-incr
    redis-incrby
    redis-decr
    redis-decrby
    redis-append
    redis-substr
    redis-rpush
    redis-lpush
    redis-llen
    redis-lrange
    redis-ltrim
    redis-lindex
    redis-lset
    redis-lrem
    redis-lpop
    redis-rpop
    redis-blpop
    redis-brpop
    redis-rpoplpush
    redis-brpoplpush
    redis-sadd
    redis-srem
    redis-spop
    redis-smove
    redis-scard
    redis-sismember
    redis-sinter
    redis-sinterstore
    redis-sunion
    redis-sunionstore
    redis-sdiff
    redis-sdiffstore
    redis-smembers
    redis-srandmember
    redis-zadd
    redis-zrem
    redis-zincrby
    redis-zrank
    redis-zrevrank
    redis-zrange
    redis-zrevrange
    redis-zrangebyscore
    redis-zcount
    redis-zcard
    redis-zscore
    redis-zremrangebyrank
    redis-zremrangebyscore
    redis-zunionstore
    redis-zinterstore
    redis-hset
    redis-hget
    redis-hmget
    redis-hmset
    redis-hincrby
    redis-hexists
    redis-hdel
    redis-hlen
    redis-hkeys
    redis-hvals
    redis-hgetall
    redis-sort
    redis-multi
    redis-exec
    redis-discard
    redis-watch
    redis-unwatch
    redis-subscribe
    redis-unsubscribe
    redis-publish
    redis-save
    redis-bgsave
    redis-lastsave
    redis-shutdown
    redis-bgrewriteaof
    redis-info
    redis-monitor
    redis-slaveof
    redis-config
    ))

(define (redis-connect host port)
  (set! *redis-socket* 
    (socket-connect/ai 
      (address-information host port family: af/inet)))
  (set! (so-keep-alive? *redis-socket*) #t)
  (define-values (in-port out-port)
                 (socket-i/o-ports *redis-socket*))
  (*redis-in-port* in-port)
  (*redis-out-port* out-port)
  (and (port? (*redis-in-port*)) (port? (*redis-out-port*))))

)

; Example program:
;
;(pp (redis-connect "127.0.0.1" 6379))
;(pp (redis-publish "my-queue" "hello world"))

