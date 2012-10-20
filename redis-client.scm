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
  (letrec ((bulk
             (lambda (arg) 
               (sprintf "$~A\r\n~A\r\n" (string-length arg) arg)))
           (multi-bulk
             (lambda ()
               (apply string-append (map bulk args))))
           (format-command
             (lambda ()
               (sprintf "*~A\r\n$~A\r\n~A\r\n~A~!"
                  (+ 1 (length args))
                  (string-length command)
                  command
                  (multi-bulk)))))
    (fprintf port (format-command))))

(define (redis-read-response port)
  (letrec ((argc 1)(args '())
           (update-args!
             (lambda (a) (set! args (append args (list a)))))
           (single-line 
             (lambda () (read-line port)))
           (single-line-number
             (lambda () (string->number (single-line))))
           (multi-bulk
             (lambda () (single-line-number)))
           (bulk
             (lambda () (let ((n (single-line-number)))
                          (cond ((equal? n -1)   '())
                                (else (let ((arg (read-string n port)))
                                         (read-string 2 port)
                                          arg))))))
           (next-line
             (lambda () (cond ((equal? argc (length args))  args)
                              ((equal? argc -1)  '())
                              (else (prefix)))))
           (prefix
             (lambda ()
               (let ((ch (read-char port)))
                 (case ch
                   ((#\+) (begin       ; single line reply
                            (update-args! (single-line))
                            (next-line)))
                   ((#\-) (begin       ; error message
                            (set! args (single-line))
                            (next-line)))
                   ((#\:) (begin       ; integer number
                            (update-args! (single-line-number))
                            (next-line)))
                   ((#\*) (begin       ; multi-bulk
                            (set! argc (multi-bulk))
                            (next-line)))
                   ((#\$) (begin       ; bulk
                            (update-args! (bulk))
                            (next-line)))
                   (else (error "unrecognised prefix" ch )))))))
    (prefix)))

(define-syntax redis-transaction
  (ir-macro-transformer
    (lambda (x i c)
      `(handle-exceptions
         exn
        (begin
          (redis-discard)
          (abort exn))
        (redis-multi)
        ,@(cdr x)
        (redis-exec)))))
 
(define-syntax map-make-redis-parameter-function
  (ir-macro-transformer
    (lambda (x i c)
      `(begin
         ,@(map (lambda(f)
             `(define (,f . a)
                (redis-write-command
                  (*redis-out-port*)
                  ,(string-upcase
                     (cadr
                       (string-split (symbol->string (i f)) "-"))) a)
                (redis-read-response
                  (*redis-in-port*))))
             (cadr x))))))

(map-make-redis-parameter-function
  (
    ;; Generic
    redis-del
    redis-expire
    redis-expireat
    redis-keys
    redis-move
    redis-persist
    redis-randomkey
    redis-rename
    redis-renamenx
    redis-sort
    redis-ttl
    redis-type
    ;; String
    redis-append
    redis-decr
    redis-decrby
    redis-get
    redis-getbit
    redis-getset
    redis-incr
    redis-incrby
    redis-mget
    redis-mset
    redis-msetnx
    redis-set
    redis-setbit
    redis-setex
    redis-setnx
    redis-setrange
    redis-strlen
    redis-substr
    ;; List
    redis-blpop
    redis-brpop
    redis-brpoplpush
    redis-lindex
    redis-linsert
    redis-llen
    redis-lpop
    redis-lpush
    redis-lpushx
    redis-lrange
    redis-lrem
    redis-lset
    redis-ltrim
    redis-rpop
    redis-rpoplpush
    redis-rpush
    redis-rpushx
    ;; Set
    redis-sadd
    redis-scard
    redis-sdiff
    redis-sdiffstore
    redis-sinter
    redis-sinterstore
    redis-sismember
    redis-smembers
    redis-smove
    redis-spop
    redis-srandmember
    redis-srem
    redis-sunion
    redis-sunionstore
    ;; Sorted set
    redis-zadd
    redis-zcard
    redis-zcount
    redis-zincrby
    redis-zinterstore
    redis-zrange
    redis-zrangebyscore
    redis-zrank
    redis-zrem
    redis-zremrangebyrank
    redis-zremrangebyscore
    redis-zrevrange
    redis-zrevrangebyscore
    redis-zrevrank
    redis-zscore
    redis-zunionstore
    ;; Hash
    redis-hdel
    redis-hexists
    redis-hget
    redis-hgetall
    redis-hincrby
    redis-hkeys
    redis-hlen
    redis-hmget
    redis-hmset
    redis-hset
    redis-hsetnx
    redis-hvals
    ;; Publish/Subscribe
    redis-psubscribe
    redis-publish
    redis-punsubscribe
    redis-subscribe
    redis-unsubscribe
    ;; Transactions
    redis-discard
    redis-exec
    redis-multi
    redis-unwatch
    redis-watch
    ;; Connection
    redis-auth
    redis-echo
    redis-ping
    redis-quit
    redis-select
    ;; Server
    redis-bgrewriteaof
    redis-bgsave
    redis-config
    redis-dbsize
    redis-debug
    redis-exists
    redis-flushall
    redis-flushdb
    redis-info
    redis-lastsave
    redis-monitor
    redis-save
    redis-shutdown
    redis-slaveof
    redis-sync
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

