; chicken-scheme redis-client
; Copyright (C) 2011 A. Carl Douglas
(module redis-client *
(import chicken scheme extras)
(use socket)
(begin-for-syntax
  (import chicken))

(define (redis-write-command port command args)
  (fprintf port "*~A\r\n$~A\r\n~A\r\n~A~!" 
            (+ 1 (length args))
            (string-length (symbol->string command))
            (symbol->string command)
            (apply string-append 
              (map (lambda(arg)
                     (sprintf "$~A\r\n~A\r\n" (string-length arg) arg)) args))))

(define (redis-read-response port)
  (letrec ((parse (lambda(argc args)
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
                   (else  (error "unrecognised prefix" ch (read-line port)))))))))
            (parse 1 '())))

(define-syntax make-redis-parameter-function
  (lambda (x r c)
    (let ((command-proc (string->symbol(sprintf "redis-~A" (cadr x)))))
      `(define (,command-proc . args)
         (redis-write-command (*redis-out-port*) ',(cadr x) args)
         (redis-read-response (*redis-in-port*))))))

(define-syntax map-make-redis-parameter-function
  (syntax-rules ()
    ((_ (fn ...)) (begin (make-redis-parameter-function fn) ...))))

(map-make-redis-parameter-function
  (list 
    ping
    quit
    auth
    exists
    del
    type
    keys
    randomkey
    rename
    renamenx
    dbsize
    expire
    persist
    ttl
    select
    move
    flushdb
    flushall
    set
    get
    getset
    setnx
    setex
    setbit
    mset
    msetnx
    mget
    incr
    incrby
    decr
    decrby
    append
    substr
    rpush
    lpush
    llen
    lrange
    ltrim
    lindex
    lset
    lrem
    lpop
    rpop
    blpop
    brpop
    rpoplpush
    brpoplpush
    sadd
    srem
    spop
    smove
    scard
    sismember
    sinter
    sinterstore
    sunion
    sunionstore
    sdiff
    sdiffstore
    smembers
    srandmember
    zadd
    zrem
    zincrby
    zrank
    zrevrank
    zrange
    zrevrange
    zrangebyscore
    zcount
    zcard
    zscore
    zremrangebyrank
    zremrangebyscore
    zunionstore
    zinterstore
    hset
    hget
    hmget
    hmset
    hincrby
    hexists
    hdel
    hlen
    hkeys
    hvals
    hgetall
    sort
    multi
    exec
    discard
    watch
    unwatch
    subscribe
    unsubscribe
    publish
    save
    bgsave
    lastsave
    shutdown
    bgrewriteaof
    info
    monitor
    slaveof
    config))

(define *redis-in-port* (make-parameter #f))
(define *redis-out-port* (make-parameter #f))

(define *redis-socket* '())

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

