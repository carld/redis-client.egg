; chicken-scheme redis-client
; Copyright (C) 2011 A. Carl Douglas
(use socket)

(define (write-command port command args)
  (fprintf port "*~A\r\n$~A\r\n~A\r\n~A~!" 
            (+ 1 (length args))
            (string-length (symbol->string command))
            (symbol->string command)
            (apply string-append 
              (map (lambda(arg)
                     (sprintf "$~A\r\n~A\r\n" (string-length arg) arg)) args))))

(define (read-response port)
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

(define (make-redis-client host port)
  (parameterize ((socket-receive-buffer-size 4096))
    (define redis-socket 
      (socket-connect/ai (address-information host port family: af/inet)))
    (define-values (i o) (socket-i/o-ports redis-socket))
    (define (xfer command args)
      (begin
        (write-command o command args)
        (read-response i)))
    (lambda (command . args)
      (case command
        ((close) (socket-close redis-socket))
        (else (xfer command args))))))

(define-syntax make-redis-function
  (syntax-rules ()
                ((_ command) 
                    (define (command . args)
                       (write-command (current-output-port) 'command args)
                       (read-response (current-input-port))) )))

; Example program:
;
;(make-redis-function publish)
;(define *redis-socket* 
;  (socket-connect/ai (address-information "127.0.0.1" 6379 family: af/inet)))
;(define-values (i o) (socket-i/o-ports *redis-socket*))
;(current-input-port i)
;(current-output-port o)
;(pp (publish "my-queue" "hello world"))

