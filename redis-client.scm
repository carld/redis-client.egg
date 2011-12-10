; chicken-scheme redis-client
; Copyright (c) 2011 A. Carl Douglas
(use socket)

(define (format-command command args)
  (sprintf "*~A\r\n$~A\r\n~A\r\n~A" 
            (+ 1 (length args))
            (string-length (symbol->string command))
            (symbol->string command)
            (apply
              string-append 
              (map (lambda(arg)
                     (sprintf "$~A\r\n~A\r\n" (string-length arg) arg)) args))))

(define (format-response reply)
  (with-input-from-string reply 
    (lambda()
      (letrec ((parse (lambda(argc args)
                  (let ((ch (read-char)))
                    (if (eof-object? ch)
                      args
                      (case ch
                        ((#\+) (parse argc (cons (read-line) args)))
                        ((#\*) (parse (read-line) args))
                        ((#\:) (parse argc (cons (read-line) args)))
                        ((#\$) (let ((l (read-line)))
                                 (parse argc (cons (read-string (string->number l)) args))))
                        ((#\return) args)
                        (else (error "unrecognised prefix" ch))))))))
               (parse 0 '())))))

(define (make-redis-client host port)
  (define local-socket 
    (socket-connect/ai (address-information host port family: af/inet)))
  (define (xfer command args)
    (begin
      (socket-send local-socket (format-command command args))
      (format-response (socket-receive local-socket 4096))))
  (lambda (command . args)
    (case command
      ((close) (socket-close local-socket))
      (else (xfer command args)))))

