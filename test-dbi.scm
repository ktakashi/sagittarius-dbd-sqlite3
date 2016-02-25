(add-load-path ".")
(import (rnrs) (sagittarius control)
	(dbi) (sqlite3)
	(srfi :19)
	(srfi :26)
	(srfi :64))

(define-constant +db+  "test.db")
(when (file-exists? +db+) (delete-file +db+))

(test-begin "SQLite3 tests")
;; prepare
(define conn (dbi-connect (string-append "dbi:sqlite3:database=" +db+)))

(test-assert "create table"
 (dbi-execute-using-connection! conn
  "create table test (i integer, f float, t text, b blob, n int)"))

(test-error "invalid syntax" sqlite-error?
	    (dbi-execute! (dbi-prepare conn "create table ttt")))
(test-error "duplicate "sqlite-error?
	    (dbi-execute! (dbi-prepare conn "create table test (id int)")))

(dbi-commit! conn)

(test-equal "insert" 1 
	    (let* ((q (dbi-prepare 
		       conn
		       "insert into test (i, f, t, b, n) values (?, ?, ?, ?, ?)"
		       100 3.14 "text" #vu8(1 2 3 4 5) '()))
		   (r (dbi-execute! q)))
	      (dbi-close q)
	      r))

(define (get-value col)
  (if (input-port? col)
      (get-bytevector-all col)
      col))
(define (get-values col)
  (vector-map get-value col))

(test-equal "fetch"
	    '(#(100 3.14 "text" #vu8(1 2 3 4 5) ()))
	    (let1 stmt (dbi-prepare conn "select * from test")
	      (dbi-execute! stmt)
	      (let loop ((v (dbi-fetch! stmt)) (r '()))
		(if v
		    (loop (dbi-fetch! stmt) (cons (get-values v) r))
		    (begin (dbi-close stmt) r)))))

(test-equal "fetch-all"
	    '(#(100 3.14 "text" #vu8(1 2 3 4 5) ()))
	    (let1 stmt (dbi-prepare conn "select * from test")
	      (dbi-execute! stmt)
	      (let1 r (dbi-fetch-all! stmt)
		(dbi-close stmt)
		(map get-values r))))

(test-equal "columns"
	    #("i" "f" "t" "b" "n")
	    (let1 stmt (dbi-prepare conn "select i, f, t, b, n from test")
	      (dbi-execute! stmt)
	      (let1 r (dbi-columns stmt)
		(dbi-fetch-all! stmt)
		(dbi-close stmt)
		r)))

(let ((stmt (dbi-prepare conn "select * from test")))
  (test-assert (dbi-open? stmt))
  (dbi-close stmt)
  (test-assert (not (dbi-open? stmt))))

(dbi-commit! conn)

(test-assert 
 "create table (2)"
 (let ((q (dbi-prepare conn
	    "create table test_date (t timestamp, d date, dt datetime)")))
   (dbi-execute! q)
   (dbi-close q)))
(dbi-commit! conn)

(test-equal "insert(2)" 1 
	    (let* ((q (dbi-prepare 
		       conn
		       "insert into test_date (t, d, dt) values (?, ?, ?)"
		       (current-time) (current-date) (current-date)))
		   (r (dbi-execute! q)))
	      (dbi-close q)
	      r))


(test-assert "fetch (timestamp)"
	     (let1 stmt (dbi-prepare conn "select t from test_date")
	       (dbi-execute! stmt)
	       (let1 v (vector-ref (dbi-fetch! stmt) 0)
		 (dbi-close stmt)
		 (time? v))))
(test-assert "fetch (date)"
	     (let1 stmt (dbi-prepare conn "select d from test_date")
	       (dbi-execute! stmt)
	       (let1 v (vector-ref (dbi-fetch! stmt) 0)
		 (dbi-close stmt)
		 (and (date? v)
		      (zero? (date-hour v))
		      (zero? (date-minute v))
		      (zero? (date-second v))))))
(test-assert "fetch (datetime)"
	     (let1 stmt (dbi-prepare conn "select dt from test_date")
	       (dbi-execute! stmt)
	       (let1 v (vector-ref (dbi-fetch! stmt) 0)
		 (dbi-close stmt)
		 (date? v))))

(dbi-rollback! conn)
(test-assert "rollbacked!"
	     (let1 stmt (dbi-prepare conn "select d from test_date")
	       (not (dbi-fetch! (dbi-execute-query! stmt)))))

(test-assert "close" (dbi-close conn))
(test-end)
