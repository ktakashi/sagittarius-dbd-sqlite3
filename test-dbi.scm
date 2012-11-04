(add-load-path ".")
(import (rnrs) (sagittarius control)
	(dbi) (sqlite3)
	(srfi :64))

(define-constant +db+  "test.db")
(when (file-exists? +db+) (delete-file +db+))

(test-begin "SQLite3 tests")
;; prepare
(define conn (dbi-connect (string-append "dbi:sqlite3:database=" +db+)))

(test-assert 
 "create table"
 (dbi-execute! 
  (dbi-prepare conn
	       "create table test (i integer, f float, t text, b blob, n int)"))
 )

(test-error "invalid syntax" sqlite-error?
	    (dbi-execute! (dbi-prepare conn "create table ttt")))
(test-error "duplicate "sqlite-error?
	    (dbi-execute! (dbi-prepare conn "create table test (id int)")))

(test-equal "insert" 1 
	    (dbi-execute! 
	     (dbi-prepare 
	      conn
	      "insert into test (i, f, t, b, n) values (?, ?, ?, ?, ?)"
	      100 3.14 "text" #vu8(1 2 3 4 5) '())))


(test-equal "fetch"
	    '(#(100 3.14 "text" #vu8(1 2 3 4 5) ()))
	    (let1 stmt (dbi-prepare conn "select * from test")
	      (dbi-execute! stmt)
	      (let loop ((v (dbi-fetch! stmt)) (r '()))
		(if v
		    (loop (dbi-fetch! stmt) (cons v r))
		    r))))

(test-equal "fetch-all"
	    '(#(100 3.14 "text" #vu8(1 2 3 4 5) ()))
	    (let1 stmt (dbi-prepare conn "select * from test")
	      (dbi-execute! stmt)
	      (dbi-fetch-all! stmt)))

(test-assert "close" (dbi-close conn))
(test-end)
