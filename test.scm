(add-load-path ".")
(import (rnrs) (sagittarius control) (sqlite3)
	(srfi :64))

(define-constant +db+  "test.db")
(when (file-exists? +db+) (delete-file +db+))

(test-begin "SQLite3 testsn")
;; prepare
(define context (sqlite3-open +db+))
(test-assert "create table"
 (let ((stmt
	(sqlite3-prepare context
			 "create table test (i integer, f float, t text, b blob, n int)")))
   (sqlite3-step! stmt)
   (sqlite3-finalize! stmt)))
(test-error "invalid syntax" sqlite-error?
	    (let ((stmt
		   (sqlite3-prepare context
			 "create table ttt")))
	      (sqlite3-step! stmt)
	      (sqlite3-finalize! stmt)))
(test-error "duplicate "sqlite-error?
	    (let ((stmt
		   (sqlite3-prepare context
			 "create table test (id int)")))
	      (sqlite3-step! stmt)
	      (sqlite3-finalize! stmt)))
(sqlite3-close! context)

(define db (sqlite3-open +db+))
(let ((stmt
       (sqlite3-prepare 
	db "insert into test (i, f, t, b, n) values (?, ?, ?, ?, ?)")))
   (test-assert "bind" (sqlite3-bind! stmt 100 3.14 "text" #vu8(1 2 3 4 5) '()))
   (test-equal "step! (insert)" 'done (sqlite3-step! stmt))
   (test-assert "finalize!"(sqlite3-finalize! stmt)))


(test-equal "exec!"
	    '(("i" . "100") ("f" . "3.14") ("t" . "text")
	      ("b" . "\x1;\x2;\x3;\x4;\x5;") ("n"))
	    (let ((r '()))
	      (sqlite3-exec! db "select * from test"
			     (lambda (column&values)
			       (set! r column&values)))
	      r))

(test-equal "prepared"
	    '(("i" . 100) ("f" . 3.14) ("t" . "text")
	      ("b" . #vu8(1 2 3 4 5)) ("n"))
	    (let ((stmt (sqlite3-prepare db "select * from test")))
	      (let loop ((stat (sqlite3-step! stmt)) (r '()))
		(if (eq? stat 'row)
		    (let* ((size (sqlite3-column-count stmt))
			   (v (do ((i 0 (+ i 1))
				   (r '() (acons
					   (sqlite3-column-name stmt i)
					   (sqlite3-column stmt i)
					   r)))
				  ((= i size) (reverse! r)))))
		      (loop (sqlite3-step! stmt) v))
		    (begin
		      (sqlite3-finalize! stmt)
		      r)))))

(test-assert "close" (sqlite3-close! db))
(test-end)

