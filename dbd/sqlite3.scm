;;; -*- mode:scheme; coding:utf-8; -*-
;;;
;;; dbd/sqlite3.scm - DBD for SQLite library
;;;  
;;;   Copyright (c) 2012  Takashi Kato  <ktakashi@ymail.com>
;;;   
;;;   Redistribution and use in source and binary forms, with or without
;;;   modification, are permitted provided that the following conditions
;;;   are met:
;;;   
;;;   1. Redistributions of source code must retain the above copyright
;;;      notice, this list of conditions and the following disclaimer.
;;;  
;;;   2. Redistributions in binary form must reproduce the above copyright
;;;      notice, this list of conditions and the following disclaimer in the
;;;      documentation and/or other materials provided with the distribution.
;;;  
;;;   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
;;;   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
;;;   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
;;;   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
;;;   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
;;;   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
;;;   TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
;;;   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
;;;   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
;;;   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
;;;   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
;;;  

#!read-macro=sagittarius/regex
(library (dbd sqlite3)
    (export make-sqlite3-driver)
    (import (rnrs)
	    (sqlite3)
	    (dbi)
	    (clos user)
	    (sagittarius)
	    (sagittarius object)
	    (sagittarius regex)
	    (sagittarius control))
  
  (define-class <dbi-sqlite3-driver> (<dbi-driver>) ())
  (define-class <state-mixin> ()
    ((open? :init-value #t)))
  (define-class <dbi-sqlite3-connection> (<dbi-connection> <state-mixin>)
    ((db :init-keyword :db :reader sqlite3-driver-db)))
  (define-class <dbi-sqlite3-query> (<dbi-query> <state-mixin>)
    ;; temporary storage to keep the result of dbi-execute!
    ((step :init-value #f :accessor sqlite3-query-step)))
  
  (define-method dbi-make-connection ((driver <dbi-sqlite3-driver>)
				      (options <string>)
				      (option-alist <list>) . opt)
    (let1 database (assoc "database" option-alist)
      (unless database
	(assertion-violation 'dbi-make-connection
			     "database option is required" options))
      (make <dbi-sqlite3-connection> :db (sqlite3-open (cdr database)))))

  (define-method dbi-open? ((conn <dbi-sqlite3-connection>))
    (~ conn 'open?))
  (define-method dbi-close ((conn <dbi-sqlite3-connection>))
    (sqlite3-close! (sqlite3-driver-db conn))
    (set! (~ conn 'open?) #f))

  (define-method dbi-open? ((q <dbi-sqlite3-query>))
    (~ q 'open?))
  (define-method dbi-close ((q <dbi-sqlite3-query>))
    (sqlite3-finalize! (dbi-query-prepared q))
    (set! (~ q 'open?) #f))

  (define-method dbi-prepare ((conn <dbi-sqlite3-connection>)
			      (sql <string>) . args)
    (let1 stmt (sqlite3-prepare (sqlite3-driver-db conn) sql)
      (apply sqlite3-bind! stmt args)
      (make <dbi-sqlite3-query>
	:connection (sqlite3-driver-db conn)
	:prepared stmt)))
  ;; commit and rollback are not supported
  (define-method dbi-commit! ((conn <dbi-sqlite3-connection>)) #t)
  (define-method dbi-rollback! ((conn <dbi-sqlite3-connection>)) #t)
  ;; query levels either
  (define-method dbi-commit! ((query <dbi-sqlite3-query>)) #t)
  (define-method dbi-rollback! ((query <dbi-sqlite3-query>)) #t)

  (define-method dbi-bind-parameter! ((query <dbi-sqlite3-query>)
				      (index <integer>) value . args)
    (sqlite3-bind-1! (dbi-query-prepared query) index value))

  (define-method dbi-execute! ((query <dbi-sqlite3-query>) . args)
    (let* ((stmt (dbi-query-prepared query))
	   (sql  (sqlite3-sql stmt)))
      (unless (null? args) (apply sqlite3-bind! stmt args))
      (sqlite3-query-step query (sqlite3-step! stmt))
      (cond ((#/^select.*/ sql) -1)
	    (else
	     (let1 count (sqlite3-changes (dbi-query-connection query))
	       (sqlite3-finalize! stmt)
	       count)))))

  (define-method dbi-fetch! ((query <dbi-sqlite3-query>))
    (define (step-or-prev stmt)
      (or (and-let* ((prev (sqlite3-query-step query))
		     ( (memq  prev '(row done)) ))
	    (sqlite3-query-step query 'invalid)
	    prev)
	  (sqlite3-step! stmt)))
    (let* ((stmt (dbi-query-prepared query))
	   (state (step-or-prev stmt)))
      (cond ((eq? state 'row)
	     (let* ((count (sqlite3-column-count stmt))
		    (ret (make-vector count)))
	       (do ((i 0 (+ i 1)))
		   ((= i count) ret)
		 (vector-set! ret i (sqlite3-column stmt i)))))
	    (else
	     (sqlite3-finalize! stmt)
	     #f))))

  (define-method dbi-fetch-all! ((query <dbi-sqlite3-query>))
    (let loop ((v (dbi-fetch! query)) (r '()))
      (if v
	  (loop (dbi-fetch! query) (cons v r))
	  (reverse! r))))
  ;; not supported but just returns a empty vector
  (define-method dbi-columns ((query <dbi-sqlite3-query>))
    (or (and-let* (( (symbol? (sqlite3-query-step query)) )
		   (stmt (dbi-query-prepared query)))
	  (let* ((count (sqlite3-column-count stmt))
		 (ret (make-vector count)))
	    (dotimes (i count ret)
	      (vector-set! ret i (sqlite3-column-name stmt i)))))
	(error 'dbi-columns
	       "dbi-execute! must be called before")))

  (define (make-sqlite3-driver) (make <dbi-sqlite3-driver>))
)
