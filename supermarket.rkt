#lang racket
(require racket/match)
(require racket/trace)
(require "queue.rkt")

(provide (all-defined-out))

(define ITEMS 5)


; TODO
; Aveți libertatea să vă structurați programul cum doriți (dar cu restricțiile
; de mai jos), astfel încât funcția serve să funcționeze conform specificației.
; 
; Restricții (impuse de checker):
; - trebuie să existe în continuare funcția (empty-counter index)
; - cozile de la case trebuie implementate folosind noul TDA queue
(define-struct counter (index tt et queue closed) #:transparent)


(define (empty-counter index)
  (make-counter index 0 0 empty-queue #f)
  )

(define (update f counters index)
  (map (λ (current) (if (= index (counter-index current))
                        (f current)
                        current
                        ))
       counters)
  )

(define (tt+ minutes)
  (λ (C)
    (make-counter (counter-index C) (+ minutes (counter-tt C)) (counter-et C) (counter-queue C) (counter-closed C))
    )
  )

(define (et+ minutes)
  (λ (C)
    (make-counter (counter-index C) (counter-tt C) (+ minutes (counter-et C)) (counter-queue C) (counter-closed C))
    )
  )

(define (add-to-counter name items)    
  (λ (C)                               
    (if (zero? (counter-et C))
        (make-counter (counter-index C) (+ (counter-tt C) items) items (enqueue (cons name items) (counter-queue C)) (counter-closed C))
        (if
         (queue-empty? (counter-queue C))
         (make-counter (counter-index C) (+ (counter-tt C) items) (+ (counter-et C) items) (enqueue (cons name items) (counter-queue C)) (counter-closed C))
         (make-counter (counter-index C) (+ (counter-tt C) items) (counter-et C) (enqueue (cons name items) (counter-queue C)) (counter-closed C))
         )
        )
    )
  )

(define (min-> counters function min index)
  (match function
    ['min-et (foldl
              (λ (current pair)
                (if (< (counter-et current) (cdr pair))
                    (cons (counter-index current) (counter-et current))
                    pair
                    )
                )
              (cons index min)
              counters
              )
             ]
    ['min-tt (foldl
              (λ (current pair)
                (if (< (counter-tt current) (cdr pair))
                    (cons (counter-index current) (counter-tt current))
                    pair
                    )
                )
              (cons index min)
              counters
              )
             ]
    )
  )

(define (min-tt counters) 
  (min-> counters 'min-tt 9999 0)
  )
(define (min-et counters) 
  (min-> counters 'min-et 9999 0)
  )

(define (remove-first-from-counter C) 
  (if (= 1 (+ (queue-size-l (counter-queue C)) (queue-size-r (counter-queue C))))
      (make-counter (counter-index C) 0 0 empty-queue (counter-closed C))
      (make-counter (counter-index C) (- (counter-tt C) (+ (cdr (top (counter-queue C))) (- (counter-et C) (cdr (top (counter-queue C)))))) (cdr (top (dequeue (counter-queue C)))) (dequeue (counter-queue C)) (counter-closed C))
      )
  )

(define (counter-empty? C)
  (if (and (zero? (counter-tt C)) (zero? (counter-et C)) (queue-empty? (counter-queue C)))
      #t
      #f)
  )

(define (pass-time-through-counter minutes)
  (λ (C)
    (if (counter-empty? C)
        C
        (cond
          ((and (< (- (counter-tt C) minutes) 0) (< (- (counter-et C) minutes) 0))
           (make-counter (counter-index C) 0 0 (counter-queue C) (counter-closed C)))
          ((and (< (- (counter-tt C) minutes) 0) (> (- (counter-et C) minutes) 0))
           (make-counter (counter-index C) 0 (- (counter-et C) minutes) (counter-queue C) (counter-closed C)))
          (else
           (if (and (> (- (counter-tt C) minutes) 0) (< (- (counter-et C) minutes) 0))
               (make-counter (counter-index C) (- (counter-tt C) minutes) 0 (counter-queue C) (counter-closed C))
               (make-counter (counter-index C) (- (counter-tt C) minutes) (- (counter-et C) minutes) (counter-queue C) (counter-closed C))
               )
           )
          )
        )
    )
  )

  
; TODO
; Implementați funcția care simulează fluxul clienților pe la case.
; ATENȚIE: Față de etapa 3, apare un nou tip de cerere, așadar
; requests conține 5 tipuri de cereri (cele moștenite din etapa 3 plus una nouă):
;   - (<name> <n-items>) - persoana <name> trebuie așezată la coadă la o casă              (ca înainte)
;   - (delay <index> <minutes>) - casa <index> este întârziată cu <minutes> minute         (ca înainte)
;   - (ensure <average>) - cât timp tt-ul mediu al caselor este mai mare decât
;                          <average>, se adaugă case fără restricții (case slow)           (ca înainte)
;   - <x> - trec <x> minute de la ultima cerere, iar starea caselor se actualizează
;           corespunzător (cu efect asupra câmpurilor tt, et, queue)                       (ca înainte)
;   - (close <index>) - casa index este închisă                                            (   NOU!   )
; Sistemul trebuie să proceseze cele 5 tipuri de cereri în ordine, astfel:
; - persoanele vor fi distribuite la casele DESCHISE cu tt minim; nu se va întâmpla
;   niciodată ca o persoană să nu poată fi distribuită la nicio casă                       (mică modificare)
; - când o casă suferă o întârziere, tt-ul și et-ul ei cresc (chiar dacă nu are clienți);
;   nu aplicați vreun tratament special caselor închise                                    (ca înainte)
; - tt-ul mediu (ttmed) se calculează pentru toate casele DESCHISE, 
;   iar la nevoie veți adăuga case slow una câte una, până când ttmed <= <average>         (mică modificare)
; - când timpul prin sistem avansează cu <x> minute, tt-ul, et-ul și queue-ul tuturor 
;   caselor se actualizează pentru a reflecta trecerea timpului; dacă unul sau mai mulți 
;   clienți termină de stat la coadă, ieșirile lor sunt contorizate în ordine cronologică. (ca înainte)
; - când o casă se închide, ea nu mai primește clienți noi; clienții care erau deja acolo
;   avansează normal, până la ieșirea din supermarket                                    
; Rezultatul funcției serve va fi o pereche cu punct între:
; - lista sortată cronologic a clienților care au părăsit supermarketul:
;   - fiecare element din listă va avea forma (index_casă . nume)
;   - dacă mai mulți clienți ies simultan, sortați-i crescător după indexul casei
; - lista cozilor (de la case DESCHISE sau ÎNCHISE) care încă au clienți:
;   - fiecare element va avea forma (index_casă . coadă) (coada este de tip queue)
;   - lista este sortată după indexul casei
(define (make_tt_list counters)
  (foldl (λ (current new_list) (cons (counter-tt current) new_list)) '() counters))

(define (calculate_ttmed fast-counters slow-counters)
  (/ (apply + (make_tt_list (append fast-counters slow-counters))) (length (append fast-counters slow-counters)))
  )

(define (add-slow-counter ttmed average index fast_counters slow_counters)
  (let* (
         (nclosed-fast (filter (λ (current) (equal? #f (counter-closed current))) fast_counters))
         (nclosed-slow (filter (λ (current) (equal? #f (counter-closed current))) slow_counters))
         )
    (if (< average ttmed)
        (add-slow-counter
         (calculate_ttmed nclosed-fast (append nclosed-fast (list (empty-counter (add1 index)))))
         average
         (add1 index)
         fast_counters
         (append slow_counters (list (empty-counter (add1 index))))
         )
        slow_counters
        )
    )
  )

(define (counter-update minutes counter exit-people)
  (cond
    ((zero? minutes) (cons counter exit-people))
    ((< minutes (counter-et counter)) (counter-update 0 ((pass-time-through-counter minutes) counter) exit-people))
    (else
     (if (queue-empty? (counter-queue counter))
         (counter-update 0 ((pass-time-through-counter minutes) counter) exit-people)
         (counter-update (- minutes (counter-et counter)) (remove-first-from-counter counter) (append exit-people (list (cons (counter-index counter) (car (top (counter-queue counter)))))))
         )
     )
      
    )
  )                      

(define (update-after-minutes minutes fast-counters slow-counters new_fast-counters new_slow-counters)
  (cond
    ((and (null? fast-counters) (null? slow-counters)) (list new_fast-counters new_slow-counters))
    ((not (null? fast-counters))
     (cond
       ((or (queue-empty? (counter-queue (car fast-counters))) (> (counter-et (car fast-counters)) minutes))
        (update-after-minutes minutes (cdr fast-counters) slow-counters 
                              (append new_fast-counters (update (pass-time-through-counter minutes) (list (car fast-counters)) (counter-index (car fast-counters)))) new_slow-counters))
       ((= (counter-et (car fast-counters)) minutes)
        (update-after-minutes minutes (cdr fast-counters) slow-counters 
                              (append new_fast-counters (list (remove-first-from-counter (car (update (pass-time-through-counter minutes) (list (car fast-counters)) (counter-index (car fast-counters))))))) new_slow-counters))
       (else (update-after-minutes minutes (cdr fast-counters) slow-counters 
                                   (append new_fast-counters (list (car (counter-update minutes (car fast-counters) '())))) new_slow-counters)) 
       )
     )
    (else
     (cond
       ((or (queue-empty? (counter-queue (car slow-counters))) (> (counter-et (car slow-counters)) minutes))
        (update-after-minutes minutes fast-counters (cdr slow-counters) new_fast-counters
                              (append new_slow-counters (update (pass-time-through-counter minutes) (list (car slow-counters)) (counter-index (car slow-counters))))))
       ((= (counter-et (car slow-counters)) minutes)
        (update-after-minutes minutes fast-counters (cdr slow-counters) new_fast-counters
                              (append new_slow-counters (list (remove-first-from-counter (car (update (pass-time-through-counter minutes) (list (car slow-counters)) (counter-index (car slow-counters)))))))))
       (else (update-after-minutes minutes fast-counters (cdr slow-counters)
                                   new_fast-counters (append new_slow-counters (list (car (counter-update minutes (car slow-counters) '())))))) 
       )
     )
    )
  )

(define (get-exit-people minutes counters exit-people)
  (let* (
         (counters-sorted-by-index (sort counters #:key counter-index <))
         (counters (sort counters-sorted-by-index #:key counter-et <))
         (min (if (and
                   (not (null? (filter (λ (C) (not (zero? (counter-et C)))) counters)))
                   (> minutes (counter-et (first (filter (λ (C) (not (zero? (counter-et C)))) counters))))
                   )
                  (counter-et (car (filter (λ (C) (not (zero? (counter-et C)))) counters))) 
                  minutes
                  )
              )
         (new_counters (foldr
          (λ (current new)
            (cond
              ((queue-empty? (counter-queue current))
               (cons current new))
              ((= (counter-et current) min) 
               (cons (remove-first-from-counter current) new))
              (else (cons ((pass-time-through-counter min) current) new))                    
              )
            )
          '()
          counters
          ))
         (new_exit-people (foldr
                           (λ (current new)
                             (if (and (= (counter-et current) min) (not (zero? min)))
                                 (cons (cons (counter-index current) (car (top (counter-queue current)))) new)
                                 new                  
                                 )
                             )
                           '()
                           counters
                           ))
         )  
    (if (<= minutes 0)
        exit-people
        (get-exit-people (- minutes min) new_counters (append exit-people new_exit-people))
        )
    )
  )



(define (close-counter index counters)
  (map (λ (C)
         (if (= (counter-index C) index)
             (make-counter (counter-index C) (counter-tt C) (counter-et C) (counter-queue C) #t)
             C
          )
         )
         counters)
  )

(define (serve-helper requests fast-counters slow-counters exit-people)
  (let* (
            (nclosed-fast (filter (λ (current) (equal? #f (counter-closed current))) fast-counters))
            (nclosed-slow (filter (λ (current) (equal? #f (counter-closed current))) slow-counters))
            (nclosed (filter (λ (current) (equal? #f (counter-closed current))) (append fast-counters slow-counters)))
            )
    (if (null? requests)
        (cons exit-people (foldr (λ (C new) (if (queue-empty? (counter-queue C))
                                                new
                                                (cons (cons
                                                             (counter-index C)
                                                             (queue (stream->list (queue-left (counter-queue C)))
                                                                   (queue-right (counter-queue C))
                                                                   (queue-size-l (counter-queue C))
                                                                   (queue-size-r (counter-queue C)))
                                                             )
                                                      new
                                                        )
                                            )
                                   )
                                 '()
                                 (append fast-counters slow-counters)
                                 )
              )
        (match (car requests)
          [(list 'close index)(if
                              (> index (counter-index (last fast-counters)))
                              (serve-helper (cdr requests) fast-counters (close-counter index slow-counters) exit-people)
                              (serve-helper (cdr requests) (close-counter index fast-counters) slow-counters exit-people)
                              )
                             ]
          [(list name n-items) #:when (not (equal? name 'ensure))
                               (if (<= n-items ITEMS)
                                   (if
                                    (> (car (min-tt nclosed)) (counter-index (last fast-counters)))
                                    (serve-helper (cdr requests) fast-counters (update (add-to-counter name n-items) slow-counters (car (min-tt nclosed-slow))) exit-people)
                                    (serve-helper (cdr requests) (update (add-to-counter name n-items) fast-counters (car (min-tt nclosed-fast))) slow-counters exit-people)
                                    )
                                   (serve-helper (cdr requests) fast-counters (update (add-to-counter name n-items) slow-counters (car (min-tt nclosed-slow))) exit-people)
                                   )
                               ]
          [(list 'delay index minutes) (if
                                        (> index (counter-index (last fast-counters)))
                                        (serve-helper (cdr requests) fast-counters (update (et+ minutes) (update (tt+ minutes) slow-counters index) index) exit-people)
                                        (serve-helper (cdr requests) (update (et+ minutes) (update (tt+ minutes) fast-counters index) index) slow-counters exit-people)
                                        )
                                       ]
          [(list 'ensure average) (serve-helper
                                   (cdr requests)
                                   fast-counters
                                   (add-slow-counter (calculate_ttmed nclosed-fast nclosed-slow) average (counter-index (last slow-counters)) fast-counters slow-counters)
                                   exit-people
                                   )
                                  ]
          [minutes (serve-helper
                    (cdr requests)
                    (first (update-after-minutes minutes fast-counters slow-counters '() '()))
                    (second (update-after-minutes minutes fast-counters slow-counters '() '()))
                    (append exit-people (get-exit-people minutes (append fast-counters slow-counters) '()))
                    )
                   ]
          )
        )
    )
  )

(define (serve requests fast-counters slow-counters)
  (serve-helper requests fast-counters slow-counters '())
  )


  