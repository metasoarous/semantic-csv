(ns semantic-csv.transducers
  (:require [clojure.java.io :as io]
            [clojure-csv.core :as csv]
            [semantic-csv.impl.core :as impl :refer [?>>]]))

(defn mappify
  "Takes a sequence of row vectors, as commonly produced by csv parsing libraries, and returns a sequence of
  maps. By default, the first row vector will be interpreted as a header, and used as the keys for the maps.
  However, this and other behaviour are customizable via an optional `opts` map with the following options:

  * `:keyify` - bool; specify whether header/column names should be turned into keywords (default: `true`).
  * `:header` - specify the header to use for map keys, preventing first row of data from being consumed as header."
  ([] (mappify {}))
  ([{:keys [keyify header] :or {keyify true} :as opts}]
   (fn [rf]
     (let [hdr (volatile! header)]
       (fn
         ([] (rf))
         ([results] (rf results))
         ([results input]
          (if (empty? @hdr)
            (do (vreset! hdr (if keyify (mapv keyword input) input))
                results)
            (rf results (impl/mappify-row @hdr input)))))))))

(defn structify
  "Takes a sequence of row vectors, as commonly produced by csv parsing libraries, and returns a sequence of
  structs. By default, the first row vector will be interpreted as a header, and used as the keys for the maps.
  However, this and other behaviour are customizable via an optional `opts` map with the following options:
 
  * `:keyify` - bool; specify whether header/column names should be turned into keywords (default: `true`).
  * `:header` - specify the header to use for map keys, preventing first row of data from being consumed as header."
  ([] (structify {}))
  ([{:keys [keyify header] :or {keyify true} :as opts}]
   (fn [rf]
     (let [hdr (volatile! header)]
       (fn
         ([] (rf))
         ([results] (rf results))
         ([results input]
          (if (empty? @hdr)
            (do (vreset! hdr (if keyify (mapv keyword input) input))
                results)
            (rf results (apply struct (apply create-struct @hdr) input)))))))))

(defn remove-comments
  "Removes rows which start with a comment character (by default, `#`). Operates by checking whether
  the first item of every row in the collection matches a comment pattern. Also removes empty lines.
  Options include:

  * `:comment-re` - Specify a custom regular expression for determining which lines are commented out.
  * `:comment-char` - Checks for lines lines starting with this char.

  Note: this function only works with rows that are vectors, and so should always be used before mappify."
  ([] (remove-comments {:comment-re #"^\#"}))
  ([{:keys [comment-re comment-char]}]
   (fn [rf]
     (let [commented? (if comment-char
                        #(= comment-char (first %))
                        (partial re-find comment-re))]
       (fn
         ([] (rf))
         ([results] (rf results))
         ([results input]
          (let [x (first input)]
            (if (and x (commented? x))
              results
              (rf results input)))))))))

(defn cast-with
  "Casts the vals of each row according to `cast-fns`, which must either be a map of
  `column-name -> casting-fn` or a single casting function to be applied towards all columns.
  Additionally, an `opts` map can be used to specify:

  * `:except-first` - Leaves the first row unaltered; useful for preserving header row.
  * `:exception-handler` - If cast-fn raises an exception, this function will be called with args
    `colname, value`, and the result used as the parse value.
  * `:only` - Only cast the specified column(s); can be either a single column name, or a vector of them."
  ([cast-fns]
   (cast-with cast-fns {}))
  ([cast-fns {:keys [except-first exception-handler only] :as opts}]
   (fn [rf]
     (let [fst (volatile! nil)]
       (fn
         ([] (rf))
         ([results] (rf results))
         ([results input]
          (if except-first
            (if @fst
              (rf results (impl/cast-row cast-fns input :only only :exception-handler exception-handler))  ;; we captured the first already, keep reducing.
              (do (vreset! fst input) (rf results input))) ;; fst is nil. reset fst and return the results.
            (rf results (impl/cast-row cast-fns input :only only :exception-handler exception-handler)))))))))

;; TODO: properly implement except first as transducer.  need to take n functions and apply then in order as in ->>.
;; (defn except-first
;;   []
;;   (fn [rf]
;;     (let [fst (volatile! nil)]
;;       (fn
;;         ([] (rf))
;;         ([results] (rf results))
;;         ([results input]
;;          (if @fst
;;            (rf results)))))))
