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

(defn process
  "This function wraps together the most frequently used input processing capabilities,
  controlled by an `opts` hash with opinionated defaults:

  * `:mappify` - bool; transform rows from vectors into maps using `mappify`.
  * `:keyify` - bool; specify whether header/column names should be turned into keywords (default: `true`).
  * `:header` - specify header to be used in mappify; as per `mappify`, first row will not be consumed as header
  * `:structs` - bool; use structs instead of array-maps or hash-maps in mappify.
  * `:remove-comments` - bool; remove comment lines, as specified by `:comment-re` or `:comment-char`. Also
     removes empty lines. Defaults to `true`.
  * `:comment-re` - specify a regular expression to use for commenting out lines.
  * `:comment-char` - specify a comment character to use for filtering out comments; overrides comment-re.
  * `:cast-fns` - optional map of `colname | index -> cast-fn`; row maps will have the values as output by the
     assigned `cast-fn`.
  * `:cast-exception-handler` - If cast-fn raises an exception, this function will be called with args
    `colname, value`, and the result used as the parse value.
  * `:cast-only` - Only cast the specified column(s); can be either a single column name, or a vector of them."
  ([{:keys [mappify keyify header remove-comments comment-re comment-char structs cast-fns cast-exception-handler cast-only]
     :or   {mappify         true
            keyify          true
            remove-comments true
            comment-re      #"^\#"}
     :as opts}]
   (let [map-fn (when mappify
                  (if structs
                    (semantic-csv.transducers/structify {:keyify keyify :header header})
                    (semantic-csv.transducers/mappify {:keyify keyify :header header}))) ;; use mappify or structify
         remove-fn (when remove-comments
                     (semantic-csv.transducers/remove-comments {:comment-re comment-re :comment-char comment-char}))
         cast-with-fn (when cast-fns
                        (semantic-csv.transducers/cast-with cast-fns {:exception-handler cast-exception-handler :only cast-only}))
         ]
     (apply comp (remove nil? [remove-fn map-fn cast-with-fn]))))
  ; Use all defaults
  ([]
   (process {})))

(defn parse-and-process
  "This is a convenience function for reading a csv file using `clojure/data.csv` and passing it through `process`
  with the given set of options (specified _last_ as kw_args, in contrast with our other processing functions).
  Note that `:parser-opts` can be specified and will be passed along to `clojure-csv/parse-csv`"
  [csv-readable & {:keys [parser-opts]
                   :or   {parser-opts {}}
                   :as   opts}]
  (let [rest-options (dissoc opts :parser-opts)]
    (transduce (process rest-options) conj []
     (impl/apply-kwargs csv/parse-csv csv-readable parser-opts))))

(defn slurp-csv
  "This convenience function let's you `parse-and-process` csv data given a csv filename. Note that it is _not_
  lazy, and must read in all data so the file handle can be closed."
  [csv-filename & {:as opts}]
  (let [rest-options (dissoc opts :parser-opts)]
    (with-open [in-file (io/reader csv-filename)]
       (impl/apply-kwargs parse-and-process in-file opts))))
