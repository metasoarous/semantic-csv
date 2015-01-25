; ## Higher level CSV parsing functionality
;
; The most popular CSV parsing libraries for Clojure presently -- `clojure.data.csv` and `clojure-csv` -- are really focused on handling the _syntax_ of CSV;
; They take CSV text and transform it into collections of row vectors of string values, providing a minimal translation into the world of data.
; Semantic CSV takes it the next step by giving you tools for addressing the _semantics_ of your data, helping you put it into the form that better reflects what it means, and what's most useful for you.
;
; ## Features
; 
; To be less abstract about it, `semantic-csv` lets you easily:
; 
; * Absorb header row as a vector of column names, and return remaining rows as maps of `column-name -> row-val`
; * Write from a collection of maps, given a pre-specified `:header`
; * When reading, apply casting functions on a column by column basis (for casting to ints, floats, etc) via `:cast-fns`
; * When writing, apply formatting functions on a column by column basis via `:format-fns`, when `str` won't cut it
; * Remove lines starting with comment characters (by default `#`)
; * An optional "sniffer" that reads in N lines, and uses them to guess column types (SOON)
;
; ## This namespace...
;
; ...is the core of the API.


(ns semantic-csv.core
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]))


(defn read-csv-row
  "Translates a single row of values into a map of `colname -> val`, given colnames in header.
  The cast-fn arg should be a vector of translation functions the same length as header and row,
  and will be used to translate the raw string vals in row."
  [header cast-fns row]
  (into
    {}
    (map
      (fn [rowname cast-fn v] [rowname (cast-fn v)])
      header
      cast-fns
      row)))


(defn read-csv-rows
  "Given a `lines` collection, produces a seq of maps (`colname -> val`) where the column names are
  based on the first row's values.
  * `:header`: bool; consume the first row as a header?
  * `:comment-re`: specify a regular expression to use for commenting out lines, or something falsey
     if this isn't desired
  * `:remove-empty`: also remove empty rows?
  * `:cast-fns`: optional map of `colname | index -> cast-fn`; row maps will have the values as output by the
     assigned `cast-fn`.
              `(cast-fns row-name)` to the string val"
  [lines & {:keys [comment-re header remove-empty cast-fns]
                 :or   {comment-re   #"^\#"
                        header       true
                        remove-empty true
                        cast-fns     {}}
                 :as opts}]
  (let [non-cmnt-lines (remove #(or (when comment-re
                                      (re-find comment-re %))
                                    (when remove-empty
                                      (re-find #"^\s*$" %)))
                               lines)
        header (first (csv/read-csv (first non-cmnt-lines)))
        non-cmnt-lines (rest non-cmnt-lines)]
    (map
      (comp
        (partial read-csv-row
                 header 
                 (map #(or (cast-fns %) identity) header))
        first
        csv/read-csv)
      non-cmnt-lines)))


(defn read-csv-file
  "Read csv in from a filename or file handle. For details see the docstring for read-csv-rows"
  [file-or-filename & opts]
  (if (string? file-or-filename)
    (with-open [f (io/reader file-or-filename)]
      (doall
        (impl/apply-kwargs read-csv-file f opts)))
    (impl/apply-kwargs read-csv-rows (line-seq file-or-filename) opts)))


(defn read-csv-str
  "Read csv in from a csv string. For details see the docstring for read-csv-rows"
  [csv-str & opts]
  (impl/apply-kwargs read-csv-rows (clojure.string/split-lines csv-str) opts))



;; ## Some parsing functions for your convenience
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; These functions can be imported and used in your `:cast-fns` specification

(defn ->int
  "Translating string into integers"
  [string]
  (Integer/parseInt string))

(defn ->float
  "Translate into float"
  [string]
  (Float/parseFloat string))


