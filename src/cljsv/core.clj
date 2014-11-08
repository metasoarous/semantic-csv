(ns cljsv.core
  (:require [clojure.java.io :as io]))


(defn- apply-kwargs
  "Takes a function f, any number of regular args, and a final kw-args argument which will be
  splatted in as a final argument"
  [f & args]
  (apply (apply partial f (butlast args)) (apply concat (last args))))

(defn trace
  [thing]
  (println thing) thing)


; This is cause I don't have data.csv right now...
(defn- raw-read-csv-string
  [csv-str]
  (->> csv-str
       clojure.string/split-lines
       (map #(clojure.string/split % #","))))


(defn read-csv-row
  "Translates a single row of values into a map of colname -> val, given colnames in header.
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


(defn ->int
  "Convenience function for translating strings into integers"
  [string]
  (Integer/parseInt string))

(defn ->float
  "Convenience function for translating strings into floats"
  [string]
  (Float/parseFloat string))


(defn read-csv-rows
  "Given a lines-iter, produces a sequence of hash-maps of col -> val mappings, as specified by
  a header in file (if specified; indices ow). Options
    header: bool; consume the first row as a header?
    comment-re: specify a regular expression to use for commenting out lines, or something falsey
                if this isn't desired
    remove-empty: also remove empty rows?
    cast-fns: optionally cast the vals in the map by applying the corresponding function in
              (cast-fns row-name) to the string val"
  [lines-iter & {:keys [comment-re header remove-empty cast-fns]
                 :or   {comment-re   #"^\#"
                        header       true
                        remove-empty true
                        cast-fns     {}}
                 :as opts}]
  (let [non-cmnt-lines (remove #(or (when comment-re
                                      (re-find comment-re %))
                                    (when remove-empty
                                      (re-find #"^\s*$" %)))
                               lines-iter)
        header (first (raw-read-csv-string (first non-cmnt-lines)))
        non-cmnt-lines (rest non-cmnt-lines)]
    (map
      (comp
        (partial read-csv-row
                 header 
                 (map #(or (cast-fns %) identity) header))
        first
        raw-read-csv-string)
      non-cmnt-lines)))


(defn read-csv-file
  [file-or-filename & opts]
  "Read csv in from a filename or file handle. For details see the docstring for read-csv-rows"
  (if (string? file-or-filename)
    (with-open [f (io/reader file-or-filename)]
      (apply-kwargs read-csv-file f opts))
    (apply-kwargs read-csv-rows (line-seq file-or-filename) opts)))


(defn read-csv-str
  "Read csv in from a csv string. For details see the docstring for read-csv-rows"
  [csv-str & opts]
  (apply-kwargs read-csv-rows (clojure.string/split-lines csv-str) opts))


