;; # Higher level CSV parsing functionality
;;
;; The two most popular CSV parsing libraries for Clojure presently - `clojure.data.csv` and `clojure-csv` -
;; concern themselves only wtih the _syntax_ of CSV;
;; They take CSV text, transform it into a collection of vectors of string values, and nothing more.
;; Semantic CSV takes the next step by giving you tools for addressing the _semantics_ of your data, helping you put it into the form that better reflects what it means, and what's most useful for you.
;;
;; ## Features
;;
;; * Absorb header row as a vector of column names, and return remaining rows as maps of `column-name -> row-val`
;; * Write from a collection of maps, given a header
;; * When reading, apply casting functions by column name
;; * When writing, apply formatting functions by column name
;; * Remove lines starting with comment characters (by default `#`)
;; * Fully compatible with any CSV parsing library that retruning/writing a sequence of row vectors
;; * (SOON) A "sniffer" that reads in N lines, and uses them to guess column types
;;
;; ## Structure
;;
;; Semantic CSV _emphasizes_ a number of individual processing functions which can operate on the output of a
;; syntactic csv parser such as `clojure.data.csv` or `clojure-csv`.
;; This reflects a nice decoupling of grammar and semantics, in an effort to make this library as composable
;; and interoperable as possible.
;; However, as a convenience, we offer a few functions which wrap these individual steps with a set of
;; opinionated defaults and an option map for overriding the default behaviour.
;;
;; <br/>


(ns semantic-csv.core
  "# Core API namespace"
  (:require [clojure.java.io :as io]
            [clojure-csv.core :as csv]
            [semantic-csv.impl.core :as impl]
            [plumbing.core :as pc :refer [?>>]]))


;; To start, require this namespace, `clojure.java.io`, and your favorite CSV parser (e.g.,
;; [clojure-csv](https://github.com/davidsantiago/clojure-csv) or 
;; [clojure/data.csv](https://github.com/clojure/data.csv); we'll be using the former).
;; 
;;     (require '[semantic-csv.core :refer :all]
;;              '[clojure-csv.core :as csv]
;;              '[clojure.java.io :as io])
;;
;; Now let's take a tour through some of the processing functions we have available, starting with the reader
;; functions.
;;
;; <br/>


;; # Reader functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Note that all of these processing functions leave the rows collection as the final argument.
;; This is to make these functions interoperable with other standard collection processing functions (`map`,
;; `filter`, `take`, etc.) in the context of the `->>` threading macro.
;; You're welcome.


;; ## mappify-row

(defn mappify-row
  "Translates a single row of values into a map of `colname -> val`, given colnames in `header`."
  [header row]
  (into {} (map vector header row)))

;; We leave this in the main API as a courtesy in case you'd like to map lines over this function in your own
;; fashion.
;; However, in general, you'll want to use the following instead:


;; ## mappify

(defn mappify
  "Comsumes the first item as a header, and returns a seq of the remaining items as a maps with the header
  values as keys (see mappify-row)."
  ([rows]
   (mappify {} rows))
  ([{:keys [keyify] :or {keyify true} :as opts}
    rows]
   (let [header (first rows)
         header (if keyify (mapv keyword header) header)]
     (map (partial mappify-row header) (rest rows)))))

;; Here's an example to whet our whistle:
;;
;;     => (with-open [in-file (io/reader "test/test.csv")]
;;          (->>
;;            (csv/parse-csv in-file)
;;            mappify
;;            doall))
;;
;;     ({:this "# some comment lines..."}
;;      {:this "1", :that "2", :more "stuff"}
;;      {:this "2", :that "3", :more "other yeah"})
;;
;; Note that "# some comment lines..." was really intended to be left out of the _data_ as a comment.
;; We can solve this with the following function:


;; ## remove-comments

(defn remove-comments
  "Removes rows which start with a comment character (by default, `#`). Operates by checking the regular 
  expression against the first argument of every row in the collection."
  ([rows]
   (remove-comments #"^\#" rows))
  ([comment-re rows]
   (remove
     (fn [row]
       (let [x (first row)]
         (when x
           (re-find comment-re x))))
     rows)))

;; Let's see this in action with the above code:
;;
;;     => (with-open [in-file (io/reader "test/test.csv")]
;;          (->>
;;            (csv/parse-csv in-file)
;;            remove-comments
;;            mappify
;;            doall))
;;
;;     ({:this "1", :that "2", :more "stuff"}
;;      {:this "2", :that "3", :more "other yeah"})
;;
;; Much better :-)
;;
;; [**Sidenote**: it feels awkward to me that this operates _after_ the initial parsing step has already taken
;; place.
;; However, it's not clear how you would do this safely;
;; It seems you have to make assumptions about how things are going into the parsing function, which I'd
;; rather avoid.]


;; ## cast-with

(defn cast-with
  "Casts the vals of each row according to `cast-fns`, which maps `column-name -> casting-fn`."
  [cast-fns rows]
  (map
    (fn [row]
      (reduce
        (fn [row [col update-fn]]
          (update-in row [col] update-fn))
        row
        cast-fns))
    rows))

;; Note that we have a couple of numeric columns in the play data we've been dealing with.
;; Let's try casting them as such using `cast-with`:
;;
;;     => (with-open [in-file (io/reader "test/test.csv")]
;;          (->>
;;            (csv/parse-csv in-file)
;;            remove-comments
;;            mappify
;;            (cast-with {:this #(Integer/parseInt %)})
;;            doall))
;;
;;     ({:this 1, :that "2", :more "stuff"}
;;      {:this 2, :that "3", :more "other yeah"})
;;
;; Lovely :-)
;;
;; Note from the implementation here that each row need only be associative.
;; So map or vector rows are fine, but lists or lazy sequences would not be.


;; ## process

(defn process
  "This function wraps together all of the various input processing capabilities into one, with options
  controlled by an opts hash with a heavy-handed/opinionated set of defaults:

  * `:header`: bool; consume the first row as a header?
  * `:comment-re`: specify a regular expression to use for commenting out lines, or something falsey
     if filtering out comment lines is not desired.
  * `:remove-empty`: also remove empty rows? Defaults to true.
  * `:cast-fns`: optional map of `colname | index -> cast-fn`; row maps will have the values as output by the
     assigned `cast-fn`.
              `(cast-fns row-name)` to the string val"
  ([{:keys [comment-re header remove-empty cast-fns]
                  :or   {comment-re   #"^\#"
                         header       true
                         remove-empty true
                         cast-fns     {}}
                  :as opts}
    rows]
   (->> rows
        (?>> comment-re (remove-comments comment-re))
        (?>> header (mappify))
        (?>> cast-fns (cast-with cast-fns))))
  ; Use all defaults
  ([rows]
   (process {} rows)))

;; Using this function, the code we've been building above is reduced to the following:
;;
;;     (with-open [in-file (io/reader "test/test.csv")]
;;       (doall
;;         (process (csv/parse-csv in-file)
;;                  :cast-fns {:this #(Integer/parseInt %)})))

;; ## parse-and-process

(defn parse-and-process
  "This is a convenience function for reading a csv file using `clojure/data.csv` and passing it through `process`
  with the given set of options (specified _last_ as kw_args, in contrast with our other processing functions).
  Note that `:parser-opts` can be specified and will be passed along to `clojure-csv/parse-csv`"
  [csv-readable & {:keys [parser-opts]
                   :or   {parser-opts {}}
                   :as   opts}]
  (let [rest-options (dissoc opts :parser-opts)]
    (process
      rest-options
      (impl/apply-kwargs csv/parse-csv csv-readable parser-opts))))

;;     (with-open [in-file (io/reader "test/test.csv")]
;;       (doall
;;         (parse-and-process in-file
;;                            :cast-fns {:this #(Integer/parseInt %)})))


;; ## slurp-and-process

(defn slurp-and-process
  "This convenience function let's you `parse-and-process` csv data given a csv filename. Note that it is _not_
  lazy, and must read in all data so the file handle cna be closed."
  [csv-filename & {:as opts}]
  (let [rest-options (dissoc opts :parser-opts)]
    (with-open [in-file (io/reader csv-filename)]
      (doall
        (impl/apply-kwargs parse-and-process in-file opts)))))

;; For the ultimate in _programmer_ laziness:
;;
;;     (slurp-and-process "test/test.csv"
;;                        :cast-fns {:this #(Integer/parseInt %)})


;; <br/>


;; # Some parsing functions for your convenience
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; These functions can be imported and used in your `:cast-fns` specification

(defn ->int
  "Translating string into integers"
  [string]
  (Integer/parseInt string))

(defn ->float
  "Translate into float"
  [string]
  (Float/parseFloat string))

;;     (slurp-and-process "test/test.csv"
;;                        :cast-fns {:this ->int})


;; # Writer functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;; As with the input processing functions, the output processing functions are designed to be small, modular
;; peices you compose as you see fit.
;; Using these it's expected that you push your data through the processing functions and into a third party
;; writer.
;; But also as with the readers, we offer some higher level, default-opinionated, configurable functions that
;; do this composing for you, while the emphasis of the library remains with the composable functions.
;;
;; One of the first things we'll need is a function that takes a sequence of maps and turns it into a sequence
;; of vectors given column name order:

;; ## vectorify

(defn vectorify
  "Take a sequence of maps, and transform them into a sequence of vectors. Options:

  * `:header` - The header to be used. If not specified, this defaults to `(-> data first keys)`. Only
    values corresponding to the specified header will be included in the output, and will be included in the
    order corresponding to this argument.
  * `:prepend-header` - Defaults to true, and controls whether the `:header` vector should be prepended
    to the output sequence.
  * `:format-header` - If specified, this function will be called on each element of the `:header` vector, and
    the result prepended to the output sequence. The default behaviour is to leave strings alone but stringify
    keyword names such that the `:` is removed from their string representation. Passing a falsey value will
    leave the header unaltered in the output."
  ([data]
   (vectorify {} data))
  ([{:keys [header prepend-header format-header]
     :or {prepend-header true format-header impl/stringify-keyword}}
    data]
   ;; Grab the specified header, or the keys from the first data item. We'll
   ;; use these to `get` the appropriate values for each row.
   (let [header     (or header (-> data first keys))
         ;; This will be the formatted version we prepend if desired
         out-header (if format-header (mapv format-header header) header)]
     (->> data
          (map
            (fn [row] (mapv (partial get row) header)))
          (?>> prepend-header (cons out-header))))))


;; Let's see this in action:
;;
;;     => (let [data [{:this "a" :that "b"}
;;                    {:this "x" :that "y"}]]
;;          (vectorify data))
;;     (["this" "that"]
;;      ["a" "b"]
;;      ["x" "y"])
;;
;; With some options:
;;
;;     => (let [data [{:this "a" :that "b"}
;;                    {:this "x" :that "y"}]]
;;          (vectorify {:header [:that :this]
;;                      :preprend-header false}
;;                     data))
;;     (["b" "a"]
;;      ["y" "x"])


;; ## format-with

(defn format-with
  "Formats the values in `data` entries. First argument is a map of `colname -> format-fn` to be applied to
  entries for the given column name. Optional second argument is an options hash, with which you can specify
  an option for `:ignore-first`, useful for when you've already applied `vectorify` and don't want to run the
  format functions on the header row."
  ([formatters data]
   (format-with formatters {} data))
  ([formatters {:keys [ignore-first] :as opts} data]
   (->> data
        (?>> ignore-first (drop 1))
        ;; A little silly, this actually just uses cast-with
        (cast-with formatters data)
        (?>> ignore-first (cons (first data))))))

;; Note that this is actually just the `cast-with` function with an option for `:ignore-first`, potentially
;; useful when you have a header row you want to ignore.
;;
;;     => (let [data [{:this "a" :that "b"}
;;                    {:this "x" :that "y"}]]
;;          (->> data
;;               vectorify
;;               (format-with {:this (partial str "val-")}
;;                            {:ignore-first true})))
;;     (["this" "that"]
;;      ["val-a" "b"]
;;      ["val-x" "y"])


