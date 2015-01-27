;; # Higher level CSV parsing/processing functionality
;;
;; The two most popular CSV parsing libraries for Clojure presently - `clojure/data.csv` and `clojure-csv` -
;; concern themselves only wtih the _syntax_ of CSV;
;; They take CSV text, transform it into a collection of vectors of string values, and that's it.
;; Semantic CSV takes the next step by giving you tools for addressing the _semantics_ of your data, helping
;; you put it in a form that better reflects what it represents.
;;
;; ## Features
;;
;; * Absorb header row as a vector of column names, and return remaining rows as maps of `column-name -> row-val`
;; * Write from a collection of maps, given a header
;; * Apply casting/formatting functions by column name, while reading or writing
;; * Remove commented out lines (by default, those starting with `#`)
;; * Compatible with any CSV parsing library retruning/writing a sequence of row vectors
;; * (SOON) A "sniffer" that reads in N lines, and uses them to guess column types
;;
;; ## Structure
;;
;; Semantic CSV is structured around a number of composable processing functions for transforming data as it
;; comes out of or goes into a CSV file.
;; This leaves room for you to use whatever parsing/formatting tools you like, reflecting a nice decoupling
;; of grammar and semantics.
;; However, a couple of convenience functions are also provided which wrap these individual steps
;; in an opinionated but customizable manner, helping you move quickly while prototyping or working at the
;; REPL.
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
;; Now let's take a tour through some of the processing functions we have available, starting with the input
;; processing functions.
;;
;; <br/>


;; # Input processing functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Note that all of these processing functions leave the rows collection as the final argument.
;; This is to make these functions interoperable with other standard collection processing functions (`map`,
;; `filter`, `take`, etc.) in the context of the `->>` threading macro.
;; You're welcome.


;; <br/>
;; ## mappify

(defn mappify
  "Comsumes the first item as a header, and returns a seq of the remaining items as a maps with the header
  values as keys. Note that an optional `opts` map can be passed as a first arg, with the following option:
 
  * `:keyify` - specify whether header/column names should be turned into keywords (deafults to true)"
  ([rows]
   (mappify {} rows))
  ([{:keys [keyify] :or {keyify true} :as opts}
    rows]
   (let [header (first rows)
         header (if keyify (mapv keyword header) header)]
     (map (partial impl/mappify-row header) (rest rows)))))

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


;; <br/>
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


;; <br/>
;; ## cast-with

(defn cast-with
  "Casts the vals of each row according to `cast-fns`, which maps `column-name -> casting-fn`. An optional
  `opts` map can be used to specify:
  
  * `:ignore-first` - Ignore the first row in `rows`; Useful for preserving header rows"
  ([cast-fns rows]
   (cast-with cast-fns {} rows))
  ([cast-fns {:keys [ignore-first] :as opts} rows]
   (->> rows
        (?>> ignore-first (drop 1))
        (map
          (fn [row]
            (reduce
              (fn [row [col update-fn]]
                (update-in row [col] update-fn))
              row
              cast-fns)))
        (?>> ignore-first (cons (first rows))))))

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


;; <br/>
;; ## cast-all

(defn cast-all
  "Casts _multiple_ column values with the given function. Optional `opts` map can be used to specify:

  * `:only` - Only run `cast-fn` on these columns (default is to run on all columns)
  * `:ignore-first` - As in `cast-with`, you can optionally ignore the first row"
  ([cast-fn rows]
   (map (partial impl/cast-row cast-fn) rows))
  ([cast-fn {:keys [ignore-first only] :as opts} rows]
   (case (mapv boolean [ignore-first only])
     [false false]
       (cast-all cast-fn rows)
     [true false]
       (->> rows
            (drop 1)
            (cast-all cast-fn rows)
            (cons (first rows)))
     (let [cast-fns (into {} (map vector only cast-fn))]
       (cast-with cast-fns {:ignore-first ignore-first} rows)))))


;; <br/>
;; ## process

(defn process
  "This function wraps together all of the various input processing capabilities into one, with options
  controlled by an `opts` hash with opinionated defaults:

  * `:header` - bool; consume the first row as a header?
  * `:comment-re` - specify a regular expression to use for commenting out lines, or something falsey
     if filtering out comment lines is not desired.
  * `:remove-empty` - also remove empty rows? Defaults to true.
  * `:cast-fns` - optional map of `colname | index -> cast-fn`; row maps will have the values as output by the
     assigned `cast-fn`."
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


;; <br/>
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


;; <br/>
;; ## slurp-csv

(defn slurp-csv
  "This convenience function let's you `parse-and-process` csv data given a csv filename. Note that it is _not_
  lazy, and must read in all data so the file handle cna be closed."
  [csv-filename & {:as opts}]
  (let [rest-options (dissoc opts :parser-opts)]
    (with-open [in-file (io/reader csv-filename)]
      (doall
        (impl/apply-kwargs parse-and-process in-file opts)))))

;; For the ultimate in _programmer_ laziness:
;;
;;     (slurp-csv "test/test.csv"
;;                        :cast-fns {:this #(Integer/parseInt %)})


;; <br/>
;; # Some casting functions for your convenience
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

;;     (slurp-csv "test/test.csv"
;;                        :cast-fns {:this ->int})


;; <br/>
;; # Output processing functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;; As with the input processing functions, the output processing functions are designed to be small, modular
;; peices you compose together.
;; Using these it's expected that you push your data through the processing functions and into a third party
;; writer.
;; But as with the input processing functions, we offer some higher level, opinionated but configurable functions
;; which automate some of this for you.
;;
;; We've already looked at `cast-with` and `cast-all`, which can be useful as output as well as input
;; processing functions.
;; One of the first things we'll need is a function that takes a sequence of maps and turns it into a sequence
;; of vectors since this is what most of our csv writing/formatting libraries will want.


;; <br/>
;; ## vectorize

(defn vectorize
  "Take a sequence of maps, and transform them into a sequence of vectors. Options:

  * `:header` - The header to be used. If not specified, this defaults to `(-> rows first keys)`. Only
    values corresponding to the specified header will be included in the output, and will be included in the
    order corresponding to this argument.
  * `:prepend-header` - Defaults to true, and controls whether the `:header` vector should be prepended
    to the output sequence.
  * `:format-header` - If specified, this function will be called on each element of the `:header` vector, and
    the result prepended to the output sequence. The default behaviour is to leave strings alone but stringify
    keyword names such that the `:` is removed from their string representation. Passing a falsey value will
    leave the header unaltered in the output."
  ([rows]
   (vectorize {} rows))
  ([{:keys [header prepend-header format-header]
     :or {prepend-header true format-header impl/stringify-keyword}}
    rows]
   ;; Grab the specified header, or the keys from the first row. We'll
   ;; use these to `get` the appropriate values for each row.
   (let [header     (or header (-> rows first keys))
         ;; This will be the formatted version we prepend if desired
         out-header (if format-header (mapv format-header header) header)]
     (->> rows
          (map
            (fn [row] (mapv (partial get row) header)))
          (?>> prepend-header (cons out-header))))))


;; Let's see this in action:
;;
;;     => (let [data [{:this "a" :that "b"}
;;                    {:this "x" :that "y"}]]
;;          (vectorize data))
;;     (["this" "that"]
;;      ["a" "b"]
;;      ["x" "y"])
;;
;; With some options:
;;
;;     => (let [data [{:this "a" :that "b"}
;;                    {:this "x" :that "y"}]]
;;          (vectorize {:header [:that :this]
;;                      :preprend-header false}
;;                     data))
;;     (["b" "a"]
;;      ["y" "x"])


;; <br/>
;; ## batch

(defn batch
  "Takes sequence of items and returns a sequence of batches of items from the original
  sequence, at most `n` long."
  [n rows]
  (partition n n [] rows))

;; This function can be useful when working with `clojure-csv` when writing lazily.
;; The `clojure-csv.core/write-csv` function does not actually write to a file, but just formats the data you
;; pass in as a CSV string.
;; If you're working with a lot of data, calling this function would build a single massive string of the
;; results, and likely crash.
;; To write _lazily_, you have to take some number of rows, write them, and repeat till you're done.
;; Our `batch` function helps by giving you a lazy sequence of batches of `n` rows at a time, letting you pass
;; _that_ through to something that writes off the chunks lazily.


;; <br/>
;; ## spit-csv

(defn spit-csv
  "Convenience function for spitting out CSV data to a file using `clojure-csv`.

  * `file` - Can be either a filename string, or a file handle.
  * `opts` - Optional hash of settings.
  * `rows` - Can be a sequence of either maps or vectors; if the former, vectorize will be
      called on the input with `:header` argument specifiable through `opts`.

  The Options hash can have the following mappings:

  * `:batch-size` - How many rows to format and write at a time?
  * `:cast-fns` - Formatters to be run on row values. The names used as keys in this map should correspond
     to keys if rows are maps, and positional indices if they're vectors. Note that this function will call
     `str` on all values just before writing.
  * `:writer-opts` - Options hash to be passed along to `clojure-csv.core/write-csv`.
  * `:header` - Header to be passed along to `vectorize`, if necessary.
  * `:prepend-header` - Should the header be prepended to the rows written if `vectorize` is called?"
  ([file rows]
   (spit-csv file {} rows))
  ([file
    {:keys [batch-size cast-fns writer-opts header prepend-header]
     :or   {batch-size 20 prepend-header true}
     :as   opts}
    rows]
   (if (string? file)
     (with-open [file-handle (io/writer file)]
       (spit-csv file-handle opts rows))
     ; Else assume we already have a file handle
     (->> rows
          (?>> cast-fns (cast-with cast-fns))
          (?>> (-> rows first map?)
               (vectorize {:header header
                           :prepend-header prepend-header}))
          ; For save measure
          (format-all str)
          (batch batch-size)
          (map #(impl/apply-kwargs csv/write-csv % writer-opts))
          (reduce
            (fn [w rowstr]
              (.write w rowstr)
              w)
            file)))))

;; Like `slurp-csv`, this is a convenience function which wraps together a set of opinionated options
;; for writing data to the specified file handle or filename.
;; Note that since we use `clojure-csv` here, we offer a `:batch` option that lets you format and write small
;; batches of rows out at a time, to avoid contructing a massive string representation of all the data in the
;; case of bigger data sets.


;; <br/>
;; # One last example showing everything together
;;
;; Let's see how Semantic CSV in the context of a little data pipeline.
;; We're going to thread data in, tranform into maps, run some computations for each row and assoc in,
;; then write the modified data out to a file, all lazily.
;; First let's show this with `clojure/data.csv`, which I find a little easier to use for writing.
;;
;;     (require '[clojure.data.csv :as cd-csv])
;;
;;     (with-open [in-file (io/reader "test/test.csv")
;;                 out-file (io/writer "test-out.csv")]
;;       (->>
;;         (cd-csv/read-csv in-file)
;;         remove-comments
;;         mappify
;;         (cast-with {:this ->int :that ->float})
;;         ;; Do your processing...
;;         (map
;;           (fn [row]
;;             (assoc row :jazz (* (:this row)
;;                                 (:that row)))))
;;         vectorize
;;         (cd-csv/write-csv out-file)))
;;
;; Now let's see what this looks like with `clojure-csv`.
;; Note that as mentioned above, `clojure-csv` doesn't actually handle file writing for you, just formatting
;; into a CSV string.
;; So, to maintain lazyness, you'll have to add a couple steps to the end.
;; Additionally, it doesn't accept row items with anything that isn't a string, in contrast with
;; `clojure/data.csv` which casts to a string for you, so we'll have to account for that as well.
;;
;;     (with-open [in-file (io/reader "test/test.csv")
;;                 out-file (io/writer "test-out.csv")]
;;       (->>
;;         (csv/parse-csv in-file)
;;         ...
;;         (format-with str)
;;         (batch 1)
;;         (map csv/write-csv)
;;         (reduce
;;           (fn [w row]
;;             (.write w row)
;;             w)
;;           out-file)))
;;
;; <br/>


;; # That's it for the core API
;;
;; Hope you find this library useful.
;; If you have questions or comments please either [submit an issue](https://github.com/metasoarous/semantic-csv/issues)
;; or join us in the [dedicated chat room](https://gitter.im/metasoarous/semantic-csv).


