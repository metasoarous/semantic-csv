;; # Core functionality as Transducers
;;

(ns semantic-csv.transducers
  "# Transducers API namespace"
  (:require [clojure.java.io :as io]
            [clojure-csv.core :as csv]
            [semantic-csv.impl.core :as impl :refer [?>>]]))

;; # Input processing functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Implementations of the core api's functions as transducers.
;; This allows for single passes accross collections.
;;
;; This namespace also contains the helper functions seen in core.
;;


;; <br/>
;; ## mappify

(defn mappify
  "Takes a set of input options and returns a transducer.  The transducers will use the supplied header,
  or the first row if :header is nil, to create a map for each row, where the keys for the map are the header values.
  Options include:

  * `:keyify` - bool; specify whether header/column names should be turned into keywords (default: `true`).
  * `:header` - specify the header to use for map keys, preventing first row of data from being consumed as header."
  ([] (mappify {}))
  ([{:keys [keyify transform-header header] :or {keyify true} :as opts}]
   (fn [rf]
     (let [hdr (volatile! (mapv keyword header))]
       (fn
         ([] (rf))
         ([results] (rf results))
         ([results input]
          (if (empty? @hdr)
            (do (vreset! hdr (cond
                               transform-header (mapv transform-header input)
                               keyify (mapv keyword input)
                               :else input))
                results)
            (rf results (impl/mappify-row @hdr input)))))))))

;; Here's an example using the mappify transducers.
;;
;;     => (with-open [in-file (io/reader "test/test.csv")]
;;          (into []
;;                (mappify)
;;                (csv/parse-csv in-file))
;;
;;     [{:this "# some comment lines..."}
;;      {:this "1", :that "2", :more "stuff"}
;;      {:this "2", :that "3", :more "other yeah"}]
;;
;; <br/>
;;
;; Note that `"# some comment lines..."` was really intended to be left out as a comment.
;; We can address this with remove-comments:


;; <br/>
;; ## structify

(defn structify
  "Takes a set of input options and returns a transducer.  The transducers will use the supplied header,
  or the first row if :header is nil, to create a struct for each row, where the keys for the struct are the header values.
  Options include:

  * `:keyify` - bool; specify whether header/column names should be turned into keywords (default: `true`).
  * `:header` - specify the header to use for struct keys, preventing first row of data from being consumed as header."
  ([] (structify {}))
  ([{:keys [keyify transform-header header] :or {keyify true} :as opts}]
   (fn [rf]
     (let [hdr (volatile! header)]
       (fn
         ([] (rf))
         ([results] (rf results))
         ([results input]
          (if (empty? @hdr)
            (do (vreset! hdr (cond
                               transform-header (mapv transform-header input)
                               keyify (mapv keyword input)
                               :else input))
                results)
            (rf results (apply struct (apply create-struct @hdr) input)))))))))

;; Here's an example of structify:
;;
;;     => (with-open [in-file (io/reader "test/test.csv")]
;;          (into []
;;                (structify)
;;                (csv/parse-csv in-file))
;;
;;     ({:this "# some comment lines..."}
;;      {:this "1", :that "2", :more "stuff"}
;;      {:this "2", :that "3", :more "other yeah"})
;;
;; <br/>
;;
;; Note that `"# some comment lines..."` was really intended to be left out as a comment.
;; We can address this with remove-comments:


;; <br/>
;; ## remove-comments
(defn remove-comments
  "Returns a transducers that will remove rows which start with a comment character (by default, `#`).
  Operates by checking whether the first item of every row in the collection matches a comment pattern.
  Also removes empty lines. Options include:

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

;; Notice the remove-comments is placed before mappify.
;; We want this done in order.  The order flows from left to right in the compose.
;; This is a property of transducers.
;;
;;     => (with-open [in-file (io/reader "test/test.csv")]
;;          (into []
;;                (comp (remove-comments) (mappify))
;;                (csv/parse-csv in-file)))
;;
;;     ({:this "1", :that "2", :more "stuff"}
;;      {:this "2", :that "3", :more "other yeah"})
;;
;; <br/>
;; Next, let's observe that `:this` and `:that` point to strings, while they should really be pointing to
;; numeric values.
;; This can be addressed with the following function:

;; <br/>
;; ## cast-with

(defn cast-with
  "Returns a transducer that casts the vals of each row according to `cast-fns`, which must either
  be a map of `column-name -> casting-fn` or a single casting function to be applied towards all columns.
  Options include:

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
              (do (vreset! fst input) (rf results input)))                                                 ;; fst is nil. reset fst and return the results.
            (rf results (impl/cast-row cast-fns input :only only :exception-handler exception-handler)))))))))

;; Let's try casting a numeric column using this function:
;;
;;     => (with-open [in-file (io/reader "test/test.csv")]
;;          (into []
;;                (comp (remove-comments)
;;                      (mappify)
;;                      (cast-with {:this #(Integer/parseInt %)}))
;;                (csv/parse-csv in-file)))
;;
;;     ({:this 1, :that "2", :more "stuff"}
;;      {:this 2, :that "3", :more "other yeah"})
;;
;; Alternatively, if we want to cast multiple columns using a single function, we can do so
;; by passing a single casting function as the first argument.
;;
;;     => (with-open [in-file (io/reader "test/test.csv")]
;;          (into []
;;                (comp (remove-comments)
;;                      (mappify)
;;                      (cast-with #(Integer/parseInt %) {:only [:this :that]}))
;;                (csv/parse-csv in-file)))
;;
;;     ({:this 1, :that 2, :more "stuff"}
;;      {:this 2, :that 3, :more "other yeah"})
;;
;; Note that this function handles either map or vector rows.
;; In particular, if you’ve imported data without consuming a header (by either not using mappify or
;; by passing `:header false` to `process` or `slurp-csv` below), then the columns can be keyed by their
;; zero-based index.
;; For instance, `(cast-with {0 #(Integer/parseInt %) 1 #(Double/parseDouble %)} rows)`
;; will parse the first column as integers and the second as doubles.
;;
;; <br/>
;; It's worth pointing out this function isn't strictly an _input_ processing function, but could be
;; used for intermediate or output preparation processing.


;; <br/>
;; ## process

(defn process
  "Returns a transducers that composes the most frequently used input processing capabilities,
  and is controlled by an `opts` hash with opinionated defaults:

  * `:mappify` - bool; transform rows from vectors into maps using `mappify`.
  * `:keyify` - bool; specify whether header/column names should be turned into keywords (default: `true`).
  * `:header` - specify header to be used in mappify; as per `mappify`, first row will not be consumed as header
  * `:structs` - bool; use structify insead of mappify
  * `:remove-comments` - bool; remove comment lines, as specified by `:comment-re` or `:comment-char`. Also
     removes empty lines. Defaults to `true`.
  * `:comment-re` - specify a regular expression to use for commenting out lines.
  * `:comment-char` - specify a comment character to use for filtering out comments; overrides comment-re.
  * `:cast-fns` - optional map of `colname | index -> cast-fn`; row maps will have the values as output by the
     assigned `cast-fn`.
  * `:cast-exception-handler` - If cast-fn raises an exception, this function will be called with args
    `colname, value`, and the result used as the parse value.
  * `:cast-only` - Only cast the specified column(s); can be either a single column name, or a vector of them."
  ([] (process {}))
  ([{:keys [mappify keyify header remove-comments comment-re comment-char structs cast-fns cast-exception-handler cast-only]
     :or   {mappify         true
            keyify          true
            remove-comments true
            comment-re      #"^\#"}
     :as opts}]
   (let [map-fn (when mappify
                  (if structs ;; use mappify or structify
                    (semantic-csv.transducers/structify {:keyify keyify :header header})
                    (semantic-csv.transducers/mappify {:keyify keyify :header header})))
         remove-fn (when remove-comments
                     (semantic-csv.transducers/remove-comments {:comment-re comment-re :comment-char comment-char}))
         cast-with-fn (when cast-fns
                        (semantic-csv.transducers/cast-with cast-fns {:exception-handler cast-exception-handler :only cast-only}))]
     (apply comp (remove nil? [remove-fn map-fn cast-with-fn])))))

;; Using this function, the code we've been building above is reduced to the following:
;;
;;     (with-open [in-file (io/reader "test/test.csv")]
;;       (into []
;;             (process {:cast-fns {:this #(Integer/parseInt %)}})
;;             (csv/parse-csv in-file)))


;; <br/>
;; ## parse-and-process

(defn parse-and-process
  "This is a convenience function for reading a csv file using `clojure-csv` and passing it through `process`
  with the given set of options (specified _last_ as kw_args, in contrast with our other processing functions).
  Note that `:parser-opts` can be specified and will be passed along to `clojure-csv/parse-csv`"
  [csv-readable & {:keys [parser-opts]
                   :or   {parser-opts {}}
                   :as   opts}]
  (let [rest-options (dissoc opts :parser-opts)]
    (into [] (process rest-options)
     (impl/apply-kwargs csv/parse-csv csv-readable parser-opts))))

;;     (with-open [in-file (io/reader "test/test.csv")]
;;         (parse-and-process in-file
;;                            :cast-fns {:this #(Integer/parseInt %)}))


;; <br/>
;; ## slurp-csv

(defn slurp-csv
  "This convenience function let's you `parse-and-process` csv data given a csv filename. Note that it is _not_
  lazy, and must read in all data so the file handle can be closed."
  [csv-filename & {:as opts}]
  (let [rest-options (dissoc opts :parser-opts)]
    (with-open [in-file (io/reader csv-filename)]
       (impl/apply-kwargs parse-and-process in-file opts))))

;; For the ultimate in _programmer_ laziness:
;;
;;     (slurp-csv "test/test.csv"
;;                :cast-fns {:this #(Integer/parseInt %)})


;; <br/>
;; # Some casting functions for your convenience
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; These functions can be imported and used in your `:cast-fns` specification.
;; They focus on handling some of the mess of dealing with numeric casting.
;; None of these functions are transducers.

;; ## ->int

(defn ->int
  "Translate to int from string or other numeric. If string represents a non integer value,
  it will be rounded down to the nearest int."
  [v]
  (if (string? v)
    (-> v clojure.string/trim Double/parseDouble int)
    (int v)))

;; ## ->long

(defn ->long
  "Translate to long from string or other numeric. If string represents a non integer value,
  will be rounded down to the nearest long."
  [v]
  (if (string? v)
    (-> v clojure.string/trim Double/parseDouble long)
    (long v)))

;; ## ->float

(defn ->float
  "Translate to float from string or other numeric."
  [v]
  (if (string? v)
    (-> v clojure.string/trim Float/parseFloat)
    (float v)))

;; ## ->double

(defn ->double
  "Translate to double from string or other numeric."
  [v]
  (if (string? v)
    (-> v clojure.string/trim Double/parseDouble)
    (double v)))

;;     (slurp-csv "test/test.csv"
;;                :cast-fns {:this ->int})

;; Note these functions place a higher emphasis on flexibility and convenience than performance, as you can
;; likely see from their implementations.
;; If maximum performance is a concern for you, and your data is fairly regular, you may be able to get away with
;; less robust functions, which shouldn't be hard to implement yourself.
;; For most cases though, the performance of those provided here should be fine.


;; <br/>
;; # Output processing functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;; As with the input processing functions, the output processing functions are designed to be small,
;; composable pieces which help you push your data through to a third party writer.
;; And as with the input processing functions, higher level, opinionated, but configurable functions
;; are offered which automate some of this for you.
;;
;; We've already looked at `cast-with`, which can be useful in output as well as input
;; processing.
;; Another important function we'll need is one that takes a sequence of maps and turns it into a sequence
;; of vectors since this is what most of our csv writing/formatting libraries will want.


;; <br/>
;; ## vectorize

(defn vectorize
  "Returns a transducer that transforms maps where the keys are headers (as produced by mappify)
  and transforms them into a sequence of vectors. Options include:

  * `:header` - The header to be used. If not specified, this defaults to `(-> rows first keys)`. Only
    values corresponding to the specified header will be included in the output, and will be included in the
    order corresponding to this argument.
  * `:prepend-header` - Defaults to true, and controls whether the `:header` vector should be prepended
    to the output sequence.
  * `:format-header` - If specified, this function will be called on each element of the `:header` vector, and
    the result prepended to the output sequence. The default behaviour is to leave strings alone but stringify
    keyword names such that the `:` is removed from their string representation. Passing a falsey value will
    leave the header unaltered in the output."
  ([]
   (vectorize {}))
  ([{:keys [header prepend-header format-header]
     :or {prepend-header true format-header impl/stringify-keyword}}]
   (fn [rf]
     (let [hdr (volatile! header)
           prepend-hdr (volatile! prepend-header)]
       (fn
         ([] (rf))
         ([result] (rf result))
         ([result input]
          (when (empty? @hdr)
            (do (vreset! hdr (into [] (keys input)))))
          (if @prepend-hdr
            (do (vreset! prepend-hdr false)
              (rf
                 (if format-header
                   (rf result (mapv format-header @hdr))
                   (rf result @hdr))
                 (mapv (partial get input) @hdr)))
            (rf result (mapv (partial get input) @hdr)))))))))


;; Let's see this in action:
;;
;;     => (let [data [{:this "a" :that "b"}
;;                    {:this "x" :that "y"}]]
;;          (into [] (vectorize) data))
;;     (["this" "that"]
;;      ["a" "b"]
;;      ["x" "y"])
;;
;; With some options:
;;
;;     => (let [data [{:this "a" :that "b"}
;;                    {:this "x" :that "y"}]]
;;          (into []
;;                (vectorize {:header [:that :this]
;;                           :prepend-header false})
;;                data))
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
;; And as promised, a function for doing all the dirty work for you:


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
  * `:cast-fns` - Formatter(s) to be run on row values. As with `cast-with` function, can be either a map
     of `column-name -> cast-fn`, or a single function to be applied to all values. Note that `str` is called
     on all values just before writing regardless of `:cast-fns`.
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
     (let [cast-fn (when cast-fns (cast-with cast-fns))
           vect-fn (when (-> rows first map?) (vectorize {:header header :prepend-header prepend-header}))
           vect-rows (sequence
                      (apply comp (remove nil? [cast-fn vect-fn (cast-with str)]))
                      rows)]
       (->> vect-rows
            (batch batch-size)
            (map #(impl/apply-kwargs csv/write-csv % writer-opts))
            (reduce
             (fn [w rowstr]
               (.write w rowstr)
               w)
             file))))))

;; Note that since we use `clojure-csv` here, we offer a `:batch-size` option that lets you format and write small
;; batches of rows out at a time, to avoid constructing a massive string representation of all the data in the
;; case of bigger data sets.


;; <br/>
;; # One last example showing everything together
;;
;; Let's see how Semantic CSV works in the context of a little data pipeline.
;; We're going to thread data in, transform into maps, run some computations for each row and assoc in,
;; then write the modified data out to a file, all lazily.
;;
;; First let's show this with `clojure/data.csv`, which I find a little easier to use for writing:
;;
;;     (require '[clojure.data.csv :as cd-csv])
;;
;;     (with-open [in-file (io/reader "test/test.csv")
;;                 out-file (io/writer "test-out.csv")]
;;       (->>
;;         (sequence
;;           (comp
;;             (remove-comments)
;;             (mappify)
;;             (cast-with {:this ->int :that ->float})
;;             ;; Do your processing...
;;             (map
;;               (fn [row]
;;                 (assoc row :jazz (* (:this row)
;;                                     (:that row)))))
;;             (vectorize))
;;           (cd-csv/read-csv in-file))
;;         (cd-csv/write-csv out-file)))
;;
;;
;; <br/>
;; And there you have it.
;; A simple, composable, and easy to use library for taking you the extra mile with CSV processing in
;; Clojure.
