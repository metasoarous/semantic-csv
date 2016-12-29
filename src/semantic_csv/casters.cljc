(ns semantic-csv.casters
  (:require [clojure.string :as string]
            [semantic-csv.impl.core :as impl]))

;; <br/>
;; # A Helper function to use with mappify to replace spaces in headers.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; ## ->idiomatic-keyword

(defn ->idiomatic-keyword
  "Takes a string, replacing consecutive underscores and spaces with a single dash(-),
  then returns a keyword based on the transformed string."
  [x]
  (-> x (string/replace #"[ _]+" "-") string/lower-case keyword))

;; <br/>
;; # Some casting functions for your convenience
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; These functions can be imported and used in your `:cast-fns` specification.
;; They focus on handling some of the mess of dealing with casting to numerics and booleans.

;; ## ->int

(defn ->int
  "Translate to int from string or other numeric. If string represents a non integer value,
  it will be rounded down to the nearest int.

  An opts map can be specified as the first arguments with the following options:
  * `:nil-fill` - return this when input is empty/nil."
  ([x]
   (->int {} x))
  ([{:keys [nil-fill]} x]
   (cond
     (impl/not-blank? x) (-> x string/trim Double/parseDouble int)
     (number? x) (int x)
     :else nil-fill)))

;; ## ->long

(defn ->long
  "Translate to long from string or other numeric. If string represents a non integer value,
  will be rounded down to the nearest long.

  An opts map can be specified as the first arguments with the following options:
  * `:nil-fill` - return this when input is empty/nil."
  ([x]
   (->long {} x))
  ([{:keys [nil-fill]} x]
   (cond
     (impl/not-blank? x) (-> x string/trim Double/parseDouble long)
     (number? x) (long x)
     :else nil-fill)))

;; ## ->float

(defn ->float
  "Translate to float from string or other numeric.

  An opts map can be specified as the first arguments with the following options:
  * `:nil-fill` - return this when input is empty/nil."
  ([x]
   (->float {} x))
  ([{:keys [nil-fill]} x]
   (cond
     (impl/not-blank? x) (-> x string/trim Float/parseFloat)
     (number? x) (float x)
     :else nil-fill)))

;; ## ->double

(defn ->double
  "Translate to double from string or other numeric.

  An opts map can be specified as the first arguments with the following options:
  * `:nil-fill` - return this when input is empty/nil."
  ([x]
   (->double {} x))
  ([{:keys [nil-fill]} x]
   (cond
     (impl/not-blank? x) (-> x string/trim Double/parseDouble)
     (number? x) (double x)
     :else nil-fill)))

;; ## ->boolean

(defn ->boolean
  "Translate to boolean from string or other numeric.

  An opts map can be specified as the first arguments with the following options:
  * `:nil-fill` - return this when input is empty/nil."
  ([x]
   (->boolean {} x))
  ([{:keys [nil-fill]} x]
   (cond
     (string? x) (case (-> x string/trim string/lower-case)
                   ("true" "yes" "t") true
                   ("false" "no" "f") false
                   "" nil-fill)
     (number? x) (not (zero? x))
     (nil? x) nil-fill
     :else (boolean x))))

