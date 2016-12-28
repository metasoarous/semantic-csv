(ns semantic-csv.impl.core
  "This namespace consists of implementation details for the main API"
  (:require [clojure.string :as s]))


(defn mappify-row
  "Translates a single row of values into a map of `colname -> val`, given colnames in `header`."
  [header row]
  (into {} (map vector header row)))


(defn apply-kwargs
  "Utility that takes a function f, any number of regular args, and a final kw-args argument which will be
  splatted in as a final argument"
  [f & args]
  (apply
    (apply partial
           f
           (butlast args))
    (apply concat (last args))))


(defn stringify-keyword
  "Leaves strings alone. Turns keywords into the stringified version of the keyword, sans the initial `:`
  character. On anything else, calls str."
  [x]
  (cond
    (string? x)   x
    (keyword? x)  (->> x str (drop 1) (apply str))
    :else         (str x)))


(defn row-val-caster
  "Returns a function that casts casts a single row value based on specified casting function and
  optionally excpetion handler"
  [cast-fns exception-handler]
  (fn [row col]
    (let [cast-fn (if (map? cast-fns) (cast-fns col) cast-fns)]
      (try
        (update-in row [col] cast-fn)
        (catch Exception e
          (update-in row [col] (partial exception-handler col)))))))


(defn cast-row
  "Format the values of row with the given function. This gives us some flexbility with respect to formatting
  both vectors and maps in similar fashion."
  [cast-fns row & {:keys [only exception-handler]}]
  (let [cols (cond
               ; If only is specified, just use that
               only
                 (flatten [only])
               ; If cast-fns is a map, use those keys
               (map? cast-fns)
               (keys cast-fns)
               ; Then assume cast-fns is single fn, and fork on row type
               (map? row)
               (keys row)
               :else
                 (range (count row)))]
    (reduce (row-val-caster cast-fns exception-handler)
            (if (seq? row)
              (vec row)
              row)
            cols)))

(def not-blank?
  "Check if value is a non-blank string."
  (every-pred string? (complement s/blank?)))


;; The following is for copying

(defn- subset-map [m ks]
  (into {} (map (fn [k] [k (get m k)]) ks)))

(defmacro clone
  "Clone the var pointed to by fsym into current ns such that arglists, name and doc metadata are preserned."
  [fsym]
  (let [v (resolve fsym)
        m (subset-map (meta v) [:arglists :name :doc])
        m (update m :arglists (fn [arglists] (list 'quote arglists)))]
    `(def ~(vary-meta (:name m) (constantly m)) ~fsym)))


;; i'm not sure if the above will work for self-compiling cljs; below is some work on this, but it may just not be possible
;; May have to just catch that case and do a best effort clone without getting all the metadata

;(defn- qualified-name
;  [m]
;  (symbol (str (:ns m) "/" (:name m))))
;
;(defmacro clone
;  "Clone a "
;  [fvar]
;  (let [;v (resolve fsym)
;        m' (subset-map (meta fvar) [:arglists :name :doc :ns])
;        m (subset-map (meta fvar) [:arglists :name :doc])
;        m (update m :arglists (fn [arglists] (list 'quote arglists)))]
;    (println m)
;    `(def ~(vary-meta (:name m) (constantly m)) ~(qualified-name m'))))

(comment
  (ns whatnot)

  (defn stuff
    "Shit and yeah"
    [x y]
    :yeah)

  (ns semantic-csv.impl.core)

  (clone whatnot/stuff)
  (meta #'stuff)

  :end)


;; The following is ripped off from prismatic/plumbing:

(defmacro ?>>
  "Conditional double-arrow operation (->> nums (?>> inc-all? (map inc)))"
  [do-it? & args]
  `(if ~do-it?
     (->> ~(last args) ~@(butlast args))
     ~(last args)))

;; We include it here in lieue of depending on the full library due to dependency conflicts with other
;; libraries.


