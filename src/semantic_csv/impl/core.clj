(ns semantic-csv.impl.core
  "This namespace consists of implementation details for the main API")


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


(defn cast-row
  "Format the values of row with the given function. This gives us some flexbility with respect to formatting
  both vectors and maps in similar fashion."
  [cast-fn row]
  (cond
    (map? row)
      (into {}
        (map (fn [[k v]] [k (cast-fn v)]) row))
    :else (mapv cast-fn row)))


