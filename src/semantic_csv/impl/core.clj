(ns semantic-csv.impl.core
  "This namespace consists of implementation details for the main API")


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


(defn format-row-with
  "Format the values of row with the given function. This gives us some flexbility with respect to formatting
  both vectors and maps in similar fashion."
  [formatter row]
  (cond
    (map? row)
      (into {}
        (map (fn [[k v]] [k (formatter v)]) row))
    :else (mapv formatter row)))


