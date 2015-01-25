; ## Implementation details for the main API

(ns semantic-csv.impl.core
  (:require ))


(defn apply-kwargs
  "Utility that takes a function f, any number of regular args, and a final kw-args argument which will be
  splatted in as a final argument"
  [f & args]
  (apply (apply partial f (butlast args)) (apply concat (last args))))




