(ns semantic-csv.transducers
  (:require [clojure.java.io :as io]
            [clojure-csv.core :as csv]
            [semantic-csv.impl.core :as impl :refer [?>>]]))

(defn mappify
  ([] (mappify {}))
  ([{:keys [keyify header structs] :or {keyify true} :as opts}]
   (fn [rf]
     (let [hdr (volatile! header)]
       (fn
         ([] (rf))
         ([results] (rf results))
         ([results input]
          (if (empty? @hdr)
            (do (vreset! hdr (if keyify (mapv keyword input) input))
                results)
            (rf results (impl/mappify-row @hdr input)))))))))
