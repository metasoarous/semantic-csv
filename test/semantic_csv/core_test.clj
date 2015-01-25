(ns semantic-csv.core-test
  (:require [clojure.test :refer :all]
            [clojure-csv.core :as csv]
            [semantic-csv.core :refer :all]))


(deftest mappify-test
  (let [data [["this" "that"]
              ["x" "y"]]]
    (testing "mappify should work"
      (is (= (first (mappify data))
             {:this "x" :that "y"})))
    (testing "mappify should let you avoid keyifying"
      (is (= (first (mappify {:keyify false} data))
             {"this" "x" "that" "y"})))))


