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


(deftest casting-test
  (let [data [["this" "that"]
              ["1" "y"]]]
    (testing "should work with mappify"
      (is (= (->> data
                  mappify
                  (cast-with {:this ->int})
                  first)
             {:this 1 :that "y"}))))
  (let [data [["1" "this"]
              ["2" "that"]]]
    (testing "should work without mappify"
      (is (= (->> data
                  (cast-with {0 ->int})
                  second)
             [2 "that"])))))



