(ns semantic-csv.core-test
  (:require [clojure.test :refer :all]
            [clojure-csv.core :as csv]
            [semantic-csv.core :refer :all]))


(deftest mappify-test
  (let [data [["this" "that"]
              ["x" "y"]
              ["# some comment"]]]
    (testing "mappify should work"
      (is (= (first (mappify data))
             {:this "x" :that "y"})))
    (testing "mappify should let you avoid keyifying"
      (is (= (first (mappify {:keyify false} data))
             {"this" "x" "that" "y"})))
    (testing "mappify should not regard comments"
      (is (= (last (mappify data))
             {:this "# some comment"})))))


(deftest remove-comments-test
  (let [data [["# a comment"]
              ["// another comment"]]]
    (testing "remove-comments should remove #-designated comments by default"
      (is (= (remove-comments data)
             [["// another comment"]])))
    (testing "remove-comments should take an optional comment designator"
      (is (= (remove-comments #"^//" data)
             [["# a comment"]])))))


(deftest cast-with-test
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
             [2 "that"])))
    (testing "should work with :ignore-first"
      (is (= (->> data
                  (cast-with {0 ->int} {:ignore-first true}))
             [["1" "this"] [2 "that"]])))))


(deftest cast-all-test
  (let [data [["this" "that"]
              ["1" "2"]]]
    (testing "should work with mappify"
      (is (= (->> data
                  mappify
                  (cast-all ->int)
                  first)
             {:this 1 :that 2})))
    (testing "should work with :ignore-first"
      (is (= (->> data
                  (cast-all ->int {:ignore-first true}))
             [["this" "that"] [1 2]])))
    (testing "should work with :only"
      (is (= (->> data
                  mappify
                  (cast-all ->int {:only :that})
                  first)
             {:this "1" :that 2})))))
