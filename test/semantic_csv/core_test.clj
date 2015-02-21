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
    (testing "should work with :only <seq>"
      (is (= (->> data
                  mappify
                  (cast-all ->int {:only [:that]})
                  first)
             {:this "1" :that 2})))
    (testing "should work with :only <colname>"
      (is (= (->> data
                  mappify
                  (cast-all ->int {:only :that})
                  first)
             {:this "1" :that 2})))))


(deftest casting-function-helpers-test
  (testing "with string inputs"
    (let [n "3443"
          x "4.555"]
      (is (= (->int n) 3443))
      (is (= (->long n) 3443))
      (is (= (->float x) (float 4.555)))
      (is (= (->double x) 4.555))))
  (testing "with numeric inputs"
    (doseq [x [3443 4.555]]
      (is (= (->double x) (double x)))
      (is (= (->float x) (float x))))
    (doseq [x [3443 4.555]
            f [->int ->long]]
      (is (= (f x) (long x))))))


(deftest process-test
  (testing "should generally work"
    (let [parsed-data [["this" "that" "more"]
                       ["a" "b" "c"]
                       ["d" "e" "f"]]]
      (testing "with defaults"
        (is (= (first (process parsed-data))
               {:this "a" :that "b" :more "c"}))
        (is (= (second (process parsed-data))
               {:this "d" :that "e" :more "f"})))
      (testing "with :header false"
        (is (= (first (process {:header false} parsed-data))
               ["this" "that" "more"]))
        (is (= (second (process {:header false} parsed-data))
               ["a" "b" "c"])))
      (testing "with defaults"
        (is (= (first (process {:cast-fns {:this #(str % "andstuff")}}
                               parsed-data))
               {:this "aandstuff" :that "b" :more "c"}))
        (is (= (second (process {:cast-fns {:this #(str % "andstuff")}}
                                parsed-data))
               {:this "dandstuff" :that "e" :more "f"}))))))


(deftest except-first-test
  (let [parsed-data [["this" "that" "more"]
                     ["a" "b" "c"]
                     ["d" "e" "f"]]]
    (testing "Should leave first row unchanged"
      (is (= (->> parsed-data
                  (except-first (cast-with {0 (partial str "X")}))
                  (first))
             ["this" "that" "more"])))
    (testing "Should generally work on remaining rows"
      (is (= (->> parsed-data
                  (except-first (cast-with {0 (partial str "X")})
                                (remove #(= (first %) "Xa")))
                  (second))
             ["Xd" "e" "f"])))))


