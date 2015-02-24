(ns semantic-csv.files-test
  (:require [clojure.test :refer :all]
            [clojure-csv.core :as csv]
            [clojure.java.io :as io]
            [semantic-csv.core :refer :all]))


(def test-fn "test/test.csv")


(deftest slurp-csv-test
  (testing "should work"
    (is (= (first (slurp-csv test-fn))
           {:this "1" :that "2" :more "stuff"})))
  (testing "should work with cast-fns"
    (is (= (first (slurp-csv test-fn :cast-fns {:this ->int}))
           {:this 1 :that "2" :more "stuff"}))))


(deftest spit-csv-test
  (let [data [["this" "that"]
              ["1" "2"]
              ["3" "4"]]
        test-f "test/testout.csv"]
    (testing "basic spitting"
      (is (= (do (spit-csv test-f data)
                 (with-open [f (io/reader test-f)]
                   (doall (csv/parse-csv f))))
             data)))))


