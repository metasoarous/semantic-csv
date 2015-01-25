(ns semantic-csv.files-test
  (:require [clojure.test :refer :all]
            [semantic-csv.core :refer :all]))


(def test-fn "test/test.csv")

(deftest slurp-and-process-test
  (testing "should work"
    (is (= (first (slurp-and-process test-fn))
           {:this "1" :that "2" :more "stuff"})))
  (testing "should work with cast-fns"
    (is (= (first (slurp-and-process test-fn :cast-fns {:this ->int}))
           {:this 1 :that "2" :more "stuff"}))))


