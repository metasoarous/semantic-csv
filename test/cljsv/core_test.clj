(ns cljsv.core-test
  (:require [clojure.test :refer :all]
            [cljsv.core :refer :all]))


(deftest main-test
  (testing "stuff should work"
    (let [parsed (read-csv-str "this,that\na,b\nc,d")]
      (is (= (first parsed) {:this "a" :that "b"}))
      (is (= (second parsed) {:this "c" :that "d"})))))
