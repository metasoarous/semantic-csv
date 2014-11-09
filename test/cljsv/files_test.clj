(ns cljsv.files-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [cljsv.core :refer :all]))


(def test-fn "test/test.csv")

(deftest read-csv-file-test
  (testing "should work with filenames"
    (is (read-csv-file test-fn)))
  (testing "should work with file handles"
    (is (with-open [f (io/reader test-fn)]
          (read-csv-file f)))))


