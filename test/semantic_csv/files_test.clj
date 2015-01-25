(ns semantic-csv.files-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [semantic-csv.core :refer :all]))


(def test-fn "test/test.csv")

(deftest read-csv-file-test
  (testing "should work with filenames"
    (is (read-csv-file test-fn)))
  (testing "should work with file handles"
    (is (with-open [f (io/reader test-fn)]
          (doall
            (read-csv-file f)))))
  (testing "should have the correct content"
    (is (some #(= % {"this" "1" "that" "2" "more" "stuff"})
              (read-csv-file test-fn)))))


