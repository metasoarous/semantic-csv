(ns semantic-csv.transducers-test
  (:require [semantic-csv.transducers :as xfcsv]
            ;#?(:clj [clojure-csv.core :as csv])
            #?(:clj [clojure.test :refer :all]
               :cljs [cljs.test :refer-macros [is testing deftest]])))


(deftest mappify-test
  (let [data [["this" "that"]
              ["x" "y"]
              ["# some comment"]]]
    (testing "mappify should work"
      (is (= (first (into [] (xfcsv/mappify) data))
             {:this "x" :that "y"})))
    (testing "mappify should let you avoid keyifying"
      (is (= (first (into [] (xfcsv/mappify {:keyify false}) data))
             {"this" "x" "that" "y"})))
    (testing "mappify should not regard comments"
      (is (= (last (into [] (xfcsv/mappify) data))
             {:this "# some comment"})))
    (testing "mappify should not consume header if :header is specified"
      (is (= (first (into [] (xfcsv/mappify {:header ["foo" "bar"]}) data))
             {:foo "this" :bar "that"})))))


(deftest structify-test
  (let [data [["this" "that"]
              ["x" "y"]
              ["z" "w"]]]
    (testing "should return structs"
      (is (= (->> (sequence (xfcsv/structify) data)
                  first
                  type)
             clojure.lang.PersistentStructMap)))
    (testing "should have correct structure"
      (is (= (into [] (xfcsv/structify) data)
             [{:this "x" :that "y"} {:this "z" :that "w"}])))))


(deftest remove-comments-test
  (let [data [["# a comment"]
              ["// another comment"]]]
    (testing "remove-comments should remove #-designated comments by default"
      (is (= (into [] (xfcsv/remove-comments) data)
             [["// another comment"]])))
    (testing "remove-comments should work with comment-re"
      (is (= (into [] (xfcsv/remove-comments {:comment-re #"^//"}) data)
             [["# a comment"]])))
    (testing "remove-comments should work with comment-char"
      (is (= (into [] (xfcsv/remove-comments {:comment-char \$}) [["$this is a comment"] ["this" "is data"]])
             [["this" "is data"]])))))


(deftest cast-with-test
  (let [data [["this" "that"]
              ["1" "y"]]]
    (testing "should work with mappify"
      (is (= (first (sequence (comp (xfcsv/mappify) (xfcsv/cast-with {:this xfcsv/->int})) data))
             {:this 1 :that "y"}))))
  (let [data [["1" "this"]
              ["2" "that"]]]
    (testing "should work without mappify"
      (is (= (second (sequence (xfcsv/cast-with {0 xfcsv/->int}) data))
             [2 "that"])))
    (testing "should work with :except-first"
      (is (= (into [] (xfcsv/cast-with {0 xfcsv/->int} {:except-first true}) data)
             [["1" "this"] [2 "that"]]))))
  (testing "error handling"
    (let [data [{:this "3" :that "45x"}
                {:this "6" :that "78"}]]
      (testing "should use error-handler when specified for errors"
        (is (= (->  (sequence
                       (xfcsv/cast-with {:that xfcsv/->int} {:exception-handler (fn [_ _] "stuff")}) data)
                    first
                    :that)
               "stuff")))
      (testing "should leave things alone without errors"
        (is (= (->> data
                    (sequence (xfcsv/cast-with {:that xfcsv/->int} {:exception-handler (fn [_ _] "stuff")}))
                    (second)
                    :that)
               78))))))


(deftest cast-all-test
  (let [data [["this" "that"]
              ["1" "2"]]]
    (testing "should work with mappify"
      (is (= (->> data
                  (sequence (comp (xfcsv/mappify) (xfcsv/cast-with xfcsv/->int)))
                  first)
             {:this 1 :that 2})))
    (testing "should work with :except-first"
      (is (= (->> data
                  (sequence (xfcsv/cast-with xfcsv/->int {:except-first true})))
             [["this" "that"] [1 2]])))
    (testing "should work with :only <seq>"
      (is (= (->> data
                  (sequence (comp (xfcsv/mappify)
                                  (xfcsv/cast-with xfcsv/->int {:only [:that]})))
                  first)
             {:this "1" :that 2})))
    (testing "should work with :only <colname>"
      (is (= (->> data
                  (sequence (comp (xfcsv/mappify)
                                  (xfcsv/cast-with xfcsv/->int {:only :that})))
                  first)
             {:this "1" :that 2})))))


(deftest casting-function-helpers-test
  (testing "with string inputs"
    (let [n "3443"
          x "4.555"]
      (is (= (xfcsv/->int n) 3443))
      (is (= (xfcsv/->long n) 3443))
      (is (= (xfcsv/->float x) (float 4.555)))
      (is (= (xfcsv/->double x) 4.555))))
  (testing "with numeric inputs"
    (doseq [x [3443 4.555]]
      (is (= (xfcsv/->double x) (double x)))
      (is (= (xfcsv/->float x) (float x))))
    (doseq [x [3443 4.555]
            f [xfcsv/->int xfcsv/->long]]
      (is (= (f x) (long x)))))
  (testing "with string inputs containing spaces on the ends"
    (doseq [f [xfcsv/->int xfcsv/->long xfcsv/->float xfcsv/->double]]
      (is (f " 3454 "))))
  (testing "with non integer values getting cast to integers"
    (doseq [f [xfcsv/->int xfcsv/->long]]
      (is (f "35.54")))))


(deftest process-test
  (testing "should generally work"
    (let [parsed-data [["this" "that" "more"]
                       ["a" "b" "c"]
                       ["d" "e" "f"]]]
      (testing "with defaults"
        (is (= (first (sequence (xfcsv/process) parsed-data))
               {:this "a" :that "b" :more "c"}))
        (is (= (second (sequence (xfcsv/process) parsed-data))
               {:this "d" :that "e" :more "f"})))
      (testing "with :mappify false"
        (is (= (first (sequence
                       (xfcsv/process {:mappify false})
                       parsed-data))
               ["this" "that" "more"]))
        (is (= (second (sequence
                        (xfcsv/process {:mappify false})
                        parsed-data))
               ["a" "b" "c"])))
      (testing "with :header spec"
        (is (= (first (sequence
                       (xfcsv/process {:header ["x" "y" "z"]})
                       parsed-data))
               {:x "this" :y "that" :z "more"})))
      (testing "with :structs should give structs"
        (is (= (->> parsed-data (sequence (xfcsv/process {:structs true})) first type)
               clojure.lang.PersistentStructMap)))
      (testing "with defaults"
        (is (= (first (sequence
                       (xfcsv/process {:cast-fns {:this #(str % "andstuff")}})
                       parsed-data))
               {:this "aandstuff" :that "b" :more "c"}))
        (is (= (second (sequence
                        (xfcsv/process {:cast-fns {:this #(str % "andstuff")}})
                        parsed-data))
               {:this "dandstuff" :that "e" :more "f"}))))))


(deftest vectorize-test
  (let [data [{:this "a" :that "b"}
              {:this "x" :that "y"}]]
    (testing "with no opts"
      (is (= (into [] (xfcsv/vectorize) data)
             '(["this" "that"]
               ["a" "b"]
               ["x" "y"]))))
    (testing "with header opts"
      (is (= (sequence (xfcsv/vectorize {:header [:that :this]}) data)
             '(["that" "this"]
               ["b" "a"]
               ["y" "x"]))))
    (testing "with :prepend-header false"
      (is (= (transduce (xfcsv/vectorize {:prepend-header false}) conj [] data)
             '(["a" "b"]
               ["x" "y"]))))))


(deftest batch-test
  (let [xs (for [i (range 20)] [i (* i 2)])]
    (testing "makes the right number of batches"
      (is (= (count (sequence (xfcsv/batch 7) xs))
             3)))
    (testing "doesn't put more things than it should in final batch"
      (is (= (->> xs (sequence (xfcsv/batch 7)) last count)
             6)))))
