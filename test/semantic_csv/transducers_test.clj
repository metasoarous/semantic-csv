(ns semantic-csv.transducers-test
  (:require [semantic-csv.transducers :refer :all]
            [clojure-csv.core :as csv]
            [clojure.test :refer :all]))


(deftest mappify-test
  (let [data [["this" "that"]
              ["x" "y"]
              ["# some comment"]]]
    (testing "mappify should work"
      (is (= (first (into [] (mappify) data))
             {:this "x" :that "y"})))
    (testing "mappify should let you avoid keyifying"
      (is (= (first (into [] (mappify {:keyify false}) data))
             {"this" "x" "that" "y"})))
    (testing "mappify should not regard comments"
      (is (= (last (into [] (mappify) data))
             {:this "# some comment"})))
    (testing "mappify should not consume header if :header is specified"
      (is (= (first (into [] (mappify {:header ["foo" "bar"]}) data))
             {:foo "this" :bar "that"})))))


(deftest structify-test
  (let [data [["this" "that"]
              ["x" "y"]
              ["z" "w"]]]
    (testing "should return structs"
      (is (= (->> (sequence (structify) data)
                  first
                  type)
             clojure.lang.PersistentStructMap)))
    (testing "should have correct structure"
      (is (= (into [] (structify) data)
             [{:this "x" :that "y"} {:this "z" :that "w"}])))))


(deftest remove-comments-test
  (let [data [["# a comment"]
              ["// another comment"]]]
    (testing "remove-comments should remove #-designated comments by default"
      (is (= (into [] (remove-comments) data)
             [["// another comment"]])))
    (testing "remove-comments should work with comment-re"
      (is (= (into [] (remove-comments {:comment-re #"^//"}) data)
             [["# a comment"]])))
    (testing "remove-comments should work with comment-char"
      (is (= (into [] (remove-comments {:comment-char \$}) [["$this is a comment"] ["this" "is data"]])
             [["this" "is data"]])))))


(deftest cast-with-test
  (let [data [["this" "that"]
              ["1" "y"]]]
    (testing "should work with mappify"
      (is (= (first (sequence (comp (mappify) (cast-with {:this ->int})) data))
             {:this 1 :that "y"}))))
  (let [data [["1" "this"]
              ["2" "that"]]]
    (testing "should work without mappify"
      (is (= (second (sequence (cast-with {0 ->int}) data))
             [2 "that"])))
    (testing "should work with :except-first"
      (is (= (into [] (cast-with {0 ->int} {:except-first true}) data)
             [["1" "this"] [2 "that"]]))))
  (testing "error handling"
    (let [data [{:this "3" :that "45x"}
                {:this "6" :that "78"}]]
      (testing "should use error-handler when specified for errors"
        (is (= (->  (sequence
                       (cast-with {:that ->int} {:exception-handler (fn [_ _] "stuff")}) data)
                    first
                    :that)
               "stuff")))
      (testing "should leave things alone without errors"
        (is (= (->> data
                    (sequence (cast-with {:that ->int} {:exception-handler (fn [_ _] "stuff")}))
                    (second)
                    :that)
               78))))))


(deftest cast-all-test
  (let [data [["this" "that"]
              ["1" "2"]]]
    (testing "should work with mappify"
      (is (= (->> data
                  (sequence (comp (mappify) (cast-with ->int)))
                  first)
             {:this 1 :that 2})))
    (testing "should work with :except-first"
      (is (= (->> data
                  (sequence (cast-with ->int {:except-first true})))
             [["this" "that"] [1 2]])))
    (testing "should work with :only <seq>"
      (is (= (->> data
                  (sequence (comp (mappify)
                                  (cast-with ->int {:only [:that]})))
                  first)
             {:this "1" :that 2})))
    (testing "should work with :only <colname>"
      (is (= (->> data
                  (sequence (comp (mappify)
                                  (cast-with ->int {:only :that})))
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
      (is (= (f x) (long x)))))
  (testing "with string inputs containing spaces on the ends"
    (doseq [f [->int ->long ->float ->double]]
      (is (f " 3454 "))))
  (testing "with non integer values getting cast to integers"
    (doseq [f [->int ->long]]
      (is (f "35.54")))))


(deftest process-test
  (testing "should generally work"
    (let [parsed-data [["this" "that" "more"]
                       ["a" "b" "c"]
                       ["d" "e" "f"]]]
      (testing "with defaults"
        (is (= (first (sequence (process) parsed-data))
               {:this "a" :that "b" :more "c"}))
        (is (= (second (sequence (process) parsed-data))
               {:this "d" :that "e" :more "f"})))
      (testing "with :mappify false"
        (is (= (first (sequence
                       (process {:mappify false})
                       parsed-data))
               ["this" "that" "more"]))
        (is (= (second (sequence
                        (process {:mappify false})
                        parsed-data))
               ["a" "b" "c"])))
      (testing "with :header spec"
        (is (= (first (sequence
                       (process {:header ["x" "y" "z"]})
                       parsed-data))
               {:x "this" :y "that" :z "more"})))
      (testing "with :structs should give structs"
        (is (= (->> parsed-data (sequence (process {:structs true})) first type)
               clojure.lang.PersistentStructMap)))
      (testing "with defaults"
        (is (= (first (sequence
                       (process {:cast-fns {:this #(str % "andstuff")}})
                       parsed-data))
               {:this "aandstuff" :that "b" :more "c"}))
        (is (= (second (sequence
                        (process {:cast-fns {:this #(str % "andstuff")}})
                        parsed-data))
               {:this "dandstuff" :that "e" :more "f"}))))))


(deftest vectorize-test
  (let [data [{:this "a" :that "b"}
              {:this "x" :that "y"}]]
    (testing "with no opts"
      (is (= (into [] (vectorize) data)
             '(["this" "that"]
               ["a" "b"]
               ["x" "y"]))))
    (testing "with header opts"
      (is (= (sequence (vectorize {:header [:that :this]}) data)
             '(["that" "this"]
               ["b" "a"]
               ["y" "x"]))))
    (testing "with :prepend-header false"
      (is (= (transduce (vectorize {:prepend-header false}) conj [] data)
             '(["a" "b"]
               ["x" "y"]))))))


(deftest batch-test
  (let [xs (for [i (range 20)] [i (* i 2)])]
    (testing "makes the right number of batches"
      (is (= (count (batch 7 xs))
             3)))
    (testing "doesn't put more things than it should in final batch"
      (is (= (->> xs (batch 7) last count)
             6)))))
