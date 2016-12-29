(ns semantic-csv.core-test
  (:require #?(:clj [clojure.test :refer :all]
               :cljs [cljs.test :refer-macros [deftest is testing run-tests]])
            [semantic-csv.core :as scsv]))


(deftest mappify-test
  (let [data [["this" "that"]
              ["x" "y"]
              ["# some comment"]]]
    (testing "mappify should work"
      (is (= (first (scsv/mappify data))
             {:this "x" :that "y"})))
    (testing "mappify should let you avoid keyifying"
      (is (= (first (scsv/mappify {:keyify false} data))
             {"this" "x" "that" "y"})))
    (testing "mappify should not regard comments"
      (is (= (last (scsv/mappify data))
             {:this "# some comment"})))
    (testing "mappify should not consume header if :header is specified"
      (is (= (first (scsv/mappify {:header ["foo" "bar"]} data))
             {:foo "this" :bar "that"})))
    (testing ":tranform-header overrides the :keyify option"
      (is (= (first (scsv/mappify {:transform-header identity :keyify true} data))
             {"this" "x" "that" "y"})))))


(deftest structify-test
  (let [data [["this" "that"]
              ["x" "y"]
              ["z" "w"]]]
    (testing "should return structs"
      (is (= (->> data
                  (scsv/mappify {:structs true})
                  first
                  type)
             clojure.lang.PersistentStructMap)))
    (testing "should have correct structure"
      (is (= (->> data
                  (scsv/mappify {:structs true}))
             [{:this "x" :that "y"} {:this "z" :that "w"}])))))


(deftest remove-comments-test
  (let [data [["# a comment"]
              ["// another comment"]]]
    (testing "remove-comments should remove #-designated comments by default"
      (is (= (scsv/remove-comments data)
             [["// another comment"]])))
    (testing "remove-comments should work with comment-re"
      (is (= (scsv/remove-comments {:comment-re #"^//"} data)
             [["# a comment"]])))
    (testing "remove-comments should work with comment-char"
      (is (= (scsv/remove-comments {:comment-char \$} [["$this is a comment"] ["this" "is data"]])
             [["this" "is data"]])))))


(deftest cast-with-test
  (let [data [["this" "that"]
              ["1" "y"]]]
    (testing "should work with mappify"
      (is (= (->> data
                  scsv/mappify
                  (scsv/cast-with {:this scsv/->int})
                  first)
             {:this 1 :that "y"}))))
  (let [data [["1" "this"]
              ["2" "that"]]]
    (testing "should work without mappify"
      (is (= (->> data
                  (scsv/cast-with {0 scsv/->int})
                  second)
             [2 "that"])))
    (testing "should work with :except-first"
      (is (= (->> data
                  (scsv/cast-with {0 scsv/->int} {:except-first true}))
             [["1" "this"] [2 "that"]]))))
  (testing "error handling"
    (let [data [{:this "3" :that "45x"}
                {:this "6" :that "78"}]]
      (testing "should use error-handler when specified for errors"
        (is (= (->> data
                    (scsv/cast-with {:that scsv/->int} {:exception-handler (fn [_ _] "stuff")})
                    (first)
                    :that)
               "stuff")))
      (testing "should leave things alone without errors"
        (is (= (->> data
                    (scsv/cast-with {:that scsv/->int} {:exception-handler (fn [_ _] "stuff")})
                    (second)
                    :that)
               78))))))


(deftest cast-all-test
  (let [data [["this" "that"]
              ["1" "2"]]]
    (testing "should work with mappify"
      (is (= (->> data
                  scsv/mappify
                  (scsv/cast-with scsv/->int)
                  first)
             {:this 1 :that 2})))
    (testing "should work with :except-first"
      (is (= (->> data
                  (scsv/cast-with scsv/->int {:except-first true}))
             [["this" "that"] [1 2]])))
    (testing "should work with :only <seq>"
      (is (= (->> data
                  scsv/mappify
                  (scsv/cast-with scsv/->int {:only [:that]})
                  first)
             {:this "1" :that 2})))
    (testing "should work with :only <colname>"
      (is (= (->> data
                  scsv/mappify
                  (scsv/cast-with scsv/->int {:only :that})
                  first)
             {:this "1" :that 2})))))


(deftest casting-function-helpers-test
  (testing "with string inputs"
    (let [n "3443"
          x "4.555"
          trues ["yes" "true" "t" "True"]
          falses ["no" "false" "f" "FALSE"]]
      (is (= (scsv/->int n) 3443))
      (is (= (scsv/->long n) 3443))
      (is (= (scsv/->float x) (float 4.555)))
      (is (= (scsv/->double x) 4.555))
      (is (every? true? (map scsv/->boolean trues)))
      (is (every? false? (map scsv/->boolean falses)))))
  (testing "with numeric inputs"
    (doseq [x [3443 4.555]]
      (is (= (scsv/->double x) (double x)))
      (is (= (scsv/->float x) (float x)))
      (is (false? (scsv/->boolean 0)))
      (is (true? (scsv/->boolean 1))))
    (doseq [x [3443 4.555]
            f [scsv/->int scsv/->long]]
      (is (= (f x) (long x)))))
  (testing "with nil-fill"
    (is (= (scsv/->double {:nil-fill 0.0} "") 0.0)))
  (testing "with string inputs containing spaces on the ends"
    (doseq [f [scsv/->int scsv/->long scsv/->float scsv/->double]]
      (is (f " 3454 "))))
  (testing "with non integer values getting cast to integers"
    (doseq [f [scsv/->int scsv/->long]]
      (is (f "35.54")))))


(deftest process-test
  (testing "should generally work"
    (let [parsed-data [["this" "that" "more"]
                       ["a" "b" "c"]
                       ["d" "e" "f"]]]
      (testing "with defaults"
        (is (= (first (scsv/process parsed-data))
               {:this "a" :that "b" :more "c"}))
        (is (= (second (scsv/process parsed-data))
               {:this "d" :that "e" :more "f"})))
      (testing "with :mappify false"
        (is (= (first (scsv/process {:mappify false} parsed-data))
               ["this" "that" "more"]))
        (is (= (second (scsv/process {:mappify false} parsed-data))
               ["a" "b" "c"])))
      (testing "with :header spec"
        (is (= (first (scsv/process {:header ["x" "y" "z"]} parsed-data))
               {:x "this" :y "that" :z "more"})))
      (testing "with :structs should give structs"
        (is (= (->> parsed-data (scsv/process {:structs true}) first type)
               clojure.lang.PersistentStructMap)))
      (testing "with defaults"
        (is (= (first (scsv/process {:cast-fns {:this #(str % "andstuff")}}
                               parsed-data))
               {:this "aandstuff" :that "b" :more "c"}))
        (is (= (second (scsv/process {:cast-fns {:this #(str % "andstuff")}}
                                parsed-data))
               {:this "dandstuff" :that "e" :more "f"}))))))


(deftest except-first-test
  (let [parsed-data [["this" "that" "more"]
                     ["a" "b" "c"]
                     ["d" "e" "f"]]]
    (testing "Should leave first row unchanged"
      (is (= (->> parsed-data
                  (scsv/except-first (scsv/cast-with {0 (partial str "X")}))
                  (first))
             ["this" "that" "more"])))
    (testing "Should generally work on remaining rows"
      (is (= (->> parsed-data
                  (scsv/except-first (scsv/cast-with {0 (partial str "X")})
                                     (remove #(= (first %) "Xa")))
                  (second))
             ["Xd" "e" "f"])))))


(deftest vectorize-test
  (let [data [{:this "a" :that "b"}
              {:this "x" :that "y"}]]
    (testing "with no opts"
      (is (= (scsv/vectorize data)
             '(["this" "that"]
               ["a" "b"]
               ["x" "y"]))))
    (testing "with header opts"
      (is (= (scsv/vectorize {:header [:that :this]} data)
             '(["that" "this"]
               ["b" "a"]
               ["y" "x"]))))
    (testing "with :prepend-header false"
      (is (= (scsv/vectorize {:prepend-header false} data)
             '(["a" "b"]
               ["x" "y"]))))))


(deftest batch-test
  (let [xs (for [i (range 20)] [i (* i 2)])]
    (testing "makes the right number of batches"
      (is (= (count (scsv/batch 7 xs))
             3)))
    (testing "doesn't put more things than it should in final batch"
      (is (= (->> xs (scsv/batch 7) last count)
             6)))))


