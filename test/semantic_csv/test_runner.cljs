(ns semantic-csv.test-runner
  (:require [doo.runner :refer-macros [doo-tests]]
            [semantic-csv.core-test]
            [semantic-csv.transducers-test]))


(doo-tests 'semantic-csv.core-test
           'semantic-csv.transducers-test)


