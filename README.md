# Semantic CSV

[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/metasoarous/semantic-csv?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

**A Clojure library for higher level CSV parsing/processing functionality.**

The two most popular CSV parsing libraries for Clojure presently - `clojure/data.csv` and `clojure-csv` -
concern themselves only with the _syntax_ of CSV;
They take CSV text, transform it into a collection of vectors of string values, and that's it.
Semantic CSV takes the next step by giving you tools for addressing the _semantics_ of your data, helping
you put it in a form that better reflects what it represents.

See the [introductory blog post](http://www.metasoarous.com/presenting-semantic-csv/) for more information on the
design phylosophy.

## Features

* Absorb header row as a vector of column names, and return remaining rows as maps of `column-name -> row-val`
* Write from a collection of maps, given a header
* Apply casting/formatting functions by column name, while reading or writing
* Remove commented out lines (by default, those starting with `#`)
* Compatible with any CSV parsing library returning/writing a sequence of row vectors
* Transducer versions of all the main functions by requiring `semantic-csv.transducers` instead of `semantic-csv.core`
* (UNTESTED) Cljs compatibility
* (SOON) A "sniffer" that reads in N lines, and uses them to guess column types

## Structure

Semantic CSV is structured around a number of composable processing functions for transforming data as it
comes out of or goes into a CSV file.
This leaves room for you to use whatever parsing/formatting tools you like, reflecting a nice decoupling
of grammar and semantics.
However, a couple of convenience functions are also provided which wrap these individual steps
in an opinionated but customizable manner, helping you move quickly while prototyping or working at the
REPL.


## Installation

Assuming you're using Leiningen, just add the following to your dependencies:

[![Clojars Project](http://clojars.org/semantic-csv/latest-version.svg)](http://clojars.org/semantic-csv)

Note that this release is still in alpha and is subject to change.
If you would like a stable release, please use 0.1.0 (and note that 0.2.0 was an accidental release and was supposed to be alpha).
We're hoping to have a stable 0.2.1 release soon.


## Usage

Please see [metasoarous.github.io/semantic-csv](http://metasoarous.github.io/semantic-csv) for complete documentation.

Semantic CSV _emphasizes_ a number of individual processing functions which can operate on the output of a syntactic csv parser such as `clojure.data.csv` or `clojure-csv`.
This reflects a nice decoupling of grammar and semantics, in an effort to make this library as composable and interoperable as possible.

```clojure
=> (require '[clojure.java.io :as io]
            '[clojure-csv.core :as csv]
            '[semantic-csv.core :as sc])
            
=> (with-open [in-file (io/reader "test/test.csv")]
     (->>
       (csv/parse-csv in-file)
       (sc/remove-comments)
       (sc/mappify)
       (sc/cast-with {:this ->int})
       doall))

({:this 1, :that "2", :more "stuff"}
 {:this 2, :that "3", :more "other yeah"})
```

However, some opinionated, but configurable convenience functions are also provided.

```clojure
(with-open [in-file (io/reader "test/test.csv")]
  (doall
    (sc/process (parse-csv in-file))))
```
And for the truly irreverent... (who don't need _computer_ laziness):

```clojure
(sc/slurp-csv "test/test.csv")
```
### Writing CSV data

As with the input processing functions, the writer processing functions come in modular pieces you can use as you see fit.
This time let's use `clojure/data.csv`:

```clojure
(require '[semantic-csv.core :as sc]
         '[clojure.data.csv :as cd-csv])

(def data [{:this 1, :that "2", :more "stuff"}
           {:this 2, :that "3", :more "other yeah"}])

(with-open [out-file (io/writer "test.csv")]
  (->> data
       (sc/cast-with {:this #(-> % float str)})
       sc/vectorize
       (cd-csv/write-csv out-file)))
```

And again, as with the input processing functions, here we also provide a quick and dirty, opinionated convenience function for automating some of this:

```clojure
(sc/spit-csv "test2.csv"
          {:cast-fns {:this #(-> % float str)}}
          data)
```

And there you have it.


## Transducer API

There is now a transducer API available under the `semantic-csv.transducers` namespace.
This API is now (or will soon be) in public alpha.
The API is still subject to change, but we would greatly appreciate feedback.

In particular, we'd appreciate the following feedback:
 
* should transducers hook up differently to io? io is sort of a "reducible" context...
* how should the namespaces be organized? the transformer versions current live in a mirror api as the core api.

Please see [metasoarous.github.io/semantic-csv](http://metasoarous.github.io/semantic-csv#transducers) for complete documentation.


## Contributing

Feel free to submit a pull request.
If you're looking for things to help with, please take a look at the [GH issues](https://github.com/metasoarous/semantic-csv/issues) page.
Contributing to the issues with comments, feedback, or requests is also greatly appreciated.


## Contributors

For a complete list of contributors, see [the GH contributors page](https://github.com/metasoarous/semantic-csv/graphs/contributors).

Of particular note, however:

* [Mark Hinshaw (@mahinshaw)](https://github.com/mahinshaw): Mark has been a major help on many fronts, but most notably for pushing forward the transducers and cljs compatibility work.
* [Simon Belak (@sbelak)](https://github.com/sbelak): Helped make casting functionality more robust.
* [Ben Rosas (@ballPointPenguin)](https://github.com/ballPointPenguin): Ben helped set up some of the early testing.


## License

Copyright © 2014-2017 Christopher Small

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

