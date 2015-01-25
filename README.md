# Semantic CSV

[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/metasoarous/semantic-csv?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A Clojure library with higher level CSV parsing functionality.

The two most popular CSV parsing libraries for Clojure presently - `clojure.data.csv` and `clojure-csv` - concern themselves only wtih the _syntax_ of CSV;
They take CSV text, transform it into a collection of vectors of string values, and nothing more.
Semantic CSV takes the next step by giving you tools for addressing the _semantics_ of your data, helping you put it into the form that better reflects what it means, and what's most useful for you.

## Features

* Absorb header row as a vector of column names, and return remaining rows as maps of `column-name -> row-val`
* Write from a collection of maps, given a header
* When reading, apply casting functions by column name
* When writing, apply formatting functions by column name
* Remove lines starting with comment characters (by default `#`)
* Fully compatible with any CSV parsing library that retruning/writing a sequence of row vectors
* (SOON) A "sniffer" that reads in N lines, and uses them to guess column types

## Structure

This is in the spirit of making the API as composable and interoperable as possible.
However, as a convenience, we offer a few functions which wrap these individual steps with a set of opinionated defaults and an option map for overriding the default behaviour.


## Installation

For now, it is recommended you use [`lein-git-deps`](https://github.com/tobyhede/lein-git-deps), since this project is still in early alpha and is likely to be changing frequently.
First, add the following to your `project.clj`:

    :plugins [[lein-git-deps "0.0.1-SNAPSHOT"]]
    :git-dependencies [["https://github.com/metasoarous/semantic-csv.git"]]
    :source-paths ["src/<yuourproject>", ".lein-git-deps/semantic-csv/src/"]

Then run `lein git-deps` and you should be good to go.

## Usage

Please see [metasoarous.github.io/semantic-csv](http://metasoarous.github.io/semantic-csv) for complete documentation.

Semantic CSV _emphasizes_ a number of individual processing functions which can operate on the output of a syntactic csv parser such as `clojure.data.csv` or `clojure-csv`.
This reflects a nice decoupling of grammar and semantics, making this library as composable and interoperable as possible.

```clojure
=> (require '[clojure.java.io :as io]
            '[clojure-csv :as csv]
            '[semantic-csv :as sc])
=> (with-open [in-file (io/reader "test/test.csv")]
     (->>
       (csv/parse-csv in-file)
       sc/remove-comments
       sc/mappify
       (sc/cast-cols {:this sc/->int})
       doall)))

({:this 1, :that "2", :more "stuff"}
 {:this 2, :that "3", :more "other yeah"})
```

However, some opinionated, but configurable convenience functions are also provided.

```clojure
(with-open [in-file (io/reader "test/test.csv")]
  (doall
    (process (csv/parse-csv in-file))))
```
And for the truly irreverant... (who don't need _computer_ laziness):

```clojure
(slurp-and-process "test/test.csv")
```
### Writing CSV data

As with the reader processing functions, the writer processing functions come in modular peices you can use as you see fit.

**TODO: WIP**

## License

Copyright © 2014 Christopher Small

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

