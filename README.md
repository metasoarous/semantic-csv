# Semantic CSV

[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/metasoarous/semantic-csv?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A Clojure library with higher level CSV parsing functionality.

The most popular CSV parsing libraries for Clojure presently -- `clojure.data.csv` and `clojure-csv` -- are really focused on handling the _syntax_ of CSV;
They take CSV text and transform it into collections of row vectors of string values, providing a minimal translation into the world of data.
Semantic CSV takes it the next step by giving you tools for addressing the _semantics_ of your data, helping you put it into the form that better reflects what it means, and what's most useful for you.

## Features

To be less abstract about it, `semantic-csv` lets you easily:

* Absorb header row as a vector of column names, and return remaining rows as maps of `column-name -> row-val`
* Write from a collection of maps, given a pre-specified `:header`
* When reading, apply casting functions on a column by column basis (for casting to ints, floats, etc) via `:cast-fns`
* When writing, apply formatting functions on a column by column basis via `:format-fns`, when `str` won't cut it
* Remove lines starting with comment characters (by default `#`)
* An optional "sniffer" that reads in N lines, and uses them to guess column types (SOON)

## Installation

For now, it is recommended you use [`lein-git-deps`](https://github.com/tobyhede/lein-git-deps), since this project is still in early alpha and is likely to be changing frequently.
First, add the following to your `project.clj`:

    :plugins [[lein-git-deps "0.0.1-SNAPSHOT"]]
    :git-dependencies [["https://github.com/metasoarous/semantic-csv.git"]]
    :source-paths ["src/<yuourproject>", ".lein-git-deps/semantic-csv/src/"]

Then run `lein git-deps` and you should be good to go.

## Usage

Please see [metasoarous.github.io/semantic-csv](http://metasoarous.github.io/semantic-csv) for complete documentation.

**Note: This will be evolving rapidly! In particular, I'll be decomposing the API into individual functions which give you each of the features separately, but probably still offer a function that does all the magic for you.**

The _emphasized_ usage of `semantic-csv` involves using individual processing functions on the output of a grammatical csv parser such as `clojure.data.csv` or `clojure-csv`.
This reflects a nice decoupling of grammar and semantics.

```clojure
=> (require '[clojure.java.io :as io]
            '[clojure-csv :as csv]
            '[semantic-csv :as sc])
=> (with-open [in-file (io/reader "test/test.csv")]
     (doall
       (->>
         (csv/parse-csv in-file)
         sc/remove-comments
         sc/mappify
         (sc/cast-cols {:this sc/->int}))))

({:this 1, :that "2", :more "stuff"}
 {:this 2, :that "3", :more "other yeah"})
```

However, while this nice decoupled API is emphasized, we provide some opinionated, but configurable convenience functions for automating some of this.

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

