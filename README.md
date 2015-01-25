# Semantic CSV

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

**Note: This will be evolving rapidly! In particular, I'll be decomposing the API into individual functions which give you each of the features separately, but probably still offer a function that does all the magic for you.**

There are two functions to work with at the moment: `read-csv-file` and `read-csv-str`.
They each support a number of options

* `:header` - bool; consume the first row as a header?
* `:comment-re` - specify a regular expression to use for commenting out lines, or something falsey if this isn't desired
* `:remove-empty` - also remove empty rows?
* `:cast-fns` - optionally cast the vals in the map by applying the corresponding function in (cast-fns row-name) to the string val"

The `read-csv-file` function will take as it's first argument either a filename string or a file handle.
Note that parsing is eager if a filename string is passed, but lazy if a handle is passed.

### Example

```clojure
(require '[clojure.java.io :as io]
         '[semantic-csv.core :as csv])

; Simple example
(with-open [f (io/reader "test/test.csv")]
  (doall
    (for [row (csv/read-csv-file f)]
      (:more row)))) ; uh... (get row "more") actually... though, I'll be fixing soon
:;=> ("stuff", "other yeah")
```

And demoing `cast-fns`:

```clojure
(with-open [f (io/reader "test/test.csv")]
  (doall
    (for [row (csv/read-csv-file f :cast-fns {:this csv/->int})]
      (:this row))))
:;=> (1, 2)
```

## License

Copyright © 2014 Christopher Small

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

