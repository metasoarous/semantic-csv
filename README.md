# cljsv

[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/metasoarous/semantic-csv?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A Clojure library with higher level functionality for CSV parsing.

The key features offered over `clojure.data.csv` are:

* Respecting comment lines (by default `#...`)
* Absorbing a header, and returning subsequent rows as hash-maps of `header-column -> row-val`
* Applying casting functions on a column by column basis (for casting to ints, floats, etc)

## Installation

For now, it is recommended that you use [`lein-git-deps`](https://github.com/tobyhede/lein-git-deps), since this project is still in early alpha and is likely to be changing frequently.
First, add the following to your `project.clj`:

    :plugins [[lein-git-deps "0.0.1-SNAPSHOT"]]
    :git-dependencies [["https://github.com/metasoarous/cljsv.git"]]
    :source-paths ["src/<yuourproject>", ".lein-git-deps/cljsv/src/"]

Then run `lein git-deps` and you should be good to go.

## Usage

There are two functions to work with at the moment: `read-csv-file` and `read-csv-str`.
The each support a number of options

* `:header` - bool; consume the first row as a header?
* `:comment-re` - specify a regular expression to use for commenting out lines, or something falsey if this isn't desired
* `:remove-empty` - also remove empty rows?
* `:cast-fns` - optionally cast the vals in the map by applying the corresponding function in (cast-fns row-name) to the string val"

The `read-csv-file` function will take as it's first argument either a filename string or a file handle.
Note that parsing is eager if a filename string is passed, but lazy if a handle is passed.

### Example

```clojure
(require '[clojure.java.io :as io]
         '[cljsv.core :as csv])

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

