# mruby-rocksdb

[![Build Status](https://travis-ci.org/syucream/mruby-rocksdb.svg?branch=master)](https://travis-ci.org/syucream/mruby-rocksdb)

mruby binding of [facebook/rocksdb](https://github.com/facebook/rocksdb)

# Implemented methods

* [DBM](http://ruby-doc.org/stdlib-2.3.3/libdoc/dbm/rdoc/DBM.html) like methods:


| method      | implemented?       |
|:------------|--------------------|
| []          | :heavy_check_mark: |
| []=         | :heavy_check_mark: |
| clear       | :heavy_check_mark: |
| close       | :heavy_check_mark: |
| closed?     | :heavy_check_mark: |
| delete      | :heavy_check_mark: |
| delete_if   | :heavy_check_mark: |
| reject!     | :heavy_check_mark: |
| each        | :heavy_check_mark: |
| each_pair   | :heavy_check_mark: |
| each_key    | :heavy_check_mark: |
| each_value  | :heavy_check_mark: |
| empty?      | :heavy_check_mark: |
| fetch       | :heavy_check_mark: |
| has_key?    | :heavy_check_mark: |
| include?    | :heavy_check_mark: |
| key?        | :heavy_check_mark: |
| member?     | :heavy_check_mark: |
| has_value?  | :heavy_check_mark: |
| value?      | :heavy_check_mark: |
| invert      | :heavy_check_mark: |
| key         | :heavy_check_mark: |
| keys        | :heavy_check_mark: |
| length      | :heavy_check_mark: |
| size        | :heavy_check_mark: |
| reject      | :heavy_check_mark: |
| replace     | :heavy_check_mark: |
| select      | :heavy_check_mark: |
| shift       | :heavy_check_mark: |
| store       | :heavy_check_mark: |
| to_a        | :heavy_check_mark: |
| to_hash     | :heavy_check_mark: |
| update      | :heavy_check_mark: |
| values      | :heavy_check_mark: |
| values_at   | :heavy_check_mark: |


# License

MITL
