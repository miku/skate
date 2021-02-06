# skate

Fast sorted key extraction and extra utilities.

## Problem

Handling a TB of JSON without big data tooling. Especially the following use
case:

* deriving a key from a document
* sort documents by (that) key

One use case is match candidate generation for deduplication.

## Transformation

We take jsonlines as input and extract id and derive the key. The resulting
file will be a TSV of the shape:

```
ID    KEY    DOC
```

The key will be sorted (optionally, but typical for the use case).

## Why an extra command?

We had a python program for this, which we parallelized with the great [GNU
parallel](https://www.gnu.org/software/parallel/) - however, when sharding the
input with parallel the program worked on each chunk; hence probably miss
clusters (not a problem of parallel, but our code, but still).

## Usage

```
$ skate-sorted-keys < release_entities.jsonl | skate-cluster > cluster.jsonl
```

A few options:

```
$ skate-sorted-keys -h
Usage of skate-sorted-keys:
  -S    skip sorting
  -b int
        batch size (default 50000)
  -compress-program string
        compress program, passed to sort (default "zstd")
  -f string
        key function name (default "tsand")
  -verbose
        show progress
  -w int
        number of workers (default 8)
```
