# skate

Fast sorted key extraction.

## Problem

Handling up to a few TB of JSON w/o classic big data tooling. Especially the
following use case:

* deriving a key from each document
* having documents sorted by that key

One use case is match candidate generation for deduplication.

## Transformation

We take jsonlines as input, with two parameters:

* a function to get the id of a document (or just the field name)
* a function to extract a key

The resulting file will be a TSV of the shape:

```
ID    KEY    DOC
```

The key will be sorted (optionally, but typical for the use case).

## Why an extra command?

We had a python program for this, which we parallelized with the great [GNU
parallel](https://www.gnu.org/software/parallel/) - however, when sharding the
input with parallel the program worked on each chunk; hence probably miss
clusters.

Otherwise we thought Go might be faster overall. Python took XXX minutes for
about a 1TB of input.

