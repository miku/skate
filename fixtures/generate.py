#!/usr/bin/env python

with open("a.tsv", "w") as f:
    for i in range(100000):
        print("x\t{}\tx".format(i), file=f)

with open("b.tsv", "w") as f:
    for i in range(100000):
        print("x\t{}\tx".format(i), file=f)

with open("u.tsv", "w") as f:
    for _ in range(10):
        print("x\tk1\tx", file=f)
    for _ in range(10):
        print("x\tk2\tx", file=f)

with open("v.tsv", "w") as f:
    for _ in range(10):
        print("x\tk1\tx", file=f)
    for _ in range(10):
        print("x\tk2\tx", file=f)
