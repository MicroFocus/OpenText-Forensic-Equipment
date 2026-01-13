# OpenText Tree Hashing for forensic images

This repository contains the full tree hashing specification in use by OpenText
Forensic Equipment (formerly Tableau) and OpenText Forensic (formerly EnCase).

All software and examples in this repository are covered by the MIT license and
are free to use.

The tree hashing methodology and specification are Patent(s) pending. In order
to promote the proliferation and advancement of the technology, Open Text hereby
grants a perpetual license to the tree hashing ideas and know-how expressed in
this repository.

## Specification
The tree hashing implementation is described in
[OpenText-Tree-Hashing.pdf](OpenText-Tree-Hashing.pdf).

The [Example](example) directory contains a python script that can generate the
tree hash of a file. It has options to emit all of the intermediate CV values
along with the final root hash. This script was used to generate the example in
the specification, this can be replicated with the following comand:
```
python ot-tree-hashing.py --sha1 --fng-tree --block-size-exponent 2 --output - example-input.bin
```

## Sample evidence files
The [Samples](samples) directory contains sample E01/Ex01 images created using
tree hashing. For each file format it should have an evidence file for each
supported block size, and every combination of supported hashes. It also
includes one larger sample containing multiple sector table links. The expected
final root hash and all intermediate CV's are embedded in the image files.
