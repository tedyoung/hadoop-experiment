# This is my HADOOP experiment repository.

In addition to the standard WordCount M/R, which now cleans the words a bit, but does not yet normalize them through
"stemming", it also includes WordIndex, which currently stores a list of indexes into the input files for each word.
However, the index is currently not very useful because there are multiple input files and the indexes (the keys
coming in to the Map function) don't have any information about the file.

Next Tasks:

* Create new input format so that the index can track the file and location of each word
