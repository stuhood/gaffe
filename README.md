gaffe
========

gaffe may eventually become a real graph database, but since it is scratching an itch caused
by curiosity, rather than necessity, it may never go anywhere.

So far, the most interesting portion is the planned persistence model inspired by [log
structured merge trees](http://nosqlsummer.org/paper/lsm-tree) and [CouchDB's append only
B-Tree](http://jchrisa.net/drl/_design/sofa/_show/post/CouchDB-Implements-a-Fundamental-Algorithm),
but with heuristics to layout optimize the inherent indexes embedded in graphs.

Alright, enough buzzwords.

Building
--------

Use [sbt](http://code.google.com/p/simple-build-tool/) and locally published (for Scala 2.8) copies of

- [avro-sbt](http://github.com/codahale/avro-sbt)
- [configgy](http://github.com/robey/configgy)

...to build gaffe.

