stucco-rt
==========

usage
-----
All commands should be run from within the Vagrant VM that is configured from the `dev-setup` repo.

basic sbt usage
---------------
All sbt commands need to be run from the project root directory. Prepend a tilde to any sbt command to run continuously, e.g. `'sbt ~test'` will run unit tests when any file changes.

The project can be compiled using `sbt compile`.

The project can be run using `sbt run`.

Unit tests can be run using `sbt test`.

Documentation can be generated using `sbt doc`. It will be located in `target/scala-2.10/api`.

A scala REPL (with all dependencies on the classpath) can be brought up using `sbt console`.

For storm, a .jar file can be built using `sbt assembly`. The .jar file will include all the required dependencies (e.g. `storm`). The .jar file will be located in `target/scala_X.X.X/projectname-assembly-X.X.X.jar`.

scala resources
---------------
[A Tour of Scala (+ FAQ)](http://docs.scala-lang.org/tutorials/)
[Twitter Scala School](http://twitter.github.io/scala_school/)
[Coursera Scala Course](https://www.coursera.org/course/progfun)

style guide
-----------
[Scala Style Guide](http://docs.scala-lang.org/style/)

scaladoc resources
------------------
[Writing Scaladoc](https://wiki.scala-lang.org/display/SW/Writing+Documentation)
[Scaladoc Wiki](https://wiki.scala-lang.org/display/SW/Scaladoc)
[Scaladoc Usage](http://dcsobral.blogspot.com/2011/12/using-scala-api-documentation.html)

logstash configuration
----------------------
A basic configuration for logstash is included in logstash.conf. The `dev-setup` install automatically gets this config file (from the `stucco/rt` github repo) and installs it in the VM in `/etc/logstash.conf`. To change the configuration, just edit this file. There is also an upstart script responsible for starting logstash, located in `/etc/init/logstash-indexer.conf`. If necessary, this file can be edited as well.
