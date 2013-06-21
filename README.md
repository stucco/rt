stucco-rt
==========

basic sbt usage
---------------
The project can be compiled using `sbt compile`.

The project can be run using `sbt run`.

Unit tests can be run using `sbt test`.

A scala REPL (with all dependencies on the classpath) can be brought up using `sbt console`.

For storm, a .jar file can be built using `sbt assembly`. The .jar file will include all the required dependencies (e.g. `storm`). The .jar file will be located in `target/scala_X.X.X/projectname-assembly-X.X.X.jar`.
