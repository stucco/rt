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

A scala REPL (with all dependencies on the classpath) can be brought up using `sbt console`.

For storm, a .jar file can be built using `sbt assembly`. The .jar file will include all the required dependencies (e.g. `storm`). The .jar file will be located in `target/scala_X.X.X/projectname-assembly-X.X.X.jar`.
