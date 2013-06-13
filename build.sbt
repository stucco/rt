import AssemblyKeys._

assemblySettings

name := "stucco"

version := "1.0"

scalaVersion := "2.10.1"

fork in run := true

resolvers ++= Seq(
  "clojars" at "http://clojars.org/repo/", // for storm
  "clojure-releases" at "http://build.clojure.org/releases"
)

libraryDependencies ++= Seq(
  // can't use provided dep for storm, otherwise sbt run won't work
  // "storm" % "storm" % "0.8.2" % "provided"
  "storm" % "storm" % "0.8.2",
  "com.rabbitmq" % "amqp-client" % "3.1.1",
  "com.xorlev" % "storm-amqp-spout" % "0.2.0",
  "redis.clients" % "jedis" % "2.1.0",
  "com.basho.riak" % "riak-client" % "1.1.1",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
  "junit" % "junit" % "4.10" % "test",
  "com.novocode" % "junit-interface" % "0.10-M4" % "test"
)

// following two sections needed to make sbt assembly work
// (or we can use a "provided" dependency, but then sbt run won't work)

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  cp filter { jar => Seq(
    "storm-0.8.2.jar",
    "servlet-api-2.5-20081211.jar"
  ) contains jar.data.getName }
}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { old =>
  {
    case "project.clj" => MergeStrategy.discard
    case x => old(x)
  }
}
