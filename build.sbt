import AssemblyKeys._

assemblySettings

name := "stucco-rt"

organization := "gov.ornl"

version := "0.0.1"

scalaVersion := "2.10.2"

logLevel := Level.Warn

fork in run := true

scalacOptions := Seq(
  "-unchecked", "-deprecation", "-feature"
)

resolvers ++= Seq(
  "clojars" at "http://clojars.org/repo/", // for storm
  "clojure-releases" at "http://build.clojure.org/releases"
)

libraryDependencies ++= Seq(
  "org.streum" %% "configrity-yaml" % "1.0.0",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
  "junit" % "junit" % "4.10" % "test",
  "com.novocode" % "junit-interface" % "0.10-M4" % "test",
  "ch.qos.logback" % "logback-classic" % "1.0.13",
  "org.clapper" % "grizzled-slf4j_2.10" % "1.0.1",
  "net.logstash.logback" % "logstash-logback-encoder" % "1.2",
  // can't use provided dep for storm, otherwise sbt run won't work
  // "storm" % "storm" % "0.8.2" % "provided"
  "storm" % "storm" % "0.8.2" exclude("org.slf4j", "slf4j-log4j12"),
  "com.xorlev" % "storm-amqp-spout" % "0.2.0",
  "com.basho.riak" % "riak-client" % "1.1.1",
  "com.tinkerpop.blueprints" % "blueprints-core" % "2.4.0",
  "com.thinkaurelius.titan" % "titan-all" % "0.4.0"
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
