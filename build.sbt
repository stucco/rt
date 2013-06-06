import AssemblyKeys._

assemblySettings

name := "storm-base"

version := "1.0"

scalaVersion := "2.10.1"

// sbt automatically finds the main class if it's a scala class
// otherwise the line below is required
// note that mainClass is an Option[String], e.g.
// mainClass := Some("Example")
mainClass := Some("storm.base.topology.Topology")

fork in run := true

resolvers ++= Seq(
  "clojars" at "http://clojars.org/repo/", // for storm
  "clojure-releases" at "http://build.clojure.org/releases"
)

libraryDependencies ++= Seq(
  "storm" % "storm" % "0.8.1" % "provided",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
  "junit" % "junit" % "4.10" % "test",
  "com.novocode" % "junit-interface" % "0.10-M4" % "test"
)
