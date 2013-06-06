import AssemblyKeys._

assemblySettings

name := "storm-base"

version := "1.0"

scalaVersion := "2.10.1"

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
