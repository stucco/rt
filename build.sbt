seq(com.github.retronym.SbtOneJar.oneJarSettings: _*) // include these settings

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
  "redis.clients" % "jedis" % "2.1.0",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
  "junit" % "junit" % "4.10" % "test",
  "com.novocode" % "junit-interface" % "0.8" % "test->default"
)

// generate shell script that will run the storm topology
TaskKey[File]("generate-storm") <<= (baseDirectory, fullClasspath in Compile, mainClass in Compile) map { (base, cp, main) =>
  val template = """|#!/bin/sh
                    |java -classpath "%s" %s "$@"
                    |""".stripMargin
  val mainStr = main getOrElse error("No main class specified")
  val contents = template.format(cp.files.absString, mainStr)
  val out = base / "bin/run-main-topology.sh"
  IO.write(out, contents)
  out.setExecutable(true)
  out
}
