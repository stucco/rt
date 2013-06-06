seq(com.github.retronym.SbtOneJar.oneJarSettings: _*) // include these settings

name := "greeter"

version := "1.0"

scalaVersion := "2.10.1"

// sbt automatically finds the main class if it's a scala class
// otherwise the line below is required
// mainClass := Some("Example")
error("Main class must be specified in build.sbt file")

fork in run := true

resolvers += "clojars.org" at "http://clojars.org/repo" // for storm

libraryDependencies ++= Seq(
  "storm" % "storm" % "0.7.2" % "provided",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
  "junit" % "junit" % "4.10" % "test"
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
