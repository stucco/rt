import sbt._
import Keys._

object StuccoRTBuild extends Build {
  lazy val root = Project("root", file("."))
    // .dependsOn(sbteclipse) // can chain these

  // example:
  // lazy val sbteclipse =
    // RootProject("typesafehub", "sbteclipse", "v1.2")

  def GitHub(user: String, project: String, tag: String) =
      uri("https://github.com/%s/%s.git#%s".format(user, project, tag))
}
