import sbt._
import Keys._

object StuccoRTBuild extends Build {
  lazy val root = Project("root", file("."))
    // .dependsOn(config) // can chain these

  // example:
  // lazy val config = GitHub("typesafehub", "config", "master")

  def GitHub(user: String, project: String, tag: String) =
      RootProject(
        uri("https://github.com/%s/%s.git#%s".format(user, project, tag))
      )
}
