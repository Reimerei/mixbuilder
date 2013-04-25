import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

  val appName         = "mixbuilder"
  val appVersion      = "0.000001"

  val appDependencies = Seq(
    // Add your project dependencies here,
    javaCore,
    javaJdbc,
    javaEbean,
    "mysql" % "mysql-connector-java" % "5.1.18"
  )

  val main = play.Project(appName, appVersion, appDependencies).settings(
    // Add your own project settings here      
  )

}
