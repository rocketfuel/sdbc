import sbt._
import sbt.Keys._

object Common {

  val macroParadiseVersion = "2.1.0"

  val previousVersions = Seq()

  //Some helpful compiler flags from https://tpolecat.github.io/2014/04/11/scalac-flags.html
  def extraScalacOptions(scalaVersion: String) =
    Seq(
      "-deprecation",
      "-encoding", "UTF-8",       // yes, this is 2 args
      "-feature",
      "-language:existentials",
      "-language:higherKinds",
      "-language:implicitConversions",
      "-language:postfixOps",
      "-unchecked"
    ) ++ {
      CrossVersion.partialVersion(scalaVersion) match {
        case Some((2, 11|12)) =>
          Seq("-Xfuture", "-Yno-adapted-args", "-target:jvm-1.8")
        case _ =>
          Seq.empty
      }
    }

  val settings = Seq(
    organization := "com.rocketfuel.sdbc",

    scalaVersion := "2.13.1",

    crossScalaVersions := Seq("2.11.12", "2.12.11"),

    unmanagedSourceDirectories in Compile ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, 11 | 12)) =>
          Seq(baseDirectory.value / "src" / "main" / "scala-2.1x")
        case Some((2, 13)) =>
          Seq()
      }
    },

    version := "4.0.0",

    licenses := Seq("The BSD 3-Clause License" -> url("http://opensource.org/licenses/BSD-3-Clause")),

    homepage := Some(url("https://github.com/rocketfuel/sdbc")),

    pomExtra :=
      <developers>
        <developer>
          <name>Jeff Shaw</name>
          <id>shawjef3</id>
          <url>https://github.com/shawjef3/</url>
          <organization>Rocketfuel</organization>
          <organizationUrl>http://rocketfuel.com/</organizationUrl>
        </developer>
      </developers>
        <scm>
          <url>git@github.com:rocketfuel/sdbc.git</url>
          <connection>scm:git:git@github.com:rocketfuel/sdbc.git</connection>
        </scm>,

    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },

    publishMavenStyle := true,

    publishArtifact in Test := true,

    scalacOptions ++= extraScalacOptions(scalaVersion.value)
  )

  val xml = "org.scala-lang.modules" %% "scala-xml" % "1.3.0"

  def fs2(scalaVersion: String) =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, 12|13)) =>
        "co.fs2" %% "fs2-core" % "2.3.0"
      case Some((2, 11)) =>
        "co.fs2" %% "fs2-core" % "2.1.0"
    }

  def fs2IO(scalaVersion: String) =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, 12|13)) =>
        "co.fs2" %% "fs2-io" % "2.3.0"
      case Some((2, 11)) =>
        "co.fs2" %% "fs2-io" % "2.1.0"
    }

  def scodec(scalaVersion: String) =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, 12|13)) =>
        "org.scodec" %% "scodec-bits" % "1.1.14"
      case Some((2, 11)) =>
        "org.scodec" %% "scodec-bits" % "1.1.12"
    }

}
