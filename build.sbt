ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.7.1"

val sparkVersion = "4.0.0"

//val scalusVersion = "0.10.4"
val scalusVersion = "0.10.1+366-625465d3-SNAPSHOT" // local build from master branch

lazy val root = (project in file(".")).settings(
  name := "scalus-spark",
  libraryDependencies ++= Seq(
    ("org.apache.spark" %% "spark-sql" % sparkVersion % "provided").cross(CrossVersion.for3Use2_13),
    "org.scalus" %% "scalus" % scalusVersion,
    "org.scalameta" %% "munit" % "1.1.1" % Test,
  )
)

Compile / run := Defaults.runTask(Compile / fullClasspath,
  Compile / run / mainClass, Compile / run / runner).evaluated

Test / javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)