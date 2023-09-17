ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion :="2.13.11"

lazy val root = (project in file("."))
  .settings(
    name := "stackexchange"
  )

libraryDependencies += "org.rocksdb" % "rocksdbjni" % "8.1.1.1"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2"
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "2.1.0"
libraryDependencies += "org.apache.commons" % "commons-compress" % "1.23.0"
libraryDependencies += "org.tukaani" % "xz" % "1.9"