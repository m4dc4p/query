import AssemblyKeys._ // put this at the top of the file

name := "query"

version := "1.0"

scalaVersion := "2.11.2"

libraryDependencies ++= Seq("org.scalatest" %% "scalatest" % "2.2.2" % Test,
  "org.apache.commons" % "commons-csv" % "1.0")


assemblySettings
