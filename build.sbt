name := "jspark"

version := "1.0"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.2",
  "org.apache.hive" % "hive-jdbc" % "1.2.1",
  "com.github.scopt" %% "scopt" % "3.7.0"
)

enablePlugins(AssemblyPlugin)

mainClass in assembly := Some("JSpark")
assemblyJarName in assembly := "jspark.jar"
assemblyMergeStrategy in assembly := {
  case PathList("javax", "transaction", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case "plugin.xml" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
