name := "paypay"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
	"org.apache.spark" %% "spark-sql" % "1.6.0" % "provided",
	"org.apache.spark" %% "spark-hive" % "1.6.0" % "provided",
	"org.apache.spark" %% "spark-mllib" % "1.6.0" % "provided",
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.startsWith("META-INF") => MergeStrategy.discard
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", xs @ _*) => MergeStrategy.first
    case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
    case "about.html"  => MergeStrategy.rename
    case "reference.conf" => MergeStrategy.concat
    case _ => MergeStrategy.first
}
