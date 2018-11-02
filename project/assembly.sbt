//http://stackoverflow.com/questions/28459333/how-to-build-an-uber-jar-fat-jar-using-sbt-within-intellij-idea

addSbtPlugin("com.lightbend.sbt" % "sbt-proguard" % "0.3.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.0")
