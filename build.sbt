name          := "ScalaWordNet"

organization  := "de.sciss"

version       := "0.1.0-SNAPSHOT"

scalaVersion  := "2.11.7"

scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-encoding", "utf8", "-Xfuture")

licenses      := Seq("GPL v2+" -> url("https://www.gnu.org/licenses/gpl-2.0.txt"))

libraryDependencies ++= Seq(
  "de.sciss"        % "ws4j"            % "0.1.0",
  "net.sf.jwordnet" % "jwnl"            % "1.4_rc3",
  "com.novocode"    % "junit-interface" % "0.11"     % "test"
)

// cf. http://www.scala-sbt.org/0.13.5/docs/Detailed-Topics/Classpaths.html
unmanagedClasspath in Runtime += baseDirectory.value / "src/main/config"
unmanagedClasspath in Test    += baseDirectory.value / "src/main/config"
unmanagedClasspath in Compile += baseDirectory.value / "src/main/config"

initialCommands in console :=
  """import de.sciss.wordnet._
    |val wn = WordNet()
    |import wn._
    |""".stripMargin

lazy val `download-database` = taskKey[Unit]("Download the word-net database and installation to config and link")

// cf.https://stackoverflow.com/questions/27466869/download-a-zip-from-url-and-extract-it-in-resource-using-sbt
`download-database` := {
  val configDir = file("src/main/config")
  val dbFile    = configDir / "wnjpn.db"
  val st        = streams.value
  if (dbFile.exists()) {
    st.log.info(s"Database file ${dbFile.name} already present.")
  } else {
    st.log.info("Downloading database...")
    IO.withTemporaryFile(prefix = "wnjpn.db", postfix = "gz") { tmpFile =>
      IO.download(new URL("http://compling.hss.ntu.edu.sg/wnja/data/1.1/wnjpn.db.gz"), tmpFile)
      IO.gunzip(tmpFile, dbFile)
    }
  }
  val linkDir = file("link")
  val wnFile  = linkDir / "WordNet-3.0"
  if (wnFile.exists()) {
    st.log.info(s"WordNet installation ${wnFile.name} already present.")
  } else {
    st.log.info("Downloading WordNet...")
    IO.withTemporaryFile(prefix = "WordNet", postfix = "gz") { tmpFile =>
      IO.download(new URL("http://wordnetcode.princeton.edu/3.0/WordNet-3.0.tar.gz"), tmpFile)
      Seq("tar", "-xf", tmpFile.getPath, "-C", linkDir.getPath).!
    }
  }
}
