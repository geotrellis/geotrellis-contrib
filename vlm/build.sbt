
Compile / unmanagedSourceDirectories ++= {
  val scalaBase = (Compile / scalaSource).value
  Dependencies.geotrellisVector.revision match {
    case Version("2", "1", _) ⇒ Seq(scalaBase.getParentFile / "scala-gt-2.1")
    case _ ⇒ Seq(scalaBase.getParentFile / "scala-gt-3")
  }
}
