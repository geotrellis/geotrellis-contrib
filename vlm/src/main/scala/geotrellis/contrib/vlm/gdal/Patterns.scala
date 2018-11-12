package geotrellis.contrib.vlm.gdal

import scala.util.matching.Regex


/*
 * Contains the different Regexs needed to parse
 * the paths given to GDAL to read.
 */
object Patterns {
  final val SCHEME_PATTERN: Regex = """.*?(?=\:)""".r
  final val FIRST_SCHEME_PATTERN: Regex = """[^\+]*""".r
  final val SECOND_SCHEME_PATTERN: Regex = """(?<=\+).*?(?=\:)""".r

  final val USER_INFO_PATTERN: Regex = """(?<=\/\/).*?(?=@)""".r

  final val AUTHORITY_PATTERN: Regex = """(?<=\/\/).*?(?=\/)""".r

  final val DEFAULT_PATH_PATTERN: Regex = """(?<=(?:(\/){2})).+""".r
  final val WINDOWS_LOCAL_PATH_PATTERN: Regex = """(?<=(?:(\/))).+""".r

  final val VSI_PATTERN: Regex = """/vsi[a-zA-Z].+/""".r
}
