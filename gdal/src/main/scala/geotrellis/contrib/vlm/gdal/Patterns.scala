/*
 * Copyright 2019 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  final val QUERY_PARAMS_PATTERN: Regex = """\?.+""".r
}
