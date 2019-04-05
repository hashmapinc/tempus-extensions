package com.hashmapinc

import scala.collection.mutable

package object tempus {

  type OptionsMap = mutable.Map[String, String]
  type ErrorOrOptions = Either[Throwable, OptionsMap]
  type ID[X] = X
}
