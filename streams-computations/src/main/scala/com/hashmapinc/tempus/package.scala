package com.hashmapinc

package object tempus {

  type OptionsMap = Map[String, String]
  type ErrorOrOptions = Either[Throwable, OptionsMap]
  type ID[X] = X
}
