package ru.spark_rf

import java.util

object Util {
  //TODO: search for better way
  def toArrayList[T](scalaArray: Array[T]): util.ArrayList[T] = {
    val lst = new util.ArrayList[T]()
    for (el <- scalaArray) {
      lst.add(el)
    }
    lst
  }
}
