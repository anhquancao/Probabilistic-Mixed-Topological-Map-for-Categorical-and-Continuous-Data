package com.quan.main

import org.apache.log4j.{Level, Logger}


object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val r = Reader.read("src/resources/mfeat-kar.txt", "[ \t]+").map(_.map(_.toDouble))
    val b = Reader.read("src/resources/digits.csv", ",").map(_.map(_.toInt))
    println("r size: " + r.count() + " x " + r.take(1)(0).size)
    println("b size: " + b.count() + " x " + b.take(1)(0).size)
  }
}
