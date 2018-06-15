package com.quan.main

import org.apache.log4j.{Level, Logger}


object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val r = Reader.read("src/resources/mfeat-kar.txt", "[ \t]+")

    println("r size: " + r.count() + " x " + r.take(1)(0).size)
  }
}
