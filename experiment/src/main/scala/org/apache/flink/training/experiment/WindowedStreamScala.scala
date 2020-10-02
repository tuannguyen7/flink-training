package org.apache.flink.training.experiment

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object WindowedStreamScala {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)
    env.setMaxParallelism(1)
    val ds = env.fromElements(("X,Red,10"), ("Y,Blue,10"), ("Z,Black, 22"), ("U,Green,22"), ("N,Blue,25"), ("M,Green,23"))
    val ds2 = ds.map { line =>
      val Array(name, color, size) = line.split(",")
      Box(name.trim, color.trim, size.trim.toInt)
    }.keyBy(_.color).max("size")

    ds2.print()
    env.execute()
  }

  case class Box(name: String, color: String, size: Int)
}
