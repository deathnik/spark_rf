package ru.spark_rf

import org.apache.spark.SparkContext
import ru.spark_rf.classifiers.ParallelMultipleDecisionTrees

object Example {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "SparkRf")
    val source = scala.io.Source.fromFile(Runner.localPath)
    val lines = try source.mkString finally source.close()
    val p = lines.split("\n").map(Runner.sonarInputParser).unzip

    val parallel_tree = new ParallelMultipleDecisionTrees(10, 2.5, sc)
    parallel_tree.fit(Util.toArrayList(p._1), Util.toArrayList(p._2))

    println(Util.toArrayList(p._2))
    println(parallel_tree.predict(Util.toArrayList(p._1)))

    sc.stop()
  }
}
