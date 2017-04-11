package ru.spark_rf

import java.util

import collection.JavaConverters._
import org.apache.spark.SparkContext
import ru.spark_rf.classifiers.{DecisionTree, RandomForest}

object Runner {
  def main(args: Array[String]): Unit = {
    val dataPath = "file:///home/deathnik/tmp/sonar.all-dat"
    val defaultParallelJobsCount = 1
    val sc = new SparkContext("local", "SparkRf")
    val inputRdd = sc.textFile(dataPath, minPartitions = defaultParallelJobsCount)

    def sonarInputParser(str: String): Pair[util.ArrayList[java.lang.Double], Integer] = {
      val parts = str.split(",")
      val x: Array[java.lang.Double] = parts.dropRight(1).map(x => java.lang.Double.parseDouble(x): java.lang.Double)
      val y = if (parts(parts.length - 1) == "R") new Integer(1) else new Integer(0)
      Pair(Util.toArrayList(x), y)
    }

    def testSonarParser(str: String): util.ArrayList[java.lang.Double] = {
      val parts = str.split(",")
      val array = parts.dropRight(1).map(x => java.lang.Double.parseDouble(x): java.lang.Double)
      Util.toArrayList(array)
    }

    val r = new Learner().fit[DecisionTree](inputRdd, () => new DecisionTree(), sonarInputParser, 4)
    val serializedForest = r.map(x => x.serialize()).mkString(RandomForest.TREE_DELIMITER)
    //val result = new Learner().predict(inputRdd, () => new DecisionTree(), r(0).serialize(), testSonarParser)
    val result = new Learner().predict(inputRdd, () => new RandomForest(), serializedForest, testSonarParser)
    print(result)
    sc.stop()
  }
}