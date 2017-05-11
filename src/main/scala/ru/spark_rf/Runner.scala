package ru.spark_rf

import java.util

import collection.JavaConverters._
import org.apache.spark.SparkContext
import ru.spark_rf.classifiers.{DecisionTree, RandomForest}
import ru.spark_rf.features.{NumericalFeature, CategoricalFeature, Feature}

object Runner {
  def main(args: Array[String]): Unit = {
    val dataPath = "file:///home/deathnik/tmp/sonar.all-dat"
    val defaultParallelJobsCount = 1
    val sc = new SparkContext("local", "SparkRf")
    val inputRdd = sc.textFile(dataPath, minPartitions = defaultParallelJobsCount)

    def sonarInputParser(str: String): Pair[util.ArrayList[Feature], Integer] = {
      val parts = str.split(",")
      val x: Array[Feature] = parts.dropRight(1).map(x => new NumericalFeature(java.lang.Double.parseDouble(x)))
      val y = if (parts(parts.length - 1) == "R") new Integer(1) else new Integer(0)
      Pair(Util.toArrayList(x), y)
    }

    def testSonarParser(str: String): util.ArrayList[Feature] = {
      val parts = str.split(",")
      val array = parts.dropRight(1).map(x => new NumericalFeature(java.lang.Double.parseDouble(x)) : Feature)
      Util.toArrayList(array)
    }

    val r = new Learner().fit[DecisionTree](inputRdd, () => new DecisionTree(1), sonarInputParser, 4)
    val serializedForest = r.map(x => x.serialize()).mkString(RandomForest.TREE_DELIMITER)
    //val result = new Learner().predict(inputRdd, () => new DecisionTree(), r(0).serialize(), testSonarParser)
    val result = new Learner().predict(inputRdd, () => new RandomForest(), serializedForest, testSonarParser)
    print(result)
    sc.stop()
  }
}