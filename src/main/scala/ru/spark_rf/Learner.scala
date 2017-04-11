package ru.spark_rf

import java.util

import org.apache.spark.rdd.RDD
import ru.spark_rf.classifiers.AbstractClassifier


class Learner() extends java.io.Serializable {
  private def train[T <: AbstractClassifier](rdd: RDD[String], contructor: () => T,
                                             inputParser: String => Pair[util.ArrayList[java.lang.Double], Integer],
                                             splitFactor: Int): RDD[String] = {

    def partitionMapper(inputIterator: Iterator[String]): Iterator[String] = {
      val X = new util.ArrayList[util.ArrayList[java.lang.Double]]()
      val Y = new util.ArrayList[Integer]()
      for (inputStr <- inputIterator) {
        val parsed = inputParser(inputStr)
        X.add(parsed._1)
        Y.add(parsed._2)
      }

      val classifierInstance = contructor()
      classifierInstance.fit(X, Y)
      Iterator.single(classifierInstance.serialize())
    }
    rdd.repartition(splitFactor).mapPartitions(partitionMapper)
  }

  def fit[T <: AbstractClassifier](rdd: RDD[String], contructor: () => T,
                                   inputParser: String => Pair[util.ArrayList[java.lang.Double], Integer],
                                   splitFactor: Int): Array[AbstractClassifier] = {
    val trainResult = train(rdd, contructor, inputParser, splitFactor)
    val fakeObject: T = contructor()
    val materializedClassifiers = trainResult.collect().map(s => fakeObject.deserialize(s))
    materializedClassifiers
  }

  def predict[T <: AbstractClassifier](rdd: RDD[String], contructor: () => T, serializedClassifier: String,
                                       inputParser: String => util.ArrayList[java.lang.Double]): util.ArrayList[Integer] = {
    def makePredictions(str: String): Integer = {
      val classifier = contructor().deserialize(serializedClassifier)
      classifier.predict(inputParser(str))
    }
    val materializedResult = rdd.map(makePredictions).collect()
    Util.toArrayList(materializedResult)
  }
}
