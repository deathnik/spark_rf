package ru.spark_rf.classifiers

import java.lang.Double
import java.util

import org.apache.spark.SparkContext
import ru.spark_rf.Util
import ru.spark_rf.features.Feature

import scala.collection.JavaConversions._

object FittingMapper {
  def apply(inputIterator: Iterator[(util.ArrayList[Feature], Double, Integer)]): Iterator[String] = {
    val X = new util.ArrayList[util.ArrayList[Feature]]
    val W = new util.ArrayList[Double]
    val Y = new util.ArrayList[Integer]
    for ((x, w, y) <- inputIterator) {
      X.add(x)
      W.add(w)
      Y.add(y)
    }
    val dt = new DecisionTree(3)
    dt.fit(X, W, Y)
    Iterator.single(dt.serialize())
  }
}

class PredictingMapper(serializedTrees: String, soughtClass: Int) extends Serializable {
  def this(serializedTrees: String) = this(serializedTrees, 0)

  def predict(inputIterator: Iterator[util.ArrayList[Feature]]): Iterator[Integer] = {
    val classifier = new MultipleDecisionTrees().deserialize(serializedTrees)
    for (el <- inputIterator) yield classifier.predict(el)
  }

  def predict_proba(inputIterator: Iterator[util.ArrayList[Feature]]): Iterator[Double] = {
    val classifier = new MultipleDecisionTrees().deserialize(serializedTrees).asInstanceOf[MultipleDecisionTrees]
    for (el <- inputIterator) yield classifier.predict_proba(el, soughtClass)
  }
}

class ParallelMultipleDecisionTrees(nTrees: Int, sc: SparkContext) extends MultipleDecisionTrees {
  def this(nTrees: Int) = this(nTrees, null)


  override def fit(x: util.ArrayList[util.ArrayList[Feature]], w: util.ArrayList[Double], y: util.ArrayList[Integer]): Unit = {
    if (sc == null) {
      super.fit(x, w, y)
    } else {
      val data = (x, w, y).zipped.toList
      val inputRdds = sc.makeRDD(data).randomSplit(Array.fill(this.nTrees)(1.0))
      val trees = inputRdds.map(x => x.repartition(1).mapPartitions(FittingMapper.apply).collect()(0))
      for (serializedTree <- trees) {
        val tree: DecisionTree = new DecisionTree(0).deserialize(serializedTree).asInstanceOf[DecisionTree]
        this.trees.add(tree)
      }
    }
  }

  def predict(dataset: java.util.ArrayList[java.util.ArrayList[Feature]]): java.util.ArrayList[Integer] = {
    val result = sc.makeRDD(dataset).mapPartitions(new PredictingMapper(this.serialize()).predict).collect()
    Util.toArrayList(result)
  }

}
