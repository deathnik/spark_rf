package ru.spark_rf.classifiers;

import java.util.ArrayList;

abstract public class AbstractClassifier {
    AbstractClassifier(){};
    abstract public void fit(ArrayList<ArrayList<Double>> x, ArrayList<Integer> y);

    abstract public int predict(ArrayList<Double> x);

    abstract public String serialize();

    abstract public AbstractClassifier deserialize(String data);
}