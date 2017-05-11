package ru.spark_rf;

import org.apache.spark.SparkContext;
import ru.spark_rf.classifiers.AdaBoostClassifier;
import ru.spark_rf.classifiers.QualityMetrics;
import ru.spark_rf.features.Feature;
import ru.spark_rf.features.NumericalFeature;

import java.io.*;
import java.util.*;


public class Test {
    SparkContext sc = null;

    public Test(Boolean useSpark) {
        if (useSpark) {
            sc = new SparkContext("local", "SparkRf");
        }
    }

    AdaBoostClassifier getClassifier() {
        return new AdaBoostClassifier(10, sc);
    }


    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        Test test = new Test(true);
        test.testSpambaseData();
        test.testRAOP();

        long endTime = System.currentTimeMillis();
        System.out.println("That took " + (endTime - startTime) + " milliseconds");
    }

    private void testRAOP() {
        ArrayList<ArrayList<Feature>> xTrain = new ArrayList<ArrayList<Feature>>();
        ArrayList<Integer> yTrain = new ArrayList<Integer>();

        ArrayList<ArrayList<Feature>> xTest = new ArrayList<ArrayList<Feature>>();
        ArrayList<Integer> yTest = new ArrayList<Integer>();

        ArrayList<ArrayList<Feature>> currentSampleX = xTrain;
        ArrayList<Integer> currentSampleY = yTrain;

        Random gen = new Random(23);
        try {
            BufferedReader reader = new BufferedReader(new FileReader("datasets/raop.txt"));
            String line = reader.readLine();

            while (line != null) {
                String[] tokens = line.split(",");

                if (gen.nextBoolean()) {
                    currentSampleX = xTrain;
                    currentSampleY = yTrain;
                } else {
                    currentSampleX = xTest;
                    currentSampleY = yTest;
                }

                currentSampleY.add(Integer.parseInt(tokens[12]));
                ArrayList<Feature> row = new ArrayList<Feature>();
                for (int i = 0; i < 12; ++i) {
                    row.add(new NumericalFeature(Double.parseDouble(tokens[i])));
                }

                currentSampleX.add(row);

                line = reader.readLine();
            }

            reader.close();
        } catch (IOException e) {

        }

        ArrayList<Double> w = new ArrayList<Double>();
        for (int i = 0; i < xTrain.size(); ++i) w.add(1.0);

        AdaBoostClassifier rf = this.getClassifier();
        rf.fit(xTrain, yTrain);

        ArrayList<Double> predicted = new ArrayList<Double>();
        int ok = 0;

        for (int i = 0; i < xTest.size(); ++i) {
            int q = rf.predict(xTest.get(i));
            if (q == yTest.get(i)) {
                ++ok;
            }
            predicted.add(rf.predict_proba(xTest.get(i), 1));
        }
        System.out.println(ok + " " + yTest.size() + " " + QualityMetrics.aucROC(predicted, yTest));

        try {
            BufferedReader reader = new BufferedReader(new FileReader("datasets/raop_test.txt"));
            BufferedWriter writer = new BufferedWriter(new FileWriter("datasets/raop_answers.txt"));

            writer.write("request_id,requester_received_pizza\n");

            String line = reader.readLine();
            while (line != null) {
                String[] tokens = line.split(",");

                ArrayList<Feature> row = new ArrayList<Feature>();

                for (int i = 0; i < 12; i++) {
                    row.add(new NumericalFeature(Double.parseDouble(tokens[i])));
                }

                writer.write(tokens[12] + "," + rf.predict(row) + "\n");
                line = reader.readLine();
            }
            writer.close();
        } catch (IOException e) {

        }

    }

    private void testSpambaseData() {
        ArrayList<ArrayList<Feature>> xTrain = new ArrayList<ArrayList<Feature>>();
        ArrayList<Integer> yTrain = new ArrayList<Integer>();

        ArrayList<ArrayList<Feature>> xTest = new ArrayList<ArrayList<Feature>>();
        ArrayList<Integer> yTest = new ArrayList<Integer>();

        ArrayList<ArrayList<Feature>> currentSampleX = xTrain;
        ArrayList<Integer> currentSampleY = yTrain;

        Random gen = new Random(23);
        try {
            BufferedReader reader = new BufferedReader(new FileReader("datasets/spambase_data.txt"));
            String line = reader.readLine();

            while (line != null) {
                String[] tokens = line.split(",");

                if (gen.nextBoolean()) {
                    currentSampleX = xTrain;
                    currentSampleY = yTrain;
                } else {
                    currentSampleX = xTest;
                    currentSampleY = yTest;
                }

                currentSampleY.add(Integer.parseInt(tokens[57]));
                ArrayList<Feature> row = new ArrayList<Feature>();
                for (int i = 0; i < 57; ++i) {
                    row.add(new NumericalFeature(Double.parseDouble(tokens[i])));
                }

                currentSampleX.add(row);

                line = reader.readLine();
            }

            reader.close();
        } catch (IOException e) {

        }
        AdaBoostClassifier rf = this.getClassifier();
        ArrayList<Double> w = new ArrayList<Double>();

        for (int i = 0; i < xTrain.size(); ++i) w.add(1.0);

        rf.fit(xTrain, yTrain);

        ArrayList<Double> predicted = new ArrayList<Double>();
        int ok = 0;

        for (int i = 0; i < xTest.size(); ++i) {
            int q = rf.predict(xTest.get(i));
            if (q == yTest.get(i)) {
                ++ok;
            }
            predicted.add(rf.predict_proba(xTest.get(i), 1));
        }
        System.out.println(ok + " " + yTest.size() + " " + QualityMetrics.aucROC(predicted, yTest));

    }
}