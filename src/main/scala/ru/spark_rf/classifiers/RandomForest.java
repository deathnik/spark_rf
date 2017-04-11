package ru.spark_rf.classifiers;
// by  @zxqfd555 https://github.com/vitux/java_decision_tree/blob/master/RandomForest.java

import java.util.*;


public class RandomForest extends AbstractClassifier {
    public static final String TREE_DELIMITER = "!";

    private int nTrees;
    private ArrayList<DecisionTree> trees;

    /*
     Constructors: the number of trees in the forest will be equal to 10 by default,
                   or can be re-specified in the constructor.
     */

    private void init() {
        trees = new ArrayList<DecisionTree>();
    }

    public RandomForest() {
        init();
        nTrees = 10;
    }

    public RandomForest(int _nTrees) {
        init();
        nTrees = _nTrees;
    }

    /*
     Helpers.
     */

    private ArrayList<Integer> getSample(int samplesNumber) {
        ArrayList<Integer> result = new ArrayList<Integer>();
        Random gen = new Random();
        for (int i = 0; i < samplesNumber; ++i) {
            if (gen.nextBoolean()) {
                result.add(i);
            }
        }
        if (result.size() == 0) {
            result.add(gen.nextInt(samplesNumber));
        }
        return result;
    }

    private Integer mostFrequentElement(ArrayList<Integer> input) {
        int seqEqual = 1, maxSeqEqual = 0, answer = -1;
        for (int i = 1; i < input.size(); ++i) {
            if (input.get(i) == input.get(i - 1)) {
                ++seqEqual;
            } else {
                if (seqEqual > maxSeqEqual) {
                    maxSeqEqual = seqEqual;
                    answer = input.get(i - 1);
                }
                seqEqual = 1;
            }
        }
        if (seqEqual > maxSeqEqual) {
            maxSeqEqual = seqEqual;
            answer = input.get(input.size() - 1);
        }
        return answer;
    }

    /*
     Fitting the classifier.
     */
    @Override
    public void fit(ArrayList<ArrayList<Double>> x, ArrayList<Integer> y) {
        int samplesNumber = x.size();

        trees = new ArrayList<DecisionTree>();
        for (int treeId = 0; treeId < nTrees; ++treeId) {
            DecisionTree currentTree = new DecisionTree();

            ArrayList<ArrayList<Double>> sampleX = new ArrayList<ArrayList<Double>>();
            ArrayList<Integer> sampleY = new ArrayList<Integer>();

            ArrayList<Integer> subsampleIndices = getSample(samplesNumber);
            for (int index : subsampleIndices) {
                sampleX.add(x.get(index));
                sampleY.add(y.get(index));
            }

            currentTree.fit(sampleX, sampleY);

            trees.add(currentTree);

        }
    }

    @Override
    public int predict(ArrayList<Double> x) {
        ArrayList<Integer> answers = new ArrayList<Integer>();
        for (DecisionTree tree : trees) {
            answers.add(tree.predict(x));
        }
        Collections.sort(answers);
        return mostFrequentElement(answers);
    }

    @Override
    public String serialize() {
        StringBuilder sb = new StringBuilder();
        for (DecisionTree dt : this.trees) {
            if (sb.length() > 0) {
                sb.append(TREE_DELIMITER);
            }
            sb.append(dt.serialize());
        }
        return sb.toString();
    }

    @Override
    public AbstractClassifier deserialize(String data) {
        RandomForest clf = new RandomForest();
        String[] parts = data.split(TREE_DELIMITER);
        clf.nTrees = parts.length;
        ArrayList<DecisionTree> dtLst = new ArrayList<>();
        for (String part : parts) {
            dtLst.add(new DecisionTree().deserialize(part));
        }
        clf.trees = dtLst;
        return clf;
    }

};
