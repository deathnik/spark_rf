package ru.spark_rf.features;

public class NumericalFeature extends Feature {
    private Double value;

    public NumericalFeature(Double value) {
        this.value = value;
    }

    public Double getValue() {
        return value;
    }

    @Override
    public int compareTo(Object o) {
        return Double.compare(value, ((NumericalFeature) o).getValue());
    }
}
