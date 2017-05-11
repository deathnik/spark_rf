package ru.spark_rf.features;

public class CategoricalFeature extends Feature {
    private Integer value;

    public CategoricalFeature(Integer value) {
        this.value = value;
    }

    public Integer getValue() {
        return value;
    }

    @Override
    public int compareTo(Object o) {
        if (value == null)
            return -1;
        if (((CategoricalFeature) o).getValue() == null)
            return 1;
        return Integer.compare(value, ((CategoricalFeature) o).getValue());
    }
}
