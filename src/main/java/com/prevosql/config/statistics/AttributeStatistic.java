package com.prevosql.config.statistics;

/**
 * Models attribute statistics
 */
public class AttributeStatistic {
    private int minValue;
    private int maxValue;

    /**
     * Constructs an attribute statistic
     *
     * @param minValue Minimum value of attribute
     * @param maxValue Maximum value of attribute
     */
    public AttributeStatistic(int minValue, int maxValue) {
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    /**
     * @return Minimum value of attribute
     */
    public int getMinValue() {
        return minValue;
    }

    /**
     * @return Maximum value of attribute
     */
    public int getMaxValue() {
        return maxValue;
    }

    /**
     * Sets minimum value of attribute
     *
     * @param minValue value to set attribute
     */
    public void setMinValue(int minValue) {
        this.minValue = minValue;
    }

    /**
     * Sets maximum value of attribute
     *
     * @param maxValue value to set attribute
     */
    public void setMaxValue(int maxValue) {
        this.maxValue = maxValue;
    }
}