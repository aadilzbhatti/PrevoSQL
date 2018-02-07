package com.prevosql.config.statistics;

import java.util.HashMap;
import java.util.Map;

/**
 * Models a relation statistic
 */
public class RelationStatistic {
    private int numTuples;
    private int numAttributes;
    private Map<String, AttributeStatistic> attributeMap;

    /**
     * Constructs a relation statistic
     *
     * @param numTuples Number of tuples in the relation
     * @param attributeStats Map of attribute statistics to add to this relation stat
     */
    RelationStatistic(int numTuples, Map<String, AttributeStatistic> attributeStats) {
        this.numTuples = numTuples;
        this.attributeMap = new HashMap<>();
        this.numAttributes = 0;
        for (String attribute : attributeStats.keySet()) {
            addAttributeStatistic(attribute, attributeStats.get(attribute));
            this.numAttributes++;
        }
    }

    /**
     * Returns attribute statistic corresponding to the input
     * attribute
     *
     * @param attribute Input attribute to get statistic for
     * @return Attribute statistic corresponding to the input attribute
     */
    public AttributeStatistic getAttributeStatistic(String attribute) {
        return attributeMap.get(attribute);
    }

    /**
     * @return Number of tuples in the relation
     */
    public int getNumTuples() {
        return numTuples;
    }

    /**
     * @return Number of attributes in the relation
     */
    public int getNumAttributes() {
        return numAttributes;
    }

    /**
     * Adds an attribute statistic to the relation statistic
     *
     * @param attribute Name of attribute
     * @param statistic Attribute statistic corresponding to this attribute
     */
    private void addAttributeStatistic(String attribute, AttributeStatistic statistic) {
        attributeMap.put(attribute, statistic);
    }
}
