package com.prevosql.config.operator;

/**
 * Models parameters set in configuration file
 */
public class PhysicalConfigParser {
    private static PhysicalConfigParser instance;

    /**
     * Returns global instance of ConfigFileParser
     *
     * @return Global instance of ConfigFileParser
     */
    public static PhysicalConfigParser getInstance() {
        if (instance == null) {
            instance = new PhysicalConfigParser();
        }
        return instance;
    }

    /**
     * @return Number of outer relation buffer pages to use in block-nested loop join
     */
    public int getNumOuterRelationBufferPages() {
        return 100;
    }

    /**
     * @return Number of sort buffer pages to use in external merge sort
     */
    public int getNumSortBufferPages() {
        return 100;
    }
}