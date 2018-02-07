package com.prevosql.config.catalog;

import org.apache.log4j.Logger;

import java.util.HashMap;

/**
 * Abstract class to model DB catalogs
 */
public abstract class Catalog {
    HashMap<String, Table> tableMap;
    HashMap<String, String> aliasMap;
    static Logger LOG;

    /**
     * Constructs a com.cs5321.config.catalog
     */
    Catalog() {
        LOG = Logger.getLogger(this.getClass());
    }

    /**
     * Gets a logical table based on a table name
     *
     * @param tableName Name of table to get
     * @return Logical table
     */
    public abstract Table getTable(String tableName);
}
