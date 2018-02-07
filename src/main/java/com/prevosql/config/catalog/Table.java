package com.prevosql.config.catalog;

import javafx.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class representing logical tables, used to map
 * column names to to tuple indices
 */
public class Table {
    private String fileName;
    private String tableName;
    private Map<Pair<String, String>, Integer> columnMap;
    private List<String> attributeList;

    /**
     * Default constructor for logical tables
     */
    public Table() {
        columnMap = new HashMap<>();
        attributeList = new ArrayList<>();
    }

    /**
     *
     * Builds a logical table from a tablename and a filename
     *
     * @param tname Table name
     * @param fname Filename
     */
    Table(String tname, String fname) {
        this();
        tableName = tname.toLowerCase();
        fileName = fname;
    }

    /**
     * Builds a table from an existing table to be aliased
     *
     * @param alias Alias to use for existing table
     * @param other Table to be aliased
     */
    Table(String alias, Table other) {
        this();
        if (other == null) {
            System.err.println("No such table in schema");
            System.exit(1);
        }
        tableName = other.tableName;
        fileName = other.fileName;
        for (Pair<String, String> p : other.columnMap.keySet()) {
            Pair<String, String> newPair = new Pair<>(alias, p.getValue());
            setIndexForColumn(newPair, other.columnMap.get(p));
        }
    }

    /**
     * Gets a tuple com.cs5321.index for a column name in a table
     *
     * @param columnName Name of column
     * @return Index of column information in tuple
     */
    public Integer getIndexForColumn(String columnName) {
        return columnMap.get(new Pair<>(tableName, columnName));
    }

    /**
     * Sets a tuple com.cs5321.index for a column. Only used for aliased columns/tables
     *
     * @param key A key-value pair with the table name as the key, column name as value
     * @param index The new column com.cs5321.index
     */
    private void setIndexForColumn(Pair<String, String> key, int index) {
        columnMap.put(new Pair<>(key.getKey(), key.getValue()), index);
        attributeList.add(key.getValue());
    }

    /**
     * Sets a tuple com.cs5321.index for a column, used for non aliased columns/tables
     *
     * @param column Name of column to set the com.cs5321.index for
     * @param index New column com.cs5321.index
     */
    void setIndexForColumn(String column, int index) {
        columnMap.put(new Pair<>(tableName, column), index);
        attributeList.add(column);
    }

    /**
     * Get name of logical table
     *
     * @return Name of logical table
     */
    public String getName() {
        return tableName;
    }

    /**
     * Set name of logical table
     *
     * @param tableName Name of logical table
     */
    void setName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Get the filename for this table
     *
     * @return Filename of this table
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * Joins two tables, if pickLeft is set, returns the left table, otherwise
     * returns the right table
     *
     * @param left Left table to join
     * @param right Right table to join
     * @param pickLeft If true, returns joined table with left table name, otherwise returns
     *                 joined table with right table name
     * @return Table made by joining left and right tables
     */
    static Table createJoinedTable(Table left, Table right, boolean pickLeft) {
        try {
            Table res;
            if (pickLeft) {
                res = new Table(left.tableName.toLowerCase(), left.fileName);
            } else {
                res = new Table(right.tableName.toLowerCase(), right.fileName);
            }
            int offset = left.columnMap.keySet().size();
            if (!left.getFileName().equalsIgnoreCase(right.getFileName())) {
                for (Pair<String, String> p : left.columnMap.keySet()) {
                    res.setIndexForColumn(new Pair<>(res.getName(), p.getValue()), left.columnMap.get(p));
                }
                for (Pair<String, String> p : right.columnMap.keySet()) {
                    res.setIndexForColumn(new Pair<>(res.getName(), p.getValue()), right.columnMap.get(p) + offset);
                }
            } else {
                for (Pair<String, String> p : left.columnMap.keySet()) {
                    res.setIndexForColumn(p, left.columnMap.get(p));
                }
                for (Pair<String, String> p : right.columnMap.keySet()) {
                    res.setIndexForColumn(p, right.columnMap.get(p) + offset);
                }
            }
            return res;
        } catch (NullPointerException e) {
            System.err.println("Invalid table chosen");
            return null;
        }
    }

    /**
     * @return List of attributes in table
     */
    public List<String> getAttributeList() {
        return attributeList;
    }
}
