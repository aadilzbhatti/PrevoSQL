package com.prevosql.util.disjointset;

import com.prevosql.config.catalog.DBCatalog;
import net.sf.jsqlparser.schema.Column;

import java.util.*;

/**
 * Models union-find data structure
 */
public class DisjointSet {
    private List<Element> sets;
    private Map<String, Column> columnMap;
    private Map<String, Boolean> tableMap;

    /**
     * Constructs a DisjointSet
     */
    public DisjointSet() {
        sets = new ArrayList<>();
        columnMap = new HashMap<>();
        tableMap = new HashMap<>();
    }

    /**
     * Makes a disjoint set element with only this column
     *
     * @param column Column to make set out of
     * @return The created disjoint set element
     */
    public Element makeSet(Column column) {
        Element s = new Element(column);
        sets.add(s);
        columnMap.put(column.toString(), column);
        tableMap.put(column.getTable().getName(), true);
        return s;
    }

    /**
     * Merges the disjoint sets which hold these columns
     *
     * @param c1 Column in set to merge
     * @param c2 Column in set to merge
     */
    public void mergeSets(Column c1, Column c2) {
        Element d1 = find(c1);
        Element d2 = find(c2);
        if (d1 != null && d2 != null) {
            Element d3 = new Element(d1, d2);
            if (d1.lowerBoundExclusive() || d2.lowerBoundExclusive()) {
                d3.setLowerBoundExclusive(true);
            }
            if (d1.upperBoundExclusive() || d2.upperBoundExclusive()) {
                d3.setUpperBoundExclusive(true);
            }
            sets.add(d3);
            sets.remove(d1);
            sets.remove(d2);
        }
    }

    /**
     * Finds a disjoint set element holding this column
     *
     * @param column Column whose set we want to find
     * @return Disjoint set element holding this column
     */
    public Element find(Column column) {
        for (Element s : sets) {
            if (s.contains(column)) {
                return s;
            }
        }
        return null;
    }

    /**
     * @return Iterator over all disjoint set elements
     */
    public Iterator<Element> getIterator() {
        return sets.iterator();
    }

    /**
     * Finds a JSQL column corresponding to the input column name
     *
     * @param columnName Name of column we want
     * @return JSQL column referring to the input name
     */
    public Column getColumn(String columnName) {
        return columnMap.get(columnName);
    }

    public List<Element> setsWithTable(String tableName) {
        List<Element> setsWithTable = new ArrayList<>();
        for (Element element : sets) {
            if (element.containsTable(tableName)) {
                setsWithTable.add(element);
            }
        }
        String alias = DBCatalog.getInstance().getAliasForTable(tableName);
        if (alias != null) {
            for (Element element : sets) {
                if (element.containsTable(alias)) {
                    setsWithTable.add(element);
                }
            }
        }
        return setsWithTable;
    }

    /**
     * Determines if one disjoint set is equivalent to another
     *
     * @param o Other disjoint set
     * @return If this set is equivalent to the other
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DisjointSet)) return false;

        DisjointSet that = (DisjointSet) o;

        if (!sets.equals(that.sets)) return false;
        if (!columnMap.equals(that.columnMap)) return false;
        return tableMap.equals(that.tableMap);
    }
}
