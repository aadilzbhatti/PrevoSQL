package com.prevosql.util.disjointset;

import net.sf.jsqlparser.schema.Column;

import java.util.*;

/**
 * Disjoint set elements
 */
public class Element {
    private Map<DisjointSetColumn, Boolean> set;
    private int lowerBound = Integer.MIN_VALUE;
    private int upperBound = Integer.MAX_VALUE;
    private int equalityConstraint = -1;
    private boolean hasEqualityConstraint = false;
    private boolean hasUpperBound = false;
    private boolean hasLowerBound = false;
    private boolean upperBoundExclusive = false;
    private boolean lowerBoundExclusive = false;

    /**
     * Builds a disjoint set element
     *
     * @param column Column to build the element from
     */
    public Element(Column column) {
        this.set = new HashMap<>();
        this.set.put(new DisjointSetColumn(column), true);
    }

    /**
     * Builds an element from two other elements
     *
     * @param s1 First element
     * @param s2 Second element
     */
    public Element(Element s1, Element s2) {
        set = new HashMap<>(s1.set);
        set.putAll(s2.set);
        if (s1.lowerBound > lowerBound) {
            setLowerBound(s1.lowerBound);
        }
        if (s2.lowerBound > lowerBound) {
            setLowerBound(s2.lowerBound);
        }
        if (s1.upperBound < upperBound) {
            setUpperBound(s1.upperBound);
        }
        if (s2.upperBound < upperBound) {
            setUpperBound(s2.upperBound);
        }
        if (s1.equalityConstraint != -1) {
            setEqualityConstraint(s1.equalityConstraint);
        }
        if (s2.equalityConstraint != -1) {
            setEqualityConstraint(s2.equalityConstraint);
        }
    }

    /**
     * Checks if this element contains a JSQL column
     *
     * @param column Column which may or may not be in this element
     * @return Whether or not this element contains the column
     */
    public boolean contains(Column column) {
        DisjointSetColumn key = new DisjointSetColumn(column);
        if (set.get(key) == null) {
            return false;
        }
        return set.get(key);
    }

    /**
     * Checks if this element contains a table with this name
     *
     * @param tableName Name of table which might be in element
     * @return Whether or not this element contains the table
     */
    public boolean containsTable(String tableName) {
        for (DisjointSetColumn col : set.keySet()) {
            String name = col.getTableName();
            if (name.equalsIgnoreCase(tableName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return Lower bound of the element
     */
    public int getLowerBound() {
        return lowerBound;
    }

    /**
     * Sets lower bound of the element
     *
     * @param lowerBound Value to set lower bound to
     */
    public void setLowerBound(int lowerBound) {
        this.lowerBound = lowerBound;
        this.hasLowerBound = true;
    }

    public int getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(int upperBound) {
        this.upperBound = upperBound;
        this.hasUpperBound = true;
    }

    public int getEqualityConstraint() {
        return equalityConstraint;
    }

    public void setEqualityConstraint(int equalityConstraint) {
        this.equalityConstraint = equalityConstraint;
        this.hasEqualityConstraint = true;
    }

    public boolean hasEqualityConstraint() {
        return hasEqualityConstraint;
    }

    public boolean hasUpperBound() {
        return hasUpperBound;
    }

    public boolean hasLowerBound() {
        return hasLowerBound;
    }

    public boolean upperBoundExclusive() {
        return upperBoundExclusive;
    }

    public void setUpperBoundExclusive(boolean upperBoundExclusive) {
        this.upperBoundExclusive = upperBoundExclusive;
    }

    public boolean lowerBoundExclusive() {
        return lowerBoundExclusive;
    }

    public void setLowerBoundExclusive(boolean lowerBoundExclusive) {
        this.lowerBoundExclusive = lowerBoundExclusive;
    }

    public Iterator<DisjointSetColumn> getIterator() {
        return set.keySet().iterator();
    }

    public Set<DisjointSetColumn> getColumns() {
        return set.keySet();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Element)) return false;

        Element that = (Element) o;

        if (lowerBound != that.lowerBound) return false;
        if (upperBound != that.upperBound) return false;
        if (equalityConstraint != that.equalityConstraint) return false;
        if (hasEqualityConstraint != that.hasEqualityConstraint) return false;
        if (hasUpperBound != that.hasUpperBound) return false;
        if (hasLowerBound != that.hasLowerBound) return false;
        if (upperBoundExclusive != that.upperBoundExclusive) return false;
        if (lowerBoundExclusive != that.lowerBoundExclusive) return false;
        return set.equals(that.set);
    }
}
