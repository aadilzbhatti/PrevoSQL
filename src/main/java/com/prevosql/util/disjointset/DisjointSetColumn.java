package com.prevosql.util.disjointset;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * Models columns in disjoint sets so we don't have to
 * deal with JSQL and its columns that don't implement
 * .equals() or .hashcode()
 */
public class DisjointSetColumn {
    private String tableName;
    private String columnName;
    private String fullName;
    private boolean hasColumn;

    /**
     * Constructs a disjoint set column
     *
     * @param tableName Name of table
     * @param columnName Name of column
     */
    public DisjointSetColumn(String tableName, String columnName) {
        this.tableName = tableName;
        this.fullName = tableName;
        if (columnName == null) {
            this.hasColumn = false;
        } else {
            this.columnName = columnName;
            this.fullName += "." + columnName;
            this.hasColumn = true;
        }
    }

    /**
     * Constructs a disjoint set column from a JSQL column
     *
     * @param column Column to build disjoint set column from
     */
    public DisjointSetColumn(Column column) {
        this(column.getTable().getName(), column.getColumnName());
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public boolean isHasColumn() {
        return hasColumn;
    }

    /**
     * Implemented so we can use hashing
     *
     * @param o Other column to compare to
     * @return Whether this column is equivalent to the other
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DisjointSetColumn)) return false;

        DisjointSetColumn that = (DisjointSetColumn) o;

        if (hasColumn != that.hasColumn) return false;
        if (!tableName.equalsIgnoreCase(that.tableName)) return false;
        return columnName != null ? columnName.equalsIgnoreCase(that.columnName) : that.columnName == null;
    }

    /**
     * @return Full name of column, i.e. tableName.columnName
     */
    public String getFullName() {
        return fullName;
    }

    public String toString() {
        return getFullName();
    }

    /**
     * @return a JSQL column referring to this column
     */
    public Column getColumn() {
        Column c = new Column();
        c.setTable(new Table(tableName));
        c.setColumnName(columnName);
        return c;
    }

    /**
     * So we can use hashing
     *
     * @return The hash code
     */
    @Override
    public int hashCode() {
        int result = tableName.hashCode();
        result = 31 * result + (columnName != null ? columnName.hashCode() : 0);
        return result;
    }
}
