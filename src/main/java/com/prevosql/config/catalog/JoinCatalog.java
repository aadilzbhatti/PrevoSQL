package com.prevosql.config.catalog;

import com.prevosql.operator.physical.binary.join.JoinPhysicalOperator;

import java.util.HashMap;

/**
 * Implements a catalog specifically for join operators,
 * which allows for joined catalog entries and specific operator
 * use instead of global use like the DBCatalog
 */
public class JoinCatalog extends Catalog {

    /**
     * Constructs a JoinCatalog from a DBCatalog
     *
     * @param catalog DBCatalog to base this JoinCatalog on
     */
    public JoinCatalog(DBCatalog catalog) {
        this.tableMap = new HashMap<>(catalog.tableMap);
    }

    /**
     * Constructs a JoinCatalog from another JoinCatalog
     *
     * @param jc JoinCatalog to base this JoinCatalog on
     */
    private JoinCatalog(JoinCatalog jc) {
        this.tableMap = new HashMap<>(jc.tableMap);
    }

    /**
     * Creates a joined logical table entry in the JoinCatalog for both
     * tables being joined (i.e. we store left+right and right+left)
     *
     * @param leftTable Name of left table to join
     * @param rightTable Name of right table to join
     */
    public void createJoinedTable(String leftTable, String rightTable) {
        String fullLeft, fullRight;
        if (DBCatalog.getInstance().getTableName(leftTable) != null && DBCatalog.getInstance().getTableName(rightTable) != null) {
            fullLeft = DBCatalog.getInstance().getTableName(leftTable);
            fullRight = DBCatalog.getInstance().getTableName(rightTable);

        } else if (DBCatalog.getInstance().getTableName(leftTable) != null) {
            fullLeft = DBCatalog.getInstance().getTableName(leftTable);
            fullRight = rightTable;

        } else if (DBCatalog.getInstance().getTableName(rightTable) != null) {
            fullLeft = leftTable;
            fullRight = DBCatalog.getInstance().getTableName(rightTable);

        } else {
            fullLeft = leftTable;
            fullRight = rightTable;
        }

        createJoinedTableHelper(fullLeft, fullRight);
        if (!fullLeft.equalsIgnoreCase(leftTable)) {
            createJoinedTableHelper(leftTable, rightTable);
        }
    }

    /**
     * Returns a logical table based on an input table name
     *
     * @param tableName Name of table to get
     * @return Logical table
     */
    @Override
    public Table getTable(String tableName) {
        return tableMap.get(tableName.toLowerCase());
    }

    /**
     * Joins two tables, both from different catalogs and adds them to a new com.cs5321.config.catalog
     *
     * @param leftCatalog Left com.cs5321.config.catalog
     * @param rightCatalog Right com.cs5321.config.catalog to get new table from
     * @param leftTable Left table name to join
     * @param rightTable Right table name to join
     * @return New JoinCatalog built from two input catalogs
     */
    public static JoinCatalog combineCatalogs(JoinCatalog leftCatalog, JoinCatalog rightCatalog, String leftTable, String rightTable) {
        JoinCatalog jc = new JoinCatalog(leftCatalog);
        Table oldRight = rightCatalog.getTable(rightTable);
        Table oldLeft = leftCatalog.getTable(rightTable);
        Table newLeft = Table.createJoinedTable(oldLeft, oldRight, true);
        Table newRight = Table.createJoinedTable(oldLeft, oldRight, false);
        jc.tableMap.put(leftTable.toLowerCase(), newLeft);
        jc.tableMap.put(rightTable.toLowerCase(), newRight);
        return jc;
    }

    /**
     * Joins the JoinPhysicalOperator's two tables with a new table, and builds a
     * new com.cs5321.config.catalog with those two tables in it
     *
     * @param catalog Input JoinCatalog
     * @param child JoinPhysicalOperator to get table names from
     * @param rightTable Table to join to the JoinPhysicalOperator's tables
     * @return New JoinCatalog with the two added tables
     */
    public static JoinCatalog addToCatalog(JoinCatalog catalog, JoinPhysicalOperator child, String rightTable) {
        Table oldLeftLeft = catalog.getTable(child.getLeftTableName());
        Table oldLeftRight = catalog.getTable(child.getRightTableName());
        Table oldRight = DBCatalog.getInstance().getTable(rightTable);
        Table newLeft = Table.createJoinedTable(oldLeftLeft, oldRight, true);
        Table newRight = Table.createJoinedTable(oldLeftRight, oldRight, true);
        JoinCatalog ret = new JoinCatalog(catalog);
        ret.tableMap.put(child.getLeftTableName().toLowerCase(), newLeft);
        ret.tableMap.put(child.getRightTableName().toLowerCase(), newRight);
        Table last = Table.createJoinedTable(oldLeftLeft, oldRight, false);
        ret.tableMap.put(rightTable.toLowerCase(), last);
        return ret;
    }

    /**
     * Joins two tables
     *
     * @param leftTable Name of left table to join
     * @param rightTable Name of right table to join
     */
    private void createJoinedTableHelper(String leftTable, String rightTable) {
        Table oldLeft = tableMap.get(leftTable.toLowerCase());
        Table oldRight = tableMap.get(rightTable.toLowerCase());
        Table newLeft = Table.createJoinedTable(oldLeft, oldRight, true);
        Table newRight = Table.createJoinedTable(oldLeft, oldRight, false);
        tableMap.put(leftTable.toLowerCase(), newLeft);
        tableMap.put(rightTable.toLowerCase(), newRight);
    }
}
