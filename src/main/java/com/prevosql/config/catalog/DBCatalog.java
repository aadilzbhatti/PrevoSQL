package com.prevosql.config.catalog;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

/**
 * Builds a catalog of table names to logical tables and columns for easy reference
 */
public class DBCatalog extends Catalog {
    private static final DBCatalog ourInstance = new DBCatalog();

    /**
     * Singleton method to return global instance of DBCatalog
     *
     * @return DBCatalog instance
     */
    public static DBCatalog getInstance() {
        return ourInstance;
    }

    /**
     * Constructs plain DBCatalog
     */
    private DBCatalog() {
        tableMap = new HashMap<>();
        aliasMap = new HashMap<>();
    }

    /**
     * Returns a logical table based on an input table name
     *
     * @param tableName Name of table to be returned
     * @return Logical table with name of input string
     */
    @Override
    public Table getTable(String tableName) {
        return tableMap.get(tableName.toLowerCase());
    }

    /**
     * Adds a logical table to the com.cs5321.config.catalog
     *
     * @param tableName Name of table to be added
     * @param fileName Filename referring to input data of table
     * @param schemaFile Schema file which determines table structure
     */
    public void setTable(String tableName, String fileName, String schemaFile) {
        LOG.info("Adding table " + tableName + " with filename " + fileName);
        Table t = new Table(tableName.toLowerCase(), fileName);
        try {
            BufferedReader fileReader = new BufferedReader(new FileReader(schemaFile));
            String line;
            while ((line = fileReader.readLine()) != null) {
                String[] tokens = line.split(" ");
                if (tokens[0].equalsIgnoreCase(tableName)) {
                    for (int i = 1; i < tokens.length; i++) {
                        LOG.debug("Setting column " + tokens[i] + " with index " + i);
                        t.setIndexForColumn(tokens[i], i - 1);
                    }

                    tableMap.put(tableName.toLowerCase(), t);
                    return;
                }
            }
        } catch (IOException e) {
            LOG.trace("Schema file " + schemaFile + " not found.", e);
            System.out.println("Schema file " + schemaFile + " not found.");
        }
    }

    /**
     * Copies a logical table into a new table with an alias
     * as the table name
     *
     * @param alias Alias to refer to existing table
     * @param tableName Existing table name
     */
    public void copyTable(String alias, String tableName) {
        LOG.info("Copying table " + tableName + " into alias " + alias);
        Table t = tableMap.get(tableName.toLowerCase());
        Table newTable = new Table(alias.toLowerCase(), t);
        newTable.setName(alias.toLowerCase());
        tableMap.put(alias.toLowerCase(), newTable);
        aliasMap.put(alias.toLowerCase(), tableName.toLowerCase());
    }

    /**
     * Returns the name of a table given an alias
     *
     * @param alias Input alias
     * @return Name of table corresponding to alias
     */
    public String getTableName(String alias) {
        String tableName;
        String key = alias.toLowerCase();
        if ((tableName = aliasMap.get(key)) != null) {
            return tableName;
        }
        return null;
    }

    /**
     * @return List of logical tables in the catalog
     */
    public Collection<Table> getTables() {
        return tableMap.values();
    }

    /**
     * Returns the alias associated with this table
     *
     * @param tableName Name of table to find alias for
     * @return Alias associated with this table
     */
    public String getAliasForTable(String tableName) {
        for (String alias : aliasMap.keySet()) {
            if (aliasMap.get(alias).equalsIgnoreCase(tableName)) {
                return alias;
            }
        }
        return null;
    }
}
