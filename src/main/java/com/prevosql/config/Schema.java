package com.prevosql.config;

import com.prevosql.config.catalog.DBCatalog;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * Initializes the schema based on the schema file and directories
 */
class Schema {
    private static Schema instance;
    private boolean initialized;

    /**
     * Singleton method to get global schema instance
     *
     * @return Schema instance
     */
    public static Schema getInstance() {
        if (instance == null) {
            instance = new Schema();
        }
        return instance;
    }

    /**
     * Initializes the schema manager
     *
     * @param schemaFilePath Schema file path
     * @param inputDirPath Input directory file path
     */
    void initializeSchema(String schemaFilePath, String inputDirPath) {
        if (!initialized) {
            try {
                BufferedReader fileReader = new BufferedReader(new FileReader(schemaFilePath));
                String line;
                while ((line = fileReader.readLine()) != null) {
                    String[] tokens = line.split(" ");
                    String tableName = tokens[0];
                    String tableFile = Paths.get(inputDirPath, "db", "data", tableName).toString();
                    DBCatalog.getInstance().setTable(tableName, tableFile, schemaFilePath);
                }
                initialized = true;
            } catch (IOException e) {
                System.err.println("Schema file " + schemaFilePath + " not found.");
            }
        }
    }
}
