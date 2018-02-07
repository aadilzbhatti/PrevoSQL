package com.prevosql.config.index;

import com.prevosql.index.Index;
import com.prevosql.config.Configuration;
import javafx.util.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to get information from index configuration
 */
public class IndexConfigFileParser {
    private boolean shouldEvaluateWithIndexes;
    private Map<Pair<String, String>, IndexInfo> indexes;
    private boolean initialized = false;

    private static IndexConfigFileParser instance;

    /**
     * @return Global index config instance
     */
    public static IndexConfigFileParser getInstance() {
        if (instance == null) {
            instance = new IndexConfigFileParser();
        }
        return instance;
    }

    /**
     * Initializes the index config
     *
     * @param shouldBuild Whether or not we should build indexes
     * @param shouldEvaluateWithIndexes Whether or not we should evaluate with indexes
     */
    public void initialize(int shouldBuild, int shouldEvaluateWithIndexes) {
        if (!initialized) {
            boolean shouldBuildIndexes = shouldBuild == 1;
            this.shouldEvaluateWithIndexes = shouldEvaluateWithIndexes == 1;
            indexes = new HashMap<>();

            String indexInfoPath = Paths.get(Configuration.getInstance().getInputDirPath(), "db", "index_info.txt").toString();
            try (BufferedReader br = new BufferedReader(new FileReader(indexInfoPath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] tokens = line.split("\\s+");
                    if (tokens.length != 4) {
                        System.err.println("Invalid com.cs5321.index info file: Invalid number of arguments: " + line);
                        System.exit(1);
                    }
                    String tableName = tokens[0];
                    String searchKey = tokens[1];
                    boolean isClustered = Integer.parseInt(tokens[2]) == 1;
                    int order = Integer.parseInt(tokens[3]);

                    if (shouldBuildIndexes) {
                        Index i = new Index(tableName, searchKey, order, isClustered);
                        indexes.put(new Pair<>(tableName, searchKey), new IndexInfo(tableName, searchKey, i.getNumLeafNodes(), isClustered));
                    }
                }

            } catch (Exception e) {
                System.err.println("Failed to read com.cs5321.index info file: " + e.getMessage());
                System.exit(1);
            }

            initialized = true;
        }
    }

    /**
     * Returns index information about the index corresponding to the
     * given table name and search key
     *
     * @param tableName Name of table index is built on
     * @param searchKey Search key index is build with
     * @return IndexInfo about this index
     */
    public IndexInfo getIndex(String tableName, String searchKey) {
        if (indexes.get(new Pair<>(tableName, searchKey)) == null) {
            return null;
        }
        return indexes.get(new Pair<>(tableName, searchKey));
    }
}
