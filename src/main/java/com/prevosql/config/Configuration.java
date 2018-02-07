package com.prevosql.config;

import com.prevosql.config.index.IndexConfigFileParser;
import com.prevosql.config.operator.PhysicalConfigParser;
import com.prevosql.config.statistics.RelationStatistic;
import com.prevosql.config.statistics.StatsCollector;

/**
 * Models configuration settings for the DBMS
 */
public class Configuration {
    private static Configuration instance;
    private boolean initialized = false;
    private String tempDirPath;
    private String inputDirPath;
    private String statsFilePath;
    private String outputDirPath;

    /**
     * @return Global Configuration instance
     */
    public static Configuration getInstance() {
        if (instance == null) {
            instance = new Configuration();
        }
        return instance;
    }

    /**
     * Initializes the Configuration based on paths to the schema,
     * input directory, output directory, and temp directory
     *  @param schemaFilePath Path to schema file
     * @param inputDirPath Path to input directory
     * @param outputDirPath Path to output directory
     * @param configFilePath Path to output directory
     * @param tempDirPath Path to temp directory
     */
    public void initialize(String schemaFilePath,
                           String inputDirPath,
                           String outputDirPath, String configFilePath,
                           String tempDirPath,
                           int shouldBuildIndexes,
                           int shouldEvaluateWithIndexes) {
        if (!initialized) {
            this.tempDirPath = tempDirPath;
            this.inputDirPath = inputDirPath;
            this.outputDirPath = outputDirPath;
            Schema.getInstance().initializeSchema(schemaFilePath, inputDirPath);
            IndexConfigFileParser.getInstance().initialize(shouldBuildIndexes, shouldEvaluateWithIndexes);
            this.statsFilePath = StatsCollector.getStats();
            this.initialized = true;
        }
    }

    /**
     * @return Global ConfigFileParser instance
     */
    public static PhysicalConfigParser getPhysicalConfig() {
        return PhysicalConfigParser.getInstance();
    }

    public static IndexConfigFileParser getIndexConfig() {
        return IndexConfigFileParser.getInstance();
    }

    /**
     * @return Path to temp directory
     */
    public String getTempDirPath() {
        return tempDirPath;
    }

    /**
     * @return Path to input directory
     */
    public String getInputDirPath() {
        return inputDirPath;
    }

    public String getOutputDirPath() {
        return outputDirPath;
    }

    public String getStatsFilePath() {
        return statsFilePath;
    }

    public RelationStatistic getStatistics(String tableName) {
        return StatsCollector.getStatistics(tableName);
    }
}
