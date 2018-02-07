package com.prevosql.config.statistics;

import com.prevosql.config.catalog.DBCatalog;
import com.prevosql.config.catalog.Table;
import com.prevosql.config.Configuration;
import com.prevosql.operator.physical.leaf.ScanPhysicalOperator;
import com.prevosql.tuple.Tuple;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Collects statistics about relations in schema
 */
public class StatsCollector {
    private static final Logger LOG = Logger.getLogger(StatsCollector.class);

    private static String statsFilePath;
    private static Map<String, RelationStatistic> statistics;

    /**
     * Collects statistics about relations in schema and writes
     * them to $outputdir/stats.txt
     *
     * @return Path to statistics file
     */
    public static String getStats() {
        if (statsFilePath == null) {
            statistics = new HashMap<>();
            LOG.info("Collecting statistics on relations");
            String outputDirPath = Configuration.getInstance().getOutputDirPath();
            Path outputPath = Paths.get(outputDirPath, "stats.txt");
            statsFilePath = outputPath.toString();

            try (PrintWriter pw = new PrintWriter(new FileOutputStream(statsFilePath))) {
                for (Table t : DBCatalog.getInstance().getTables()) {
                    String stats = getStats(t);
                    LOG.debug(stats);
                    pw.println(stats);
                }
            } catch (FileNotFoundException e) {
                LOG.fatal("Could not find stats file", e);
            }
        }
        return statsFilePath;
    }

    /**
     * Gets a relation statistic corresponding to a particular
     * table
     *
     * @param tableName Name of table to get statistics for
     * @return Relation statistic about this table
     */
    public static RelationStatistic getStatistics(String tableName) {
        String fullName;
        if ((fullName = DBCatalog.getInstance().getTableName(tableName)) != null) {
            return statistics.get(fullName.toLowerCase());
        }
        return statistics.get(tableName.toLowerCase());
    }

    /**
     * Gets statistics about a particular logical table
     *
     * @param table Logical table to get stats for
     * @return String representation of statistics about table
     */
    private static String getStats(Table table) {
        StringBuilder sb = new StringBuilder(table.getName());
        HashMap<String, AttributeStatistic> attributeMap = new HashMap<>();
        for (String attribute : table.getAttributeList()) {
            attributeMap.put(attribute, new AttributeStatistic(Integer.MAX_VALUE, Integer.MIN_VALUE));
        }
        ScanPhysicalOperator so = new ScanPhysicalOperator(table.getName());
        Tuple t;
        int numTuples = 0;
        while ((t = so.getNextTuple()) != null) {
            for (String attribute : table.getAttributeList()) {
                int columnIndex = table.getIndexForColumn(attribute);
                int value = Integer.parseInt(t.get(columnIndex));
                AttributeStatistic stat = attributeMap.get(attribute);
                if (value < stat.getMinValue()) {
                    stat.setMinValue(value);
                }
                if (value > stat.getMaxValue()) {
                    stat.setMaxValue(value);
                }
                attributeMap.put(attribute, stat);
            }
            numTuples++;
        }

        RelationStatistic r = new RelationStatistic(numTuples, attributeMap);
        statistics.put(table.getName(), r);

        sb.append(" ").append(numTuples);
        for (String attribute : attributeMap.keySet()) {
            AttributeStatistic stat = attributeMap.get(attribute);
            sb.append(" ")
                    .append(attribute).append(",")
                    .append(stat.getMinValue()).append(",")
                    .append(stat.getMaxValue());
        }

        return sb.toString();
    }
}
