package com.prevosql.interpreter;

import com.prevosql.config.catalog.DBCatalog;
import com.prevosql.config.Configuration;
import com.prevosql.interpreter.query.Query;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Paths;
import java.util.Scanner;

/**
 * Main class which begins SQL interpretation. Can be run
 * with command line arguments to run query file, or can be run
 * as is and take lines from standard input
 */
class Interpreter {
    private static final Logger LOG = Logger.getLogger(Interpreter.class);

    /**
     * Main method for running interpreter
     *
     * @param args Command line arguments, if set, should
     *            have input and output directories
     */
    public static void main(String[] args) {
        if (args.length > 0) {
            String inputDir = null;
            String outputDir = null;
            String tempDir = null;
            int shouldBuildIndexes = -1;
            int shouldEvaluateWithIndexes = -1;

            if (args.length == 1) {
                try (BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
                    inputDir = br.readLine();
                    outputDir = br.readLine();
                    tempDir = br.readLine();
                    shouldBuildIndexes = Integer.parseInt(br.readLine());
                    shouldEvaluateWithIndexes = Integer.parseInt(br.readLine());

                } catch (Exception e) {
                    LOG.fatal(e);
                    System.err.println("Failed to read configuration file: " + e.getMessage());
                    System.exit(1);
                }

            } else {
                System.err.println("Usage: java -jar Project2.jar <path-to-config-file>");
                System.exit(1);
            }

            String inputDirPath = Paths.get(inputDir).toAbsolutePath().toString();
            String outputDirPath = Paths.get(outputDir).toAbsolutePath().toString();
            String tempDirPath = Paths.get(tempDir).toAbsolutePath().toString();
            String schemaFilePath = Paths.get(inputDirPath, "db", "schema.txt").toString();
            String queriesPath = Paths.get(inputDirPath, "queries.sql").toString();
            String configFilePath = Paths.get(inputDirPath, "plan_builder_config.txt").toString();

            Configuration.getInstance().initialize(schemaFilePath, inputDirPath, outputDirPath, configFilePath, tempDirPath, shouldBuildIndexes, shouldEvaluateWithIndexes);

            try {
                BufferedReader fileReader = new BufferedReader(new FileReader(queriesPath));
                String query;
                int queryNumber = 1;
                while ((query = fileReader.readLine()) != null) {
                    try {
                        LOG.debug("Query " + queryNumber + ": " + query);
                        System.err.println("Running query " + queryNumber + ": " + query);
                        Query q = new Query(query);
                        String outputFileName = "query" + queryNumber;
                        String outputFilePath = Paths.get(outputDirPath, outputFileName).toAbsolutePath().toString();
                        writeQueryPlans(q, queryNumber, outputDirPath);
                        q.invoke(outputFilePath);
                        queryNumber++;

                    } catch (Exception e) {
                        LOG.fatal(e);
                        e.printStackTrace();
                        System.err.println("A fatal error occurred while processing query " + queryNumber);
                        queryNumber++;
                    }
                }

                FileUtils.cleanDirectory(new File(tempDirPath));

            } catch (FileNotFoundException e) {
                LOG.fatal(e);
                System.err.println("Query file not found: " + queriesPath);
                System.exit(1);

            } catch (IOException e) {
                LOG.fatal(e);
                System.err.println("An error has occurred");
                System.exit(1);

            }

        } else {
            DBCatalog.getInstance().setTable("Boats", "src/test/resources/project2/boats", "src/test/resources/schema.txt");
            DBCatalog.getInstance().setTable("Reserves", "src/test/resources/project2/Reserves", "src/test/resources/schema.txt");
            DBCatalog.getInstance().setTable("Sailors", "src/test/resources/project2/Sailors", "src/test/resources/schema.txt");

            try {
                Scanner input = new Scanner(System.in);

                while (input.hasNext()) {
                    System.out.println("PrevoSQL> ");
                    String line = input.nextLine();
                    new Query(line).invoke();
                }
            } catch (Exception e) {
                LOG.fatal(e);
                System.err.println("Exception occurred during parsing. Invalid SQL entered: " + e.getMessage());
                System.exit(1);
            }
        }
    }

    private static void writeQueryPlans(Query q, int queryNumber, String outputDirPath) {
        String outputFileName = "query" + queryNumber;
        String logicalPlanOutputPath = Paths.get(outputDirPath, outputFileName + "_logicalplan").toAbsolutePath().toString();
        String physicalPlanOutputPath = Paths.get(outputDirPath, outputFileName + "_physicalplan").toAbsolutePath().toString();
        try (PrintWriter pw = new PrintWriter(new FileWriter(logicalPlanOutputPath))) {
            pw.println(q.getLogicalPlan());

        } catch (IOException e) {
            System.err.println("Failed to write logical query plan to disk: " + e.getMessage());
            LOG.fatal(e);
            System.exit(1);
        }

        try (PrintWriter pw = new PrintWriter(new FileWriter(physicalPlanOutputPath))) {
            pw.println(q.getPhysicalPlan());

        } catch (IOException e) {
            System.err.println("Failed to write physical query plan to disk: " + e.getMessage());
            LOG.fatal(e);
            System.exit(1);
        }
    }
}