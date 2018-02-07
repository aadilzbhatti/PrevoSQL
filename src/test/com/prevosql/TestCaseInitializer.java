package com.prevosql;

import com.prevosql.config.Configuration;
import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class TestCaseInitializer {
    private static String tempDirPath;

    public static void initialize(int projectName) {
        String schemaFile = "src/test/resources/schema.txt";
        String inputDirPath = "src/test/resources/project%d/input/";
        String configFilePath = "src/test/resources/project%d/input/plan_builder_config.txt";
        String tempDirPath = "src/test/resources/project%d/temp";
        String outputDirPath = "src/test/resources/project%d/output";

        Configuration.getInstance().initialize(
                schemaFile,
                String.format(inputDirPath, projectName),
                String.format(outputDirPath, projectName),
                String.format(configFilePath, projectName),
                String.format(tempDirPath, projectName),
                0,
                0);
    }

    public static void initializeProject4(int shouldBuildIndexes, int shouldEvaluateWithIndexes) {
        String configFile = "src/test/resources/project4/interpreter_config_file.txt";
        try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
            String inputDirPath = br.readLine();
            String outputDirPath = br.readLine();
            tempDirPath = br.readLine();
            String configFilePath = "src/test/resources/project4/input/plan_builder_config.txt";
            String schemaFile = "src/test/resources/project4/input/db/schema.txt";

            Configuration.getInstance().initialize(schemaFile, inputDirPath, outputDirPath, configFilePath,
                    tempDirPath, shouldBuildIndexes, shouldEvaluateWithIndexes);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void tearDownProject4() throws IOException {
        FileUtils.cleanDirectory(new File(tempDirPath));
    }
}
