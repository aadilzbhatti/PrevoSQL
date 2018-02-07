package com.prevosql.config;

import com.prevosql.TestCaseInitializer;
import com.prevosql.config.statistics.StatsCollector;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class StatsCollectorTest {
    @Before
    public void setUp() {
        TestCaseInitializer.initializeProject4(0, 0);
    }

    @Test
    public void testStatCollector() {
        String statsFilePath = StatsCollector.getStats();
        String correctLine1 = "sailors 10000 A,1,10000 B,0,10000 C,2,9998";
        String correctLine2 = "boats 10000 D,1,10000 E,4,9999 F,0,10000";
        try (BufferedReader br = new BufferedReader(new FileReader(statsFilePath))) {
            String line = br.readLine();
            assertEquals(correctLine1, line);
            line = br.readLine();
            assertEquals(correctLine2, line);
            Files.delete(Paths.get(statsFilePath));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}