package com.prevosql.index;

import com.prevosql.TestCaseInitializer;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class IndexTest {
    @Before
    public void setUp() throws IOException {
        TestCaseInitializer.initializeProject4(1, 1);
    }

    @After
    public void tearDown() {
        new File("Boats.E").delete();
    }

    @Test
    public void testBasic() throws IOException {
        Index index = new Index("Boats", "E", 10, false);
        assertTrue(FileUtils.contentEquals(new File("Boats.E"), new File("src/test/resources/project4/orig_indexes/Boats.E")));
    }

    @Test
    public void testClustered() {
        Index index = new Index("Boats", "E", 10, true);
    }
}