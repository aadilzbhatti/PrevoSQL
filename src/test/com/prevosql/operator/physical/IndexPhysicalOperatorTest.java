package com.prevosql.operator.physical;

import com.prevosql.TestCaseInitializer;
import com.prevosql.operator.physical.leaf.IndexScanPhysicalOperator;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class IndexPhysicalOperatorTest {
    private File unclusteredOut;
    private File clusteredOut;
    private static final String UNCLUSTERED = "unclustered";
    private static final String CLUSTERED = "clustered";

    @Before
    public void setUp() throws IOException {
        TestCaseInitializer.initializeProject4(0, 1);
        unclusteredOut = new File(UNCLUSTERED);
        clusteredOut = new File(CLUSTERED);
    }

    @After
    public void tearDown() {
        boolean res = unclusteredOut.delete();
        if (!res) {
            System.err.println("Failed to delete unclustered");
        }
        res = clusteredOut.delete();
        if (!res) {
            System.err.println("Failed to delete clustered");
        }
    }

    @Test
    public void testSameOutput() throws IOException {
        IndexScanPhysicalOperator opClustered = new IndexScanPhysicalOperator("Sailors", "A", 100, 200, true);
        opClustered.dump(CLUSTERED);

        IndexScanPhysicalOperator opUnclustered = new IndexScanPhysicalOperator("Sailors", "A", 100, 200, false);
        opUnclustered.dump(UNCLUSTERED);

        assertTrue(FileUtils.contentEquals(unclusteredOut, clusteredOut));
    }
}