package com.prevosql.operator.physical;

import com.prevosql.TestCaseInitializer;
import com.prevosql.operator.physical.leaf.ScanPhysicalOperator;
import com.prevosql.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ScanPhysicalOperatorTest {
    private ScanPhysicalOperator scan;

    @Before
    public void setUp() {
        TestCaseInitializer.initialize(2);
        String tableName = "Boats";
        scan = new ScanPhysicalOperator(tableName, false);
    }

	@Test
	public void testScanOperator() {
		String result = "101,2,3";
		Tuple t = scan.getNextTuple();
		Tuple other = new Tuple(result);
        assertTrue(t.equals(other));
	}

	@Test
    public void testDump() {
	    scan.dump();
    }

    @Test
    public void testReset() {
        Tuple result1 = new Tuple("101,2,3");
        Tuple result2 = new Tuple("102,3,4");
        Tuple result3 = new Tuple("104,104,2");
        Tuple t1 = scan.getNextTuple();
        Tuple t2 = scan.getNextTuple();
        Tuple t3 = scan.getNextTuple();
        assertTrue(t1.equals(result1));
        assertTrue(t2.equals(result2));
        assertTrue(t3.equals(result3));
        scan.reset();
        t1 = scan.getNextTuple();
        t2 = scan.getNextTuple();
        t3 = scan.getNextTuple();
        assertTrue(t1.equals(result1));
        assertTrue(t2.equals(result2));
        assertTrue(t3.equals(result3));
    }
}
