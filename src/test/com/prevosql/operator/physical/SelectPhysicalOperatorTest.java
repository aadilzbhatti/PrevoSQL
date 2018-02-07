package com.prevosql.operator.physical;

import com.prevosql.TestCaseInitializer;
import com.prevosql.operator.physical.leaf.ScanPhysicalOperator;
import com.prevosql.operator.physical.unary.SelectPhysicalOperator;
import com.prevosql.tuple.Tuple;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SelectPhysicalOperatorTest {
    private ScanPhysicalOperator scan;

    @Before
    public void setUp() {
        TestCaseInitializer.initialize(2);
        String tableName = "Boats";
        scan = new ScanPhysicalOperator(tableName, false);
    }

    @Test
    public void testDump() {
        Expression e = parseAndReturnExp("SELECT * FROM Boats WHERE Boats.F=3");
        SelectPhysicalOperator s = new SelectPhysicalOperator(scan, e);
        s.dump();
    }

    @Test
    public void testSingleTupleReturnedOnly() {
        Expression e = parseAndReturnExp("SELECT * FROM Boats WHERE Boats.F=3");
        SelectPhysicalOperator s = new SelectPhysicalOperator(scan, e);
        Tuple t1 = new Tuple("101,2,3");
        Tuple res = s.getNextTuple();
        assertTrue(t1.equals(res));
        Tuple t2 = s.getNextTuple();
        assertNull(t2);
        Tuple t3 = s.getNextTuple();
        assertNull(t3);
        s.reset();
        s.dump();
    }

    @Test
    public void testTwoTuplesReturnedOnly() {
        Expression e = parseAndReturnExp("SELECT * FROM Boats WHERE Boats.E=2");
        SelectPhysicalOperator s = new SelectPhysicalOperator(scan, e);
        Tuple t1 = new Tuple("101,2,3");
        Tuple res = s.getNextTuple();
        assertTrue(t1.equals(res));
        Tuple t2 = new Tuple("107,2,8");
        res = s.getNextTuple();
        assertTrue(t2.equals(res));
        res = s.getNextTuple();
        assertNull(res);
        s.reset();
        s.dump();
    }

    @Test
    public void testNoTuplesReturned() {
        Expression e = parseAndReturnExp("SELECT * FROM Boats WHERE Boats.D=100");
        SelectPhysicalOperator s = new SelectPhysicalOperator(scan, e);
        Tuple res = s.getNextTuple();
        assertNull(res);
    }

    @Test
    public void testGreaterThanExp() {
        Expression e = parseAndReturnExp("SELECT * FROM Boats WHERE Boats.D > 103");
        SelectPhysicalOperator s = new SelectPhysicalOperator(scan, e);
        Tuple t1 = new Tuple("104,104,2");
        Tuple t2 = new Tuple("107,2,8");
        Tuple res = s.getNextTuple();
        assertTrue(res.equals(t1));
        res = s.getNextTuple();
        assertTrue(res.equals(t2));
        res = s.getNextTuple();
        assertNull(res);
    }

    @Test
    public void testGreaterThanEqualsToExp() {
        Expression e = parseAndReturnExp("SELECT * FROM Boats WHERE Boats.E >= 2");
        SelectPhysicalOperator s = new SelectPhysicalOperator(scan, e);
        Tuple t1 = new Tuple("101,2,3");
        Tuple t2 = new Tuple("102,3,4");
        Tuple t3 = new Tuple("104,104,2");
        Tuple t4 = new Tuple("107,2,8");
        Tuple res = s.getNextTuple();
        assertTrue(res.equals(t1));
        res = s.getNextTuple();
        assertTrue(res.equals(t2));
        res = s.getNextTuple();
        assertTrue(res.equals(t3));
        res = s.getNextTuple();
        assertTrue(res.equals(t4));
        res = s.getNextTuple();
        assertNull(res);
    }

    @Test
    public void testLessThanExp() {
        Expression e = parseAndReturnExp("SELECT * FROM Boats WHERE Boats.E < 2");
        SelectPhysicalOperator s = new SelectPhysicalOperator(scan, e);
        Tuple t1 = new Tuple("103,1,1");
        Tuple res = s.getNextTuple();
        assertTrue(res.equals(t1));
        res = s.getNextTuple();
        assertNull(res);
    }

    @Test
    public void testLessThanEqualToExp() {
        Expression e = parseAndReturnExp("SELECT * FROM Boats WHERE Boats.E <= 2");
        SelectPhysicalOperator s = new SelectPhysicalOperator(scan, e);
        Tuple t1 = new Tuple("101,2,3");
        Tuple t2 = new Tuple("103,1,1");
        Tuple t3 = new Tuple("107,2,8");
        Tuple res = s.getNextTuple();
        assertTrue(res.equals(t1));
        res = s.getNextTuple();
        assertTrue(res.equals(t2));
        res = s.getNextTuple();
        assertTrue(res.equals(t3));
        res = s.getNextTuple();
        assertNull(res);
    }

    @Test
    public void testAndExp() {
        Expression e = parseAndReturnExp("SELECT * FROM Boats WHERE Boats.E < 4 AND Boats.F > 3");
        SelectPhysicalOperator s = new SelectPhysicalOperator(scan, e);
        Tuple t1 = new Tuple("102,3,4");
        Tuple t2 = new Tuple("107,2,8");
        Tuple res = s.getNextTuple();
        assertTrue(res.equals(t1));
        res = s.getNextTuple();
        assertTrue(res.equals(t2));
        res = s.getNextTuple();
        assertNull(res);
    }

    @Test
    public void testBiggerAndExp() {
        Expression e = parseAndReturnExp("SELECT * FROM Boats WHERE Boats.E < 4 AND Boats.F > 3 AND Boats.D > 102");
        SelectPhysicalOperator s = new SelectPhysicalOperator(scan, e);
        Tuple t1 = new Tuple("107,2,8");
        Tuple res = s.getNextTuple();
        assertTrue(res.equals(t1));
        res = s.getNextTuple();
        assertNull(res);
    }

    @Test
    public void testNotEqualsExp() {
        Expression e = parseAndReturnExp("SELECT * FROM Boats WHERE Boats.E != 2");
        SelectPhysicalOperator s = new SelectPhysicalOperator(scan, e);
        Tuple t1 = new Tuple("102,3,4");
        Tuple t2 = new Tuple("104,104,2");
        Tuple t3 = new Tuple("103,1,1");
        Tuple res = s.getNextTuple();
        assertTrue(res.equals(t1));
        res = s.getNextTuple();
        assertTrue(res.equals(t2));
        res = s.getNextTuple();
        assertTrue(res.equals(t3));
        res = s.getNextTuple();
        assertNull(res);
    }

    private Expression parseAndReturnExp(String sql) {
        try {
            Statement stmt = CCJSqlParserUtil.parse(sql);
            Select select = (Select) stmt;
            PlainSelect ps = (PlainSelect) select.getSelectBody();
            return ps.getWhere();

        } catch (JSQLParserException e) {
            e.printStackTrace();
            return null;
        }
    }
}