package com.prevosql.operator.physical.unary.sort;

import com.prevosql.TestCaseInitializer;
import com.prevosql.operator.physical.leaf.ScanPhysicalOperator;
import com.prevosql.tuple.Tuple;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class InMemorySortPhysicalOperatorTest {
    @Before
    public void setUp() throws Exception {
       TestCaseInitializer.initialize(2);
    }

    @Test
    public void testBasicOrder() throws JSQLParserException {
        Statement stmt = CCJSqlParserUtil.parse("SELECT * FROM Sailors ORDER BY Sailors.B");
        PlainSelect ps = (PlainSelect) ((Select) stmt).getSelectBody();
        ScanPhysicalOperator so = new ScanPhysicalOperator("Sailors", false);
        InMemorySortPhysicalOperator sort = new InMemorySortPhysicalOperator(so, ps.getOrderByElements());
        Tuple t1 = new Tuple("3,100,105");
        Tuple t2 = new Tuple("4,100,50");
        Tuple t3 = new Tuple("5,100,500");
        Tuple t4 = new Tuple("1,200,50");
        Tuple t5 = new Tuple("2,200,200");
        Tuple t6 = new Tuple("6,300,400");
        Tuple res = sort.getNextTuple();
        assertTrue(res.equals(t1));
        res = sort.getNextTuple();
        assertTrue(res.equals(t2));
        res = sort.getNextTuple();
        assertTrue(res.equals(t3));
        res = sort.getNextTuple();
        assertTrue(res.equals(t4));
        res = sort.getNextTuple();
        assertTrue(res.equals(t5));
        res = sort.getNextTuple();
        assertTrue(res.equals(t6));
    }

    @Test
    public void testOrderByTwoFields() throws JSQLParserException {
        Statement stmt = CCJSqlParserUtil.parse("SELECT * FROM Sailors ORDER BY Sailors.B, Sailors.C");
        PlainSelect ps = (PlainSelect) ((Select) stmt).getSelectBody();
        ScanPhysicalOperator so = new ScanPhysicalOperator("Sailors", false);
        InMemorySortPhysicalOperator sort = new InMemorySortPhysicalOperator(so, ps.getOrderByElements());
        Tuple t1 = new Tuple("4,100,50");
        Tuple t2 = new Tuple("3,100,105");
        Tuple t3 = new Tuple("5,100,500");
        Tuple t4 = new Tuple("1,200,50");
        Tuple t5 = new Tuple("2,200,200");
        Tuple t6 = new Tuple("6,300,400");
        Tuple res = sort.getNextTuple();
        assertTrue(res.equals(t1));
        res = sort.getNextTuple();
        assertTrue(res.equals(t2));
        res = sort.getNextTuple();
        assertTrue(res.equals(t3));
        res = sort.getNextTuple();
        assertTrue(res.equals(t4));
        res = sort.getNextTuple();
        assertTrue(res.equals(t5));
        res = sort.getNextTuple();
        assertTrue(res.equals(t6));
    }
}