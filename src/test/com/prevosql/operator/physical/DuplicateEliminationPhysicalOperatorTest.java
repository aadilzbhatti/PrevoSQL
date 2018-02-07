package com.prevosql.operator.physical;

import com.prevosql.TestCaseInitializer;
import com.prevosql.operator.physical.leaf.ScanPhysicalOperator;
import com.prevosql.operator.physical.unary.DuplicateEliminationPhysicalOperator;
import com.prevosql.operator.physical.unary.ProjectPhysicalOperator;
import com.prevosql.tuple.Tuple;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DuplicateEliminationPhysicalOperatorTest {
    @Before
    public void setUp() {
        TestCaseInitializer.initialize(2);
    }

    @Test
    public void testBasicDistinct() throws JSQLParserException {
        String query = "SELECT DISTINCT G FROM Reserves;";
        Statement stmt = CCJSqlParserUtil.parse(query);
        Select select = (Select) stmt;
        PlainSelect ps = (PlainSelect) select.getSelectBody();
        ScanPhysicalOperator so = new ScanPhysicalOperator(ps.getFromItem().toString(), false);
        ProjectPhysicalOperator po = new ProjectPhysicalOperator(so, ps.getSelectItems());
        DuplicateEliminationPhysicalOperator d = new DuplicateEliminationPhysicalOperator(po);
        Tuple t1 = new Tuple("1");
        Tuple t2 = new Tuple("2");
        Tuple t3 = new Tuple("3");
        Tuple t4 = new Tuple("4");
        Tuple res = d.getNextTuple();
        assertTrue(res.equals(t1));
        res = d.getNextTuple();
        assertTrue(res.equals(t2));
        res = d.getNextTuple();
        assertTrue(res.equals(t3));
        res = d.getNextTuple();
        assertTrue(res.equals(t4));
        res = d.getNextTuple();
        assertNull(res);
    }
}