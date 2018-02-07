package com.prevosql.operator.physical;

import com.prevosql.TestCaseInitializer;
import com.prevosql.operator.physical.leaf.ScanPhysicalOperator;
import com.prevosql.operator.physical.unary.ProjectPhysicalOperator;
import com.prevosql.operator.physical.unary.SelectPhysicalOperator;
import com.prevosql.tuple.Tuple;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ProjectPhysicalOperatorTest {
    private ScanPhysicalOperator scan;

    @Before
    public void setUp() throws Exception {
        TestCaseInitializer.initialize(2);
        String tableName = "Boats";
        scan = new ScanPhysicalOperator(tableName, false);
    }

    @Test
    public void testAllTuplesOneColumn() throws JSQLParserException {
        ProjectPhysicalOperator po = parseAndReturnProjectOp("SELECT D FROM Boats;");

        Tuple t1 = new Tuple("101");
        Tuple t2 = new Tuple("102");
        Tuple t3 = new Tuple("104");
        Tuple t4 = new Tuple("103");
        Tuple t5 = new Tuple("107");

        Tuple res = po.getNextTuple();
        assertTrue(res.equals(t1));
        res = po.getNextTuple();
        assertTrue(res.equals(t2));
        res = po.getNextTuple();
        assertTrue(res.equals(t3));
        res = po.getNextTuple();
        assertTrue(res.equals(t4));
        res = po.getNextTuple();
        assertTrue(res.equals(t5));
        res = po.getNextTuple();
        assertNull(res);
    }

    @Test
    public void testDump() throws JSQLParserException {
        ProjectPhysicalOperator po = parseAndReturnProjectOp("SELECT D FROM Boats;");
        po.dump();
    }

    @Test
    public void testTwoColumns() throws JSQLParserException {
        ProjectPhysicalOperator po = parseAndReturnProjectOp("SELECT D, F FROM Boats;");

        Tuple t1 = new Tuple("101,3");
        Tuple t2 = new Tuple("102,4");
        Tuple t3 = new Tuple("104,2");
        Tuple res = po.getNextTuple();
        assertTrue(res.equals(t1));
        res = po.getNextTuple();
        assertTrue(res.equals(t2));
        res = po.getNextTuple();
        assertTrue(res.equals(t3));
    }

    @Test
    public void testTwoColumnsWithExpression() throws JSQLParserException {
        ProjectPhysicalOperator po = parseAndReturnProjectOp("SELECT D, F FROM Boats WHERE Boats.E=2");

        Tuple t1 = new Tuple("101,3");
        Tuple t2 = new Tuple("107,8");
        Tuple res = po.getNextTuple();
        assertTrue(res.equals(t1));
        res = po.getNextTuple();
        assertTrue(res.equals(t2));
        res = po.getNextTuple();
        assertNull(res);
    }

    private ProjectPhysicalOperator parseAndReturnProjectOp(String sql) throws JSQLParserException {
        Statement stmt = CCJSqlParserUtil.parse(sql);
        Select select = (Select) stmt;
        PlainSelect ps = (PlainSelect) select.getSelectBody();
        Expression e = ps.getWhere();
        List<SelectItem> selectItems = ps.getSelectItems();
        SelectPhysicalOperator so = new SelectPhysicalOperator(scan, e);
        return new ProjectPhysicalOperator(so, selectItems);
    }
}