package com.prevosql.operator.physical;

import com.prevosql.TestCaseInitializer;
import com.prevosql.operator.physical.binary.join.JoinPhysicalOperator;
import com.prevosql.operator.physical.binary.join.TupleNestedLoopJoinPhysicalOperator;
import com.prevosql.operator.physical.leaf.ScanPhysicalOperator;
import com.prevosql.operator.physical.unary.ProjectPhysicalOperator;
import com.prevosql.tuple.Tuple;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TupleNestedLoopJoinPhysicalOperatorTest {
    private static final Logger LOG = Logger.getLogger(TupleNestedLoopJoinPhysicalOperatorTest.class);

    @Before
    public void setUp() {
        TestCaseInitializer.initialize(2);
    }

    @Test
    public void testBasicJoin() {
        String query = "SELECT * FROM Sailors, Reserves;";
        LOG.info("Testing query: " + query);
        try {
            Statement stmt = CCJSqlParserUtil.parse(query);
            Select select = (Select) stmt;
            PlainSelect ps = (PlainSelect) select.getSelectBody();
            String leftTable = ps.getFromItem().toString();
            List<Join> joins = ps.getJoins();
            String rightTable = joins.get(0).toString();

            ScanPhysicalOperator ls = new ScanPhysicalOperator(leftTable, false);
            ScanPhysicalOperator rs = new ScanPhysicalOperator(rightTable, false);
            TupleNestedLoopJoinPhysicalOperator jo = new TupleNestedLoopJoinPhysicalOperator(ls, rs, null);

            Tuple t = new Tuple("1,200,50,1,101");
            Tuple res = jo.getNextTuple();
            assertTrue(res.equals(t));

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testJoinWithCondition() {
        String query = "SELECT * FROM Sailors, Reserves WHERE Sailors.A=Reserves.G;";
        LOG.info("Testing query " + query);
        try {
            Statement stmt = CCJSqlParserUtil.parse(query);
            Select select = (Select) stmt;
            PlainSelect ps = (PlainSelect) select.getSelectBody();
            String leftTable = ps.getFromItem().toString();
            List<Join> joins = ps.getJoins();
            String rightTable = joins.get(0).toString();

            ScanPhysicalOperator ls = new ScanPhysicalOperator(leftTable, false);
            ScanPhysicalOperator rs = new ScanPhysicalOperator(rightTable, false);
            TupleNestedLoopJoinPhysicalOperator jo = new TupleNestedLoopJoinPhysicalOperator(ls, rs, ps.getWhere());

            Tuple t1 = new Tuple("1,200,50,1,101");
            Tuple t2 = new Tuple("1,200,50,1,102");
            Tuple t3 = new Tuple("1,200,50,1,103");
            Tuple t4 = new Tuple("2,200,200,2,101");
            Tuple t5 = new Tuple("3,100,105,3,102");
            Tuple t6 = new Tuple("4,100,50,4,104");

            Tuple res = jo.getNextTuple();
            assertTrue(res.equals(t1));
            res = jo.getNextTuple();
            assertTrue(res.equals(t2));
            res = jo.getNextTuple();
            assertTrue(res.equals(t3));
            res = jo.getNextTuple();
            assertTrue(res.equals(t4));
            res = jo.getNextTuple();
            assertTrue(res.equals(t5));
            res = jo.getNextTuple();
            assertTrue(res.equals(t6));
            res = jo.getNextTuple();
            assertNull(res);

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testJoinWithProject() {
        String query = "SELECT B, H from Sailors, Reserves WHERE Sailors.A=Reserves.G;";
        LOG.info("Testing query: " + query);
        try {
            Statement stmt = CCJSqlParserUtil.parse(query);
            Select select = (Select) stmt;
            PlainSelect ps = (PlainSelect) select.getSelectBody();
            String leftTable = ps.getFromItem().toString();
            List<Join> joins = ps.getJoins();
            String rightTable = joins.get(0).toString();
            List<SelectItem> selectItems = ps.getSelectItems();

            ScanPhysicalOperator ls = new ScanPhysicalOperator(leftTable, false);
            ScanPhysicalOperator rs = new ScanPhysicalOperator(rightTable, false);
            TupleNestedLoopJoinPhysicalOperator jo = new TupleNestedLoopJoinPhysicalOperator(ls, rs, ps.getWhere());
            ProjectPhysicalOperator po = new ProjectPhysicalOperator(jo, selectItems);
            po.dump();

        } catch (JSQLParserException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testJoinWithOnExp() {
        String query = "SELECT * FROM Sailors INNER JOIN Reserves ON Sailors.A=Reserves.G";
        LOG.info("Testing query: " + query);
        try {
            Statement stmt = CCJSqlParserUtil.parse(query);
            Select select = (Select) stmt;
            PlainSelect ps = (PlainSelect) select.getSelectBody();
            List<Join> joins = ps.getJoins();
            String rightTable = joins.get(0).getRightItem().toString();
            String leftTable = ps.getFromItem().toString();
            Expression onExp = joins.get(0).getOnExpression();
            ScanPhysicalOperator ls = new ScanPhysicalOperator(leftTable, false);
            ScanPhysicalOperator rs = new ScanPhysicalOperator(rightTable, false);
            TupleNestedLoopJoinPhysicalOperator jo = new TupleNestedLoopJoinPhysicalOperator(ls, rs, onExp);
            jo.dump();

        } catch (JSQLParserException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void tripleJoin() {
        String query = "SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Reserves.H = Boats.D;";
        LOG.info("Testing query: " + query);
        try {
            Statement stmt = CCJSqlParserUtil.parse(query);
            Select select = (Select) stmt;
            PlainSelect ps = (PlainSelect) select.getSelectBody();
            List<Join> joins = ps.getJoins();
            String secondTable = joins.get(0).getRightItem().toString();
            String thirdTable = joins.get(1).getRightItem().toString();
            String leftTable = ps.getFromItem().toString();

            ScanPhysicalOperator boats = new ScanPhysicalOperator(thirdTable, false);
            ScanPhysicalOperator reserves = new ScanPhysicalOperator(secondTable, false);
            ScanPhysicalOperator sailors = new ScanPhysicalOperator(leftTable, false);
            Expression leftExp = joins.get(0).getOnExpression();
            Expression rightExp = joins.get(1).getOnExpression();
            JoinPhysicalOperator jo1 = new TupleNestedLoopJoinPhysicalOperator(sailors, reserves, leftExp);
            JoinPhysicalOperator root = new TupleNestedLoopJoinPhysicalOperator(jo1, boats, rightExp);
            root.dump();

        } catch (JSQLParserException e) {
            e.printStackTrace();
            fail();
        }
    }
}