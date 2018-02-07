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

public class ExternalSortPhysicalOperatorTest {

    @Before
    public void setUp() {
        TestCaseInitializer.initialize(3);
    }

    @Test
    public void basicTest() throws JSQLParserException {
        ScanPhysicalOperator scan = new ScanPhysicalOperator("Sailors");
        Statement stmt = CCJSqlParserUtil.parse("SELECT * FROM Sailors ORDER BY Sailors.B");
        Select select = (Select) stmt;
        PlainSelect ps = (PlainSelect) select.getSelectBody();
        ExternalSortPhysicalOperator exo = new ExternalSortPhysicalOperator(scan, ps.getOrderByElements(), 1, false);
        Tuple t = exo.getNextTuple();
        System.out.println(t);
    }
}