package com.prevosql.interpreter;

import com.prevosql.TestCaseInitializer;
import com.prevosql.interpreter.query.Query;
import net.sf.jsqlparser.JSQLParserException;
import org.junit.Before;
import org.junit.Test;

public class Project2QueryTest {

    @Before
    public void setUp() {
        TestCaseInitializer.initialize(2);
    }

    @Test
    public void complexQueryTest() throws JSQLParserException {
        String query = "SELECT * FROM R, S, T WHERE R.A = 1 AND R.B = S.C AND T.G < 5 AND T.G = S.H;";
        Query q = new Query(query);
        System.out.println(q);
    }

    @Test
    public void testCrossProduct() throws JSQLParserException {
        String query = "SELECT * FROM R, S, T;";
        Query q = new Query(query);
        System.out.println(q);
    }

    @Test
    public void testAlias() throws JSQLParserException {
        String query = "SELECT S.A FROM Sailors S;";
        Query q = new Query(query);
        q.invoke();
    }

    @Test
    public void testAliasWithJoin() throws JSQLParserException {
        String query = "SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A < S2.A;";
        Query q = new Query(query);
        q.invoke();
    }

    @Test
    public void testSelfCrossProduct() throws JSQLParserException {
        String query = "SELECT * FROM Sailors S1, Sailors S2;";
        Query q = new Query(query);
        q.invoke();
    }

    @Test
    public void testBasicJoin() throws JSQLParserException {
        String query = "SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;";
        Query q = new Query(query);
        q.invoke();
    }

    @Test
    public void testBasicScan() throws JSQLParserException {
        String query = "SELECT * FROM Sailors;";
        Query q = new Query(query);
        q.invoke();
    }

    @Test
    public void testBasicSelect() throws JSQLParserException {
        String query = "SELECT Sailors.A FROM Sailors;";
        Query q = new Query(query);
        q.invoke();
    }

    @Test
    public void testOrderBy() throws JSQLParserException {
        String query = "SELECT * FROM Sailors ORDER BY Sailors.B;";
        Query q = new Query(query);
        q.invoke();
    }

    @Test
    public void testDistinct() throws JSQLParserException {
        String query = "SELECT DISTINCT R.G FROM Reserves R;";
        Query q = new Query(query);
        q.invoke();
    }
}