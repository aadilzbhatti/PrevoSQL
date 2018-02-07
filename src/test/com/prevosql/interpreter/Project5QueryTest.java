package com.prevosql.interpreter;

import com.prevosql.TestCaseInitializer;
import com.prevosql.interpreter.query.Query;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class Project5QueryTest {
    @Before
    public void setUp() {
        TestCaseInitializer.initializeProject4(0, 1);
    }

    @After
    public void tearDown() throws IOException {
        TestCaseInitializer.tearDownProject4();
    }

    @Test
    public void testNewJoin() {
        String query = "SELECT R.G, S.B FROM Reserves R, Sailors S, Boats B, Teams T WHERE R.G <> B.D AND R.G = S.B AND S.C = T.I AND R.G = 2 AND T.I = T.K AND B.E <> 42;";
        Query q = new Query(query);
        q.invoke();
    }

    @Test
    public void testNewJoin2() {
        Query.dumpLogicalPlan("SELECT * FROM R, S, T WHERE R.A > 10 AND R.A = S.B AND R.A <= 21");
    }

    @Test
    public void testNewJoin3() {
        Query.dumpLogicalPlan("SELECT * FROM R, S, T WHERE R.A < 100 AND R.A = R.B AND R.B = S.C AND S.C > 50 AND S.D = 42 AND S.D = T.F");
    }

    @Test
    public void testPrintPlan() {
        String query = "SELECT DISTINCT S.A, R.G FROM Sailors S, Reserves R, Boats B WHERE S.B = R.G AND S.A = B.D AND R.H <> B.D AND R.H < 100 ORDER BY S.A;";
        Query q = new Query(query);
        q.invoke();
    }

    @Test
    public void testBasicJoin() {
        String query = "SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.B = R.G AND S.A = B.D AND R.H <> B.D AND R.H < 100;";
        Query q = new Query(query);
        q.invoke();
    }
}
