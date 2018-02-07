package com.prevosql.interpreter;

import com.prevosql.TestCaseInitializer;
import com.prevosql.interpreter.query.Query;
import org.junit.Before;
import org.junit.Test;

public class Project4QueryTest {
    @Before
    public void setUp() {
        TestCaseInitializer.initializeProject4(1, 1);
    }

    @Test
    public void testOneConditionSatisfiesIndex() {
        Query q = new Query("SELECT * FROM Boats WHERE Boats.E > 100");
        q.invoke();
    }

    @Test
    public void testTwoConditionsSatisfyIndex() {
        Query q = new Query("SELECT * FROM Boats WHERE Boats.E > 100 AND Boats.E < 200");
        q.invoke();
    }

    @Test
    public void testTwoConditionsSatisfyIndexAndOneWithout() {
        Query q = new Query("SELECT * FROM Boats WHERE Boats.E > 100 AND Boats.E < 200 AND Boats.D < 1000");
        q.invoke();
    }

    @Test
    public void testTwoInclusiveSatisfyIndex() {
        Query q = new Query("SELECT * FROM Boats WHERE Boats.E >= 100 AND Boats.E <= 200");
        q.invoke();
    }

//    @Test
//    public void test
}
