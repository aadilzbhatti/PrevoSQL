package com.prevosql.interpreter;

import com.prevosql.TestCaseInitializer;
import com.prevosql.interpreter.query.Query;
import com.prevosql.tuple.util.TupleSorter;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class Project3QueryTest {
    private static final Logger LOG = Logger.getLogger(Project3QueryTest.class);
    private static final String OUT_FILE = "outfile";
    private static final String OUT_SORTED = "sorted";
    private static final String SORTED_CORRECT = "sorted_correct";
    private File outfile;
    private File sorted;
    private File sortedCorrect;

    @Before
    public void setUp() {
        TestCaseInitializer.initialize(3);
        outfile = new File(OUT_FILE);
        sorted = new File(OUT_SORTED);
        sortedCorrect = new File(SORTED_CORRECT);
    }

    @After
    public void tearDown() {
        outfile.delete();
        sorted.delete();
        sortedCorrect.delete();
    }

    @Test
    public void runQueries() throws Exception {
        Interpreter.main(new String[]{"src/test/resources/project3/input", "src/test/resources/project3/output", "src/test/resources/project3/temp"});
        String outPath = "src/test/resources/project3/output/query%d";
        String inPath = "src/test/resources/project3/expected/query%d";
        for (int i = 1; i <= 7; i++) {
            File infile = new File(String.format(inPath, i));
            File outfile = new File(String.format(outPath, i));
            try {
                assertEquals(FileUtils.readFileToString(infile, "utf-8"),
                             FileUtils.readFileToString(outfile, "utf-8"));
                LOG.debug("Test case " + i + " passed.");
            } catch (IOException e) {
                LOG.fatal(e);
            } catch (AssertionError e) {
                System.err.println("Test case " + i + " failed");
                LOG.fatal("Test case " + i + " failed");
            }
        }
    }

    @Test
    public void query1Test() throws IOException {
        Query q = new Query("SELECT * FROM Sailors;");
        q.invoke(OUT_FILE);
        assertTrue(FileUtils.contentEquals(outfile, new File("src/test/resources/project3/expected/query1")));
    }

    @Test
    public void query3Test() throws IOException {
        Query q = new Query("SELECT Boats.F, Boats.D FROM Boats;");
        q.invoke(OUT_FILE);
        assertTrue(FileUtils.contentEquals(outfile, new File("src/test/resources/project3/expected/query3")));
    }

    @Test
    public void query7Test() throws IOException {
        Query q = new Query("SELECT Sailors.A FROM Sailors WHERE Sailors.B >= Sailors.C AND Sailors.B < Sailors.C;");
        q.invoke(OUT_FILE);
        assertTrue(FileUtils.contentEquals(outfile, new File("src/test/resources/project3/expected/query7")));
    }

    @Test
    public void query8Test() throws Exception {
        Query q = new Query("SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;");
        q.invoke(OUT_FILE);
        assertTrue(TupleSorter.sortAndCompare(OUT_FILE, "src/test/resources/project3/expected/query8"));
    }

    @Test
    public void query9Test() throws IOException {
        Query q = new Query("SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Reserves.H = Boats.D;");
        q.invoke(OUT_FILE);
        assertTrue(TupleSorter.sortAndCompare(OUT_FILE, "src/test/resources/project3/expected/query9"));
    }

    @Test
    public void query11Test() throws IOException {
        Query q = new Query("SELECT DISTINCT * FROM Sailors");
        q.invoke(OUT_FILE);
        assertTrue(TupleSorter.sortAndCompare(OUT_FILE, "src/test/resources/project3/expected/query11"));
    }

    @Test
    public void query12Test() throws IOException {
        Query q = new Query("SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A < S2.A;");
        q.invoke(OUT_FILE);
        assertTrue(TupleSorter.sortAndCompare(OUT_FILE, "src/test/resources/project3/expected/query12"));
    }

    @Test
    public void query13Test() throws IOException {
        Query q = new Query("SELECT B.F, B.D FROM Boats B ORDER BY B.D;");
        q.invoke(OUT_FILE);
        assertTrue(TupleSorter.sortAndCompare(OUT_FILE, "src/test/resources/project3/expected/query13"));
    }

    @Test
    public void query14Test() throws IOException {
        Query q = new Query("SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C;");
        q.invoke(OUT_FILE);
        assertTrue(TupleSorter.sortAndCompare(OUT_FILE, "src/test/resources/project3/expected/query14"));
    }

    @Test
    public void query15Test() throws IOException {
        Query q = new Query("SELECT DISTINCT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C;");
        q.invoke(OUT_FILE);
        assertTrue(TupleSorter.sortAndCompare(OUT_FILE, "src/test/resources/project3/expected/query15"));
    }
}