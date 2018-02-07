package com.prevosql.operator.tuple;

import com.prevosql.tuple.Tuple;
import com.prevosql.tuple.io.reader.TupleReader;
import com.prevosql.tuple.io.reader.TupleReaderFactory;
import com.prevosql.tuple.io.writer.TupleWriter;
import com.prevosql.tuple.io.writer.TupleWriterFactory;
import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TupleIOIntegrationTest {
    private static final String IN_FILE = "src/test/resources/project3/input/db/data/Sailors";
    private static final String OUT_FILE = "outfile";
    private static final Logger LOG = Logger.getLogger(TupleIOIntegrationTest.class);

//    @After
//    public void tearDown() {
//        boolean deleted = (new File(OUT_FILE)).delete();
//        if (!deleted) {
//            LOG.fatal("Failed to delete test output file: " + OUT_FILE);
//        }
//    }

    @Test
    public void testWriteAndReadOneLine() {
        Tuple t = new Tuple("1,2,3,4,5");
        TupleWriter tw = TupleWriterFactory.getWriter(OUT_FILE, 5);
        tw.writeTuple(t);
        tw.flush();
        TupleReader tr = TupleReaderFactory.getReader(OUT_FILE);
        Tuple res = tr.readNextTuple();
        try {
            assertTrue(res.equals(t));
        } catch (AssertionError e) {
            System.out.println(res);
            System.out.println(t);
        }
    }

    @Test
    public void testReadAndWriteLargeFile() {
        TupleReader tr = TupleReaderFactory.getReader(IN_FILE);
        TupleWriter tw = TupleWriterFactory.getWriter(OUT_FILE, 3);
        Tuple t;
        while ((t = tr.readNextTuple()) != null) {
            LOG.info(t);
            tw.writeTuple(t);
        }
        tw.flush();

        LOG.info("Read entire data file and wrote to disk");

        TupleReader tr2 = TupleReaderFactory.getReader(IN_FILE);
        TupleReader res = TupleReaderFactory.getReader(OUT_FILE);
        Tuple t1, t2;
        t1 = tr2.readNextTuple();
        t2 = res.readNextTuple();
        try {
            while (t1 != null || t2 != null) {
                assertTrue(t1.equals(t2));
                t1 = tr2.readNextTuple();
                t2 = res.readNextTuple();
            }
        } catch (AssertionError e) {
            System.out.println(t1);
            System.out.println(t2);
            fail();

        } catch (NullPointerException e) {
            fail();
        }
    }
}
