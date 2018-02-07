package com.prevosql.operator.tuple;

import com.prevosql.tuple.Tuple;
import com.prevosql.tuple.io.writer.TupleWriter;
import com.prevosql.tuple.io.writer.TupleWriterFactory;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Test;

import java.io.File;

public class BinaryTupleWriterTest {
    private static final String OUT_FILE = "outfile";
    private static final Logger LOG = Logger.getLogger(BinaryTupleWriterTest.class);

    @After
    public void tearDown() {
        boolean deleted = (new File(OUT_FILE)).delete();
        if (!deleted) {
            LOG.fatal("Failed to delete test output file: " + OUT_FILE);
        }
    }

    @Test
    public void testWriteOneLine() {
        Tuple t = new Tuple("1,2,3,4,5");
        TupleWriter tw = TupleWriterFactory.getWriter(OUT_FILE, 5);
        tw.writeTuple(t);
        tw.flush();
    }
}