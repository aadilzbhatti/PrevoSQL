package com.prevosql.operator.tuple;

import com.prevosql.tuple.Tuple;
import com.prevosql.tuple.io.reader.TupleReader;
import com.prevosql.tuple.io.reader.TupleReaderFactory;
import com.prevosql.tuple.util.TupleSorter;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class TupleSorterTest {
    private static final String IN_FILE = "src/test/resources/project3/input/db/data/Boats";
    private static final String OUT_FILE = "outfile";

    private static final Logger LOG = Logger.getLogger(TupleSorterTest.class);

    @After
    public void tearDown() {
        boolean deleted = (new File(OUT_FILE)).delete();
        if (!deleted) {
            LOG.fatal("Failed to delete test output file: " + OUT_FILE);
        }
    }

    @Test
    public void testSorter() {
        TupleSorter.sortFile(IN_FILE, OUT_FILE);
        List<Tuple> list = new ArrayList<>();
        TupleReader tr = TupleReaderFactory.getReader(OUT_FILE);
        Tuple t;
        while ((t = tr.readNextTuple()) != null) {
            list.add(t);
        }
        List<Tuple> tmp = new ArrayList<>(list);
        tmp.sort(Comparator.comparing(Tuple::toString));
        for (int i = 0; i < tmp.size(); i++) {
            assertTrue(tmp.get(i).equals(list.get(i)));
        }
    }
}