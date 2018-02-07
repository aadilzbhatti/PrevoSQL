package com.prevosql.operator.tuple;

import com.prevosql.tuple.Tuple;
import com.prevosql.tuple.io.reader.TupleReader;
import com.prevosql.tuple.io.reader.TupleReaderFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BinaryTupleReaderTest {
    private TupleReader reader;

    @Before
    public void setUp() {
        reader = TupleReaderFactory.getReader("src/test/resources/project3/input/db/data/Boats");
    }

    @Test
    public void testBasicRead() {
        Tuple t = reader.readNextTuple();
        Tuple exp = new Tuple("12,143,196");
        assertTrue(t.equals(exp));
    }

    @Test
    public void testReadEntireFile() {
        Tuple t;
        while ((t = reader.readNextTuple()) != null) {
            assertNotNull(t);
            assertTrue(t.length() == 3);
        }
    }
}