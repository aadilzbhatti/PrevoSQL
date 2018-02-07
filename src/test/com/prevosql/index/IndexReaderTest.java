package com.prevosql.index;

import com.prevosql.index.io.reader.BinaryIndexReader;
import org.junit.Test;

public class IndexReaderTest {

    @Test
    public void test() {
        BinaryIndexReader ir = new BinaryIndexReader("src/test/resources/project4/orig_indexes/Boats.E");
        for (int i = 50; i <= 150; i++) {
            ir.findKey(i);
        }
    }
}