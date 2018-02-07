package com.prevosql.tuple.io.writer;

public class TupleWriterFactory {
    private TupleWriterFactory() {
        throw new UnsupportedOperationException("TupleWriterFactory cannot be instantiated");
    }

    public static TupleWriter getWriter(String filename, int numAttributes) {
        if (numAttributes > 0) {
            return new BinaryTupleWriter(filename, numAttributes);
        } else {
            return new PlainTupleWriter(filename);
        }
    }
}
