package com.prevosql.tuple.io.reader;

/**
 * Factory class for building TupleReader objects.
 * Used to hide implementations of TupleReaders
 */
public class TupleReaderFactory {
    /**
     * Private constructor so that we cannot construct TupleReaderFactorys
     * but only use them statically
     */
    private TupleReaderFactory() {
        throw new UnsupportedOperationException("TupleReaderFactory cannot be instantiated");
    }

    /**
     * Returns a TupleReader based on the input parameters
     *
     * @param filename File to read tuples from
     * @param binary Whether or not the file is a binary file
     * @return a TupleReader
     */
    public static TupleReader getReader(String filename, boolean binary) {
        if (binary) {
            return new BinaryTupleReader(filename);
        } else {
            return new PlainTupleReader(filename);
        }
    }

    /**
     * Automatically returns a binary TupleReader
     *
     * @param filename Input file to read tuples from (must be binary or this will break)
     * @return A binary TupleReader
     */
    public static TupleReader getReader(String filename) {
        return new BinaryTupleReader(filename);
    }
}
