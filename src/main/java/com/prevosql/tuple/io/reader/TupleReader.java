package com.prevosql.tuple.io.reader;

import com.prevosql.tuple.Tuple;

/**
 * Interface for defining tuple readers for use by
 * operators
 */
public interface TupleReader {
    /**
     * Reads in next tuple from input stream
     *
     * @return Input tuple
     */
    Tuple readNextTuple();

    /**
     * Resets input stream
     */
    void reset();

    int getNumAttributes();

    void reset(int index);

    int getCurrPage();

    int getNumTuples();
}
