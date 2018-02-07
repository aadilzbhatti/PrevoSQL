package com.prevosql.tuple.io.writer;

import com.prevosql.tuple.Tuple;

/**
 * Interface for dealing with tuple writers. All tuple writers
 * must have writeTuple(Tuple) and reset() functionality.
 */
public interface TupleWriter {
    /**
     * Writes a tuple to the output stream. Depending on
     * implementation, may write to binary file or to
     * plain text file
     *
     * @param t Tuple to write to file
     * @return Whether or not the tuple was successfully written.
     *         Returns false if tuple is null.
     */
    boolean writeTuple(Tuple t);

    /**
     * Resets output stream. Depending on implementation,
     * may not do anything.
     */
    void reset();

    /**
     * Flushes written tuples to disk. Depending on
     * implementation, may not do anything.
     */
    void flush();
}
