package com.prevosql.index.io.writer;

/**
 * Factory to build IndexWriters
 */
public class IndexWriterFactory {
    /**
     * Builds an IndexWriter for table tableName on key key in
     * either binary or ASCII text
     *
     * @param tableName Name of table to build index writer for
     * @param key Search key to build index writer for
     * @param binary Whether or not the writer should use binary format or ASCII
     * @return An IndexWriter
     */
    public static IndexWriter getIndexWriter(String tableName, String key, boolean binary) {
        if (binary) {
            return new BinaryIndexWriter(tableName, key);
        } else {
            return new PlainIndexWriter(tableName, key);
        }
    }

    /**
     * Builds a binary index writer
     *
     * @param tableName Name of table to build writer for
     * @param key Search key to build writer for
     * @return A binary index writer
     */
    public static IndexWriter getIndexWriter(String tableName, String key) {
        return new BinaryIndexWriter(tableName, key);
    }
}
