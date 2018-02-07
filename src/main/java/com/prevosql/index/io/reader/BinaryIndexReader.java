package com.prevosql.index.io.reader;

import com.prevosql.config.catalog.DBCatalog;
import com.prevosql.index.entry.DataEntry;
import com.prevosql.index.entry.RecordId;
import com.prevosql.tuple.Tuple;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

/**
 * Reader for binary indexes
 */
public class BinaryIndexReader {
    private final ByteBuffer buffer;
    private FileChannel channel;

    private static final int PAGE_SIZE = 4096;
    private static final Logger LOG = Logger.getLogger(BinaryIndexReader.class);

    /**
     * Constructs a BinaryIndexReader from the index at the
     * given filename
     *
     * @param filename File to load index from
     */
    public BinaryIndexReader(String filename) {
        try {
            FileInputStream inputStream = new FileInputStream(filename);
            channel = inputStream.getChannel();

        } catch (FileNotFoundException e) {
            LOG.fatal(e);
            System.err.println("Failed to load com.cs5321.index file: " + e.getMessage());
            System.exit(1);
        }

        buffer = ByteBuffer.allocate(PAGE_SIZE);
    }

    /**
     * Finds a data entry for a given key
     *
     * @param key Value of search key to look up
     * @return A data entry corresponding to this key, or the nearest one after
     */
    public DataEntry findKey(int key) {
        try {
            channel.read(buffer, 0);
            buffer.flip();
            int rootAddress = buffer.getInt();
            buffer.clear();
            return findKeyHelper(rootAddress, key);

        } catch (IOException e) {
            LOG.fatal(e);
            System.err.println("Failed to read com.cs5321.index file: " + e.getMessage());
            System.exit(1);
        }

        return null;
    }

    /**
     * Finds a data entry corresponding to the given search key
     * from the root node located at rootAddress
     *
     * @param rootAddress Page which root node is located on
     * @param key Search key to find data entry for
     * @return Data entry for this key, or the closest one
     */
    private DataEntry findKeyHelper(int rootAddress, int key) {
        try {
            channel.read(buffer, rootAddress * PAGE_SIZE);
            buffer.flip();
            int nodeType = buffer.getInt();

            if (nodeType == 0) {
                // leaf node
                buffer.getInt();
                while (true) {
                    while (buffer.hasRemaining()) {
                        int searchKey = buffer.getInt();
                        int numRids = buffer.getInt();
                        if (searchKey == 0 && numRids == 0) {
                            break;
                        }
                        if (searchKey < key) {
                            if (buffer.position() + numRids * 8 > PAGE_SIZE) {
                                return null;
                            }
                            buffer.position(buffer.position() + numRids * 8);

                        } else {
                            DataEntry entry = new DataEntry();
                            entry.setSearchKey(searchKey);
                            ArrayList<RecordId> rids = new ArrayList<>();
                            for (int i = 0; i < numRids; i++) {
                                int pageId = buffer.getInt();
                                int tupleId = buffer.getInt();
                                rids.add(new RecordId(pageId, tupleId));
                            }
                            buffer.clear();
                            entry.setRecordIds(rids);
//                            LOG.debug(entry);
                            return entry;
                        }
                    }

                    buffer.clear();
                    channel.read(buffer, (++rootAddress) * PAGE_SIZE);
//                    LOG.debug("SCANNING THE NEXT PAGE");
                    buffer.flip();
                    nodeType = buffer.getInt();
                    int numEntries = buffer.getInt();
                    if (numEntries == 0 || nodeType != 0) {
                        return null;
                    }
                }
            }

            int numKeys = buffer.getInt();
            int prevKey = -1;
            int addr = 0;
            while (buffer.hasRemaining()) {
                if (addr == numKeys) {
                    break;
                }
                int res = buffer.getInt();
                if (res >= key) {
                    int pos;
                    if (prevKey != -1) {
                        pos = (numKeys - 1) * 4;

                    } else {
                        pos = numKeys * 4 + (addr - 1) * 4;
                    }
                    int addrOfNextKey = buffer.getInt(buffer.position() + pos);
                    buffer.clear();
                    return findKeyHelper(addrOfNextKey, key);

                } else {
                    prevKey = res;
                }
                addr++;
            }
            int pos = addr * 4;
            int addrOfNextKey = buffer.getInt(buffer.position() + pos);
            buffer.clear();
            return findKeyHelper(addrOfNextKey, key);

        } catch (IOException e) {
            LOG.fatal(e);
            System.err.println("Failed to read com.cs5321.index file: " + e.getMessage());
            System.exit(1);
        }

        return null;
    }

    /**
     * Loads a tuple from data page in table tableName using rid as an identifier
     *
     * @param rid RecordID used to look up tuple
     * @param tableName Name of table to look up tuple in
     * @return Tuple located at rid in tableName
     */
    public static Tuple loadTuple(RecordId rid, String tableName) {
        try {
            FileInputStream fileInputStream = new FileInputStream(DBCatalog.getInstance().getTable(tableName).getFileName());
            FileChannel channel = fileInputStream.getChannel();
            ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
            channel.read(buffer, rid.getPageId() * PAGE_SIZE);
            buffer.flip();
            int numAttributes = buffer.getInt();
            buffer.getInt();
            buffer.position(8 + rid.getTupleId() * 4 * numAttributes);
            String[] args = new String[numAttributes];
            for (int i = 0; i < numAttributes; i++) {
                args[i] = "" + buffer.getInt();
            }
            return new Tuple(args);


        } catch (FileNotFoundException e) {
            LOG.fatal(e);
            System.err.println("Failed to load relation file: " + e.getMessage());
            System.exit(1);

        } catch (IOException e) {
            LOG.fatal(e);
            System.err.println("Failed to read relation file: " + e.getMessage());
            System.exit(1);
        }

        return null;
    }
}
