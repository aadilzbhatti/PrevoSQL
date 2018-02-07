package com.prevosql.tuple.io.reader;

import com.prevosql.tuple.Tuple;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

class BinaryTupleReader implements TupleReader {
    private int numAttributes;
    private int numTuples;

    private FileChannel channel;
    private ByteBuffer buffer;
    private int currTuple = 0;
    private int currPage = 0;

    private static final int PAGE_SIZE = 4096;
    private static final Logger LOG = Logger.getLogger(BinaryTupleReader.class);

    BinaryTupleReader(String filename) {
        try {
            String path = Paths.get(filename).toAbsolutePath().toString();
            FileInputStream fin = new FileInputStream(path);
            channel = fin.getChannel();
            buffer = ByteBuffer.allocate(PAGE_SIZE);
            int read = initializeDiskPage();
            if (read < 0) {
                LOG.warn("No bytes read from file " + filename);
            }

        } catch (FileNotFoundException e) {
            LOG.fatal("File " + filename + " not found", e);

        }
    }

    @Override
    public Tuple readNextTuple() {
        if (buffer.remaining() >= 4 * numAttributes) {
            if (currTuple == numTuples) {
                LOG.info("Finished reading bytes from buffer");
                return null;
            }

            // we have at least one tuple remaining to read
            String[] tuple = new String[numAttributes];
            for (int i = 0; i < numAttributes; i++) {
                tuple[i] = "" + buffer.getInt();
            }

            currTuple++;
            return new Tuple(tuple);

        } else {
            // we need to read another page into the buffer
            LOG.debug("Reading another page into buffer");

            currPage++;                         // to signify that we have read a page
            buffer.clear();                     // clear the buffer
            int read = initializeDiskPage();
            if (read < 0) {
                buffer.clear();
                return null;
            }
            return readNextTuple();
        }
    }

    @Override
    public void reset() {
        currTuple = 0;
        currPage = 0;
        buffer.clear();
        initializeDiskPage();
    }

    @Override
    public int getNumAttributes() {
        return numAttributes;
    }

    @Override
    public void reset(int index) {
        currTuple = index;
        buffer.clear();
        int numBytesSinceBeginning = 8 + index * numAttributes * 4;
        currPage = numBytesSinceBeginning / PAGE_SIZE;
        try {
            channel.read(buffer, currPage * PAGE_SIZE);
            buffer.flip();
            numAttributes = buffer.getInt();
            numTuples = buffer.getInt();
            buffer.position(numBytesSinceBeginning % PAGE_SIZE);

        } catch (IOException e) {
            LOG.fatal(e);
            System.err.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
    }

    @Override
    public int getCurrPage() {
        return currPage;
    }

    @Override
    public int getNumTuples() {
        return numTuples;
    }

    private int initializeDiskPage() {
        int res = -1;
        try {
            res = channel.read(buffer, currPage * PAGE_SIZE);
            if (res < 0) {
                return res;
            }
            buffer.flip();
            numAttributes = buffer.getInt();
            numTuples = buffer.getInt();
            currTuple = 0;

            LOG.debug("Initialized Disk Page " + currPage + " for reading: " + numTuples + " tuples, " + numAttributes + " attributes");
            return res;

        } catch (IOException e) {
            LOG.fatal("Failed to read disk page into buffer", e);
        }
        return res;
    }
}
