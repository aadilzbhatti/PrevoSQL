package com.prevosql.tuple.io.writer;

import com.prevosql.tuple.Tuple;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

class BinaryTupleWriter implements TupleWriter {
    private FileChannel channel;
    private ByteBuffer buffer;
    private final int numAttributes;
    private int numTuples = 0;
    private int currPage = 0;

    private static final int PAGE_SIZE = 4096;
    private static final Logger LOG = Logger.getLogger(BinaryTupleWriter.class);

    BinaryTupleWriter(String filename, int numAttributes) {
        this.numAttributes = numAttributes;

        String path = Paths.get(filename).toAbsolutePath().toString();
        try {
            File f = new File(path);
            while (!f.createNewFile()) {
                if (f.delete() && f.createNewFile()) {
                    break;
                }
            }
            FileOutputStream fout = new FileOutputStream(f, false);
            channel = fout.getChannel();
            buffer = ByteBuffer.allocate(PAGE_SIZE);
            initializeDiskPage(numAttributes);

        } catch (Exception e) {
            LOG.fatal("Failed to create new file: " + filename, e);
        }
    }

    @Override
    public boolean writeTuple(Tuple t) {
        assert t.length() == numAttributes;

        if (buffer.remaining() >= 4 * numAttributes) {
            // we have room to write at least 1 more tuple to the file
            for (int i = 0; i < numAttributes; i++) {
                buffer.putInt(Integer.parseInt(t.get(i)));
            }
            buffer.putInt(4, ++numTuples);

        } else {
            return !writeDiskPage() && writeTuple(t);
        }
        return true;
    }

    @Override
    public void reset() {
        buffer.clear();
        currPage = 0;
    }

    @Override
    public void flush() {
       LOG.info("Forcing a write to disk");
       writeDiskPage();
    }

    private void initializeDiskPage(int numAttributes) {
        buffer.putInt(numAttributes);
        buffer.putInt(0);
        LOG.debug("Initialized Disk Page " + currPage + " for writing: " + numTuples + " tuples, " + numAttributes + " attributes");
    }

    private boolean writeDiskPage() {
        LOG.info("Writing buffer to disk page");

        // we have to write to a disk page
        while (buffer.remaining() > 0) {
            buffer.putInt(0); // fill in the remaining space with 0's
        }
        try {
            buffer.flip();
            channel.write(buffer, currPage * PAGE_SIZE); // we write to the file in blocks of size 4kb
            LOG.info("Wrote bytes to disk at file position " + currPage * PAGE_SIZE);

        } catch (IOException e) {
            LOG.fatal("Failed to write buffer to disk", e);
            return true;
        }

        // then we have to set up the buffer to receive more data
        buffer.clear();
        numTuples = 0;
        initializeDiskPage(numAttributes);
        currPage++;
        return false;
    }
}
