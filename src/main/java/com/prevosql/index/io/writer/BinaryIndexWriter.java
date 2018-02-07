package com.prevosql.index.io.writer;

import com.prevosql.index.entry.DataEntry;
import com.prevosql.index.entry.RecordId;
import com.prevosql.index.node.IndexNode;
import com.prevosql.index.node.LeafNode;
import com.prevosql.index.node.TreeNode;
import com.prevosql.config.Configuration;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

/**
 * Writer for binary indexes
 */
class BinaryIndexWriter implements IndexWriter {
    private final ByteBuffer buffer;
    private FileChannel channel;

    private static final int PAGE_SIZE = 4096;
    private static final Logger LOG = Logger.getLogger(BinaryIndexWriter.class);

    /**
     * Builds a binary index writer for the given table name
     * on the given search key
     *
     * @param tableName Name of relation we are indexing
     * @param key Key we are indexing on
     */
    BinaryIndexWriter(String tableName, String key) {
        // set up buffer & file channel for serialization
        String indexFilePath = Paths.get(Configuration.getInstance().getInputDirPath(), "db", "indexes", tableName + "." + key).toString();
        File f = new File(indexFilePath);
        try {
            f.createNewFile();
        } catch (IOException e) {
            LOG.fatal(e);
            System.err.println("Failed to create index file: " + e.getMessage());
            System.exit(1);
        }

        try {
            FileOutputStream outputStream = new FileOutputStream(f, false);
            channel = outputStream.getChannel();

        } catch (FileNotFoundException e) {
            LOG.fatal(e);
            System.err.println("Failed to create output stream: " + e.getMessage());
            System.exit(1);
        }

        buffer = ByteBuffer.allocate(PAGE_SIZE);
    }

    /**
     * Writes index node curr to disk at the current index page
     *
     * @param curr Index node to write to disk
     * @param currIndexPage Current page to write to
     */
    @Override
    public void serializeIndexNode(IndexNode curr, int currIndexPage) {
        curr.setAddress(currIndexPage);
        buffer.putInt(1);
        buffer.putInt(curr.getNumKeys());
        for (int key : curr.getKeys()) {
            if (key == 0 && curr.getNumKeys() > 1) {
                break;
            }
            buffer.putInt(key);
        }
        for (TreeNode child : curr.getChildren()) {
            if (child == null) {
                break;
            }
            buffer.putInt(child.getAddress());
        }

        // write the node to disk
        while (buffer.remaining() > 0) {
            buffer.putInt(0);
        }
        try {
            buffer.flip();
            channel.write(buffer, currIndexPage * PAGE_SIZE);

        } catch (IOException e) {
            LOG.fatal(e);
        }

        buffer.clear();
//        LOG.info("Serialized index node " + curr + " on page " + currIndexPage);
    }

    /**
     * Writes leaf node curr to disk with size number of entries
     * at page currPage
     *
     * @param curr Leaf node to write to disk
     * @param size Number of data entries in leaf node
     * @param currPage Current disk page to write to
     */
    @Override
    public void serializeLeafNode(LeafNode curr, int size, int currPage) {
        curr.setAddress(currPage);
        buffer.putInt(0);
        buffer.putInt(size);
        for (DataEntry dataEntry : curr.getEntries()) {
            buffer.putInt(dataEntry.getSearchKey());
            buffer.putInt(dataEntry.getRecordIds().size());
            for (RecordId rid : dataEntry.getRecordIds()) {
                buffer.putInt(rid.getPageId());
                buffer.putInt(rid.getTupleId());
            }
        }

        // write the node to disk
        while (buffer.remaining() > 0) {
            buffer.putInt(0);
        }
        try {
            buffer.flip();
            channel.write(buffer, currPage * PAGE_SIZE);

        } catch (IOException e) {
            LOG.fatal(e);
        }

        buffer.clear();
//        LOG.info("Serialized leaf node: " + curr + " with " + size + " data entries on page " + currPage);
    }

    /**
     * Writes the header page to disk of order order, with numLeafNodes
     * leaf nodes and root root
     *
     * @param order Order of tree, or fanout
     * @param numLeafNodes Number of leaf nodes in the tree
     * @param root Root node of tree
     */
    @Override
    public void writeHeaderPage(int order, int numLeafNodes, TreeNode root) {
        buffer.putInt(root.getAddress());
        buffer.putInt(numLeafNodes);
        buffer.putInt(order);

        // write the page to disk
        while (buffer.remaining() > 0) {
            buffer.putInt(0);
        }
        try {
            buffer.flip();
            channel.write(buffer, 0);

        } catch (IOException e) {
            LOG.fatal(e);
        }

        buffer.clear();
        LOG.info("Wrote header page with root at address " + root.getAddress() + ", " + numLeafNodes + " leaf nodes, and order " + order);
    }
}
