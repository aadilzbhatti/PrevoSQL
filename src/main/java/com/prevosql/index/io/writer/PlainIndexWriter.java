package com.prevosql.index.io.writer;

import com.prevosql.index.entry.DataEntry;
import com.prevosql.index.entry.RecordId;
import com.prevosql.index.node.IndexNode;
import com.prevosql.index.node.LeafNode;
import com.prevosql.index.node.TreeNode;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

class PlainIndexWriter implements IndexWriter {
    private PrintWriter pw;

    private static final Logger LOG = Logger.getLogger(PlainIndexWriter.class);

    public PlainIndexWriter(String tableName, String key) {
        File f = new File(tableName + "." + key + "-testing");
        try {
            f.createNewFile();
            FileWriter fileWriter = new FileWriter(f);
            pw = new PrintWriter(fileWriter);

        } catch (IOException e) {
            LOG.fatal(e);
            System.err.println("Failed to create com.cs5321.index file: " + e.getMessage());
            System.exit(1);
        }
    }

    @Override
    public void serializeIndexNode(IndexNode curr, int currIndexPage) {
        curr.setAddress(currIndexPage);
        StringBuilder sb = new StringBuilder("Index node with keys [");
        ArrayList<String> keys = new ArrayList<>();
        for (int key : curr.getKeys()) {
            if (key == 0 && keys.size() > 0) {
                break;
            }
            keys.add("" + key);
        }
        String s = String.join(", ", keys);
        sb.append(s);
        sb.append("] and child addresses [");

        ArrayList<String> children = new ArrayList<>();
        for (TreeNode child : curr.getChildren()) {
            if (child == null) {
                break;
            }
            children.add("" + child.getAddress());
        }
        s = String.join(", ", children);
        sb.append(s);
        sb.append("]\n");
        pw.println(sb);
        LOG.info("Serialized com.cs5321.index node " + curr + " on page " + currIndexPage);
        pw.flush();
    }

    @Override
    public void serializeLeafNode(LeafNode curr, int size, int currPage) {
        curr.setAddress(currPage);
        StringBuilder sb = new StringBuilder("LeafNode[\n");
        for (DataEntry dataEntry : curr.getEntries()) {
            sb.append("<[");
            sb.append(dataEntry.getSearchKey());
            sb.append(":");

            for (RecordId rid : dataEntry.getRecordIds()) {
                sb.append("(").append(rid.getPageId()).append(",").append(rid.getTupleId()).append(")");
            }

            sb.append("]>\n");
        }
        sb.append("]\n");
        pw.println(sb);
        pw.flush();
        LOG.info("Serialized leaf node: " + curr + " with " + size + " data entries on page " + currPage);
    }

    @Override
    public void writeHeaderPage(int order, int numLeafNodes, TreeNode root) {
        pw.println("Header page info: tree has order " + order + ", a root at address " + root.getAddress() + " and " + numLeafNodes + " leaf nodes." );
        pw.flush();
        LOG.info("Wrote header page with root at address " + root.getAddress() + ", " + numLeafNodes + " leaf nodes, and order " + order);
    }
}
