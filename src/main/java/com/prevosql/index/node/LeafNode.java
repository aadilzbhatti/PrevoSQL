package com.prevosql.index.node;

import com.prevosql.index.entry.DataEntry;
import com.prevosql.index.entry.RecordId;

import java.util.ArrayList;
import java.util.List;

/**
 * Models leaf index nodes
 */
public class LeafNode extends TreeNode {
    private int numEntries;
    private List<DataEntry> entries;
    private LeafNode next;
    private boolean written;

    /**
     * Builds a leaf node with numEntries entries
     *
     * @param numEntries Number of entries in this node
     */
    public LeafNode(int numEntries) {
        this.numEntries = numEntries;
        this.entries = new ArrayList<>();
    }

    /**
     * Sets next node
     *
     * @param next Leaf node to set next pointer to
     */
    public void setNext(LeafNode next) {
        this.next = next;
    }

    /**
     * @return Next leaf node
     */
    public LeafNode getNext() {
        return next;
    }

    public void setEntries(List<DataEntry> entries) {
        this.entries = entries;
    }

    public void addEntry(int key, List<RecordId> recordIds) {
        entries.add(new DataEntry(key, recordIds));
    }

    public DataEntry getEntry(int key) {
        return entries.get(key);
    }

    public void setNumEntries(int numEntries) {
        this.numEntries = numEntries;
    }

    public List<DataEntry> getEntries() {
        return entries;
    }

    public boolean isWritten() {
        return written;
    }

    public void setWritten(boolean written) {
        this.written = written;
    }
}
