package com.prevosql.index.node;

import com.prevosql.index.entry.IndexEntry;

import java.util.Map;
import java.util.TreeMap;

/**
 * Models internal index nodes
 */
public class IndexNode extends TreeNode {
    private final int[] keys;
    private final Map<Integer, IndexEntry> indexEntryMap;
    private final TreeNode[] children;
    private final int order;
    private int childIndex = 0;
    private int keyIndex = 0;
    private int prevBefore = 0;
    private int prevSearchKey = 0;

    /**
     * Builds an IndexNode of order order
     *
     * @param order Order to build index node for
     */
    public IndexNode(int order) {
        this.keys = new int[2 * order];
        this.indexEntryMap = new TreeMap<>();
        this.children = new TreeNode[2 * order + 1];
        this.order = order;
    }

    /**
     * Adds an entry to the node
     *
     * @param searchKey Value of search key
     * @param node Child node to add
     */
    public void addEntry(int searchKey, TreeNode node) {
        addEntry(searchKey, node, 2 * order + 1);
    }

    /**
     * Adds an entry to the node with searchkey value searchKey
     * with limit limit
     *
     * @param searchKey Value of search key
     * @param node Node to add
     * @param limit Max number of child pointers for the added node
     */
    public void addEntry(int searchKey, TreeNode node, int limit) {
        if (childIndex == limit) {
            return;
        }

        if (indexEntryMap.get(searchKey) == null) {
            if (keyIndex == limit - 1) {
                indexEntryMap.get(prevSearchKey).setAfter(childIndex);
                children[childIndex] = node;
                return;
            }
            IndexEntry entry = new IndexEntry();
            entry.setAfter(childIndex + 1);
            entry.setBefore(prevBefore);
            children[childIndex] = node;
            prevBefore = childIndex + 1;
            indexEntryMap.put(searchKey, entry);
            childIndex++;
            keys[keyIndex++] = searchKey;
            prevSearchKey = searchKey;

        } else {
            children[childIndex++] = node;
        }
    }

    public int getKey(int index) {
        return keys[index];
    }

    /**
     * Returns child node at particular index in this
     * node
     *
     * @param index Position of child node in this node
     * @return Child node located at position index
     */
    public TreeNode getChild(int index) {
        return children[index];
    }

    /**
     * @return All child nodes belonging to this node
     */
    public TreeNode[] getChildren() {
        return children;
    }

    /**
     * @return All search key values belonging to this node
     */
    public int[] getKeys() {
        return keys;
    }

    /**
     * @return Number of children
     */
    public int getNumChildren() {
        return childIndex;
    }

    /**
     * @return Number of search keys
     */
    public int getNumKeys() {
        return keyIndex;
    }
}
