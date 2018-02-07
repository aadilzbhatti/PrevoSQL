package com.prevosql.index.io.writer;

import com.prevosql.index.node.IndexNode;
import com.prevosql.index.node.LeafNode;
import com.prevosql.index.node.TreeNode;

/**
 * Writes indexes to disk
 */
public interface IndexWriter {
    void serializeIndexNode(IndexNode curr, int currIndexPage);

    void serializeLeafNode(LeafNode curr, int size, int currPage);

    void writeHeaderPage(int order, int numLeafNodes, TreeNode root);
}
