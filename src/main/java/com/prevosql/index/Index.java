package com.prevosql.index;

import com.prevosql.config.catalog.DBCatalog;
import com.prevosql.index.entry.DataEntry;
import com.prevosql.index.entry.RecordId;
import com.prevosql.index.io.writer.IndexWriter;
import com.prevosql.index.io.writer.IndexWriterFactory;
import com.prevosql.index.node.IndexNode;
import com.prevosql.index.node.LeafNode;
import com.prevosql.index.node.TreeNode;
import com.prevosql.operator.physical.leaf.ScanPhysicalOperator;
import com.prevosql.operator.physical.unary.sort.ExternalSortPhysicalOperator;
import com.prevosql.tuple.Tuple;
import com.prevosql.tuple.io.reader.TupleReader;
import com.prevosql.tuple.io.reader.TupleReaderFactory;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

/**
 * Builds clustered and unclustered B+-tree indexes on
 * one search key
 */
public class Index {
    private int numLeafNodes;

    private static final Logger LOG = Logger.getLogger(Index.class);

    /**
     * Builds an index for table with name tableName on attribute key of order
     * order, can be clustered or unclustered
     *
     * @param tableName Name of table to build index for
     * @param key Name of attribute to build index on
     * @param order Order of index
     * @param clustered Whether or not the index is clustered
     */
    public Index(String tableName, String key, int order, boolean clustered) {
        buildIndex(tableName, key, order, clustered);
    }

    /**
     * @return Number of leaf nodes in the tree
     */
    public int getNumLeafNodes() {
        return numLeafNodes;
    }

    /**
     * Builds the index for table tableName on attribute key of order
     * order, can be clustered or unclustered
     *
     * @param tableName Name of table to build index for
     * @param key Name of attribute to build index on
     * @param order Order of index
     * @param clustered Whether or not the index is clustered
     */
    private void buildIndex(String tableName, String key, int order, boolean clustered) {
        // if clustered, sort the relation file on the key before doing anything
        if (clustered) {
            sortAndReplace(tableName, key);
        }

        // set up tuple reader
        TupleReader reader = TupleReaderFactory.getReader(DBCatalog.getInstance().getTable(tableName).getFileName());
        IndexWriter serializer = IndexWriterFactory.getIndexWriter(tableName, key);
        int column = DBCatalog.getInstance().getTable(tableName).getIndexForColumn(key);
        TreeMap<Integer, List<RecordId>> indexMap = new TreeMap<>();

        // create data entries indexed by search key
        int tupleOffset = 0;
        int currPage = 0;
        Tuple t;
        while ((t = reader.readNextTuple()) != null) {
            if (reader.getCurrPage() != currPage) {
                tupleOffset = 0;
                currPage = reader.getCurrPage();
            }
            int searchKey = Integer.parseInt(t.get(column));
            List<RecordId> keys;
            if (indexMap.get(searchKey) == null) {
                keys = new ArrayList<>();
            } else {
                keys = indexMap.get(searchKey);
            }
            keys.add(new RecordId(currPage, tupleOffset));
            indexMap.put(searchKey, keys);
            tupleOffset++;
        }

        // build leaf layer
        int numDataEntries = indexMap.size();
        int numNodes = (int) Math.ceil(numDataEntries / (2.0 * order));
        int size = 0;
        int limit = 2 * order;
        int currLeafPage = 1; // since we want to write on the 1st page, not 0th

        LeafNode curr = new LeafNode(2 * order);
        numNodes--;
        ArrayList<LeafNode> leafNodes = new ArrayList<>();
        leafNodes.add(curr);
        for (int searchKey : indexMap.keySet()) {
            List<RecordId> entries = indexMap.get(searchKey);
            if (size < limit) {
                size++;

            } else {
                // serialize this node and start the next one
                serializer.serializeLeafNode(curr, size, currLeafPage);
                curr.setWritten(true);

                size = 1;
                curr.setNext(new LeafNode(2 * order));
                curr = curr.getNext();
                leafNodes.add(curr);
                numNodes--;
                if (numNodes == 2 && (numDataEntries > 2 * order && numDataEntries < 3 * order)) {
                    limit = numDataEntries / 2;
                }
                currLeafPage++;
            }
            curr.addEntry(searchKey, entries);
            numDataEntries--;
        }
        if (!curr.isWritten()) {
            serializer.serializeLeafNode(curr, size, currLeafPage);
        }
        if (size < 2 * order) {
            curr.setNumEntries(size);
        }

        // build first index layer
        int currIndexNode = currLeafPage + 1;
        int numLeafNodes = leafNodes.size();
        int numIndexChildren = 2 * order + 1;
        int numNodesLeft = (int) Math.ceil(leafNodes.size() / numIndexChildren) + 1;
        limit = numIndexChildren;
        int origLimit = limit;
        List<IndexNode> indexNodes = new ArrayList<>();

        for (int i = 0; i < numLeafNodes; i += origLimit) {
            if (origLimit > limit) {
                origLimit = limit;
            }
            IndexNode node = new IndexNode(order);
            for (int j = 0; j < limit; j++) {
                if (j + i >= leafNodes.size()) {
                    break;
                }
                LeafNode leafNode = leafNodes.get(j + i);
                LeafNode nextNode = leafNode.getNext();
                DataEntry lastEntry;
                if (nextNode != null) {
                    lastEntry = leafNode.getNext().getEntry(0);
                } else {
                    lastEntry = leafNode.getEntry(0);
                }
                int nodeKey = lastEntry.getSearchKey();
                node.addEntry(nodeKey, leafNode, limit);
            }
            serializer.serializeIndexNode(node, currIndexNode);
            indexNodes.add(node);
            numNodesLeft--;
            if (numNodesLeft == 2) {
                int m = numLeafNodes - numIndexChildren - i;
                if (m > numIndexChildren && m < 3 * order + 2) {
                    limit = m / 2;
                }
            }
            currIndexNode++;
        }

        // build rest of the tree
        limit = numIndexChildren;
        origLimit = limit;
        while (true) {
            LOG.info("Added a level to the tree");
            List<IndexNode> interList = new ArrayList<>();
            numNodesLeft  = (int) Math.ceil(indexNodes.size() / numIndexChildren);
            for (int i = 0; i < indexNodes.size(); i += origLimit) {
                if (origLimit > limit) {
                    origLimit = limit;
                }
                IndexNode res = new IndexNode(order);
                for (int j = 0; j < limit; j++) {
                    if (j + i >= indexNodes.size()) {
                        break;
                    }
                    IndexNode node = indexNodes.get(j + i);
                    int lastChild = findKeyForNode(node);
                    res.addEntry(lastChild, node, limit);
                }
                serializer.serializeIndexNode(res, currIndexNode);
                interList.add(res);
                numNodesLeft--;
                if (numNodesLeft == 2) {
                    int m = indexNodes.size() - numIndexChildren - i;
                    if (m > numIndexChildren && m < 3 * order + 2) {
                        limit = m / 2;
                    }
                }
                currIndexNode++;
            }
            if (interList.size() == 1) {
                TreeNode root = interList.get(0);
                this.numLeafNodes = numLeafNodes;
                serializer.writeHeaderPage(order, numLeafNodes, root);
                return;
            } else {
                indexNodes = interList;
            }
        }
    }

    /**
     * Finds the value of a search key for a node
     *
     * @param node Node to find search key for
     * @return Value of search key for node
     */
    private int findKeyForNode(IndexNode node) {
        TreeNode[] children = node.getChildren();
        if (children[node.getNumChildren()] instanceof LeafNode) {
            LeafNode leafNode = (LeafNode) children[node.getNumChildren()];
            if (leafNode.getNext() != null) {
                return leafNode.getNext().getEntry(0).getSearchKey();
            } else {
                return leafNode.getEntry(0).getSearchKey();
            }
        } else if (children[0] instanceof LeafNode) {
            return ((LeafNode) children[0]).getEntry(0).getSearchKey();
        } else {
            return findKeyForNode((IndexNode) children[0]);
        }
    }

    /**
     * Sorts file used by table tableName on attribute key
     * and replaces it on disk
     *
     * @param tableName Name of table to sort & replace
     * @param key Attribute we are sorting on
     */
    private void sortAndReplace(String tableName, String key) {
        Column c = new Column();
        c.setColumnName(key);
        Table t = new Table();
        t.setName(tableName);
        c.setTable(t);
        OrderByElement o = new OrderByElement();
        o.setExpression(c);

        ScanPhysicalOperator so = new ScanPhysicalOperator(tableName);
        ExternalSortPhysicalOperator exo = new ExternalSortPhysicalOperator(
                so,
                Collections.singletonList(o),
                10
        );

        Path source = exo.getFinalFilePath();
        Path dest = Paths.get(DBCatalog.getInstance().getTable(tableName).getFileName());
        try {
            Files.delete(dest);
            Files.copy(source, dest);

        } catch (IOException e) {
            LOG.fatal(e);
            System.err.println("Failed to create clustered index: " + e.getMessage());
            System.exit(1);
        }
    }
}
