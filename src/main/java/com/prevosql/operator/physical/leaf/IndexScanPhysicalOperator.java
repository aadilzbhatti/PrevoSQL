package com.prevosql.operator.physical.leaf;

import com.prevosql.config.catalog.DBCatalog;
import com.prevosql.index.entry.DataEntry;
import com.prevosql.index.entry.RecordId;
import com.prevosql.index.io.reader.BinaryIndexReader;
import com.prevosql.config.Configuration;
import com.prevosql.interpreter.query.plan.visitor.PhysicalPlanVisitor;
import com.prevosql.tuple.Tuple;
import com.prevosql.tuple.io.reader.TupleReader;
import com.prevosql.tuple.io.reader.TupleReaderFactory;

import java.nio.file.Paths;

/**
 * Operator that uses an index to optimize table scans
 */
public class IndexScanPhysicalOperator extends LeafPhysicalOperator {
    private final int lowkey;
    private final int highkey;
    private final boolean isClustered;
    private int currKey;
    private final BinaryIndexReader indexReader;
    private TupleReader tupleReader;
    private final int column;
    private String searchKey;

    private int recordIdIndex = 0;
    private boolean initializedClusteredIndex = false;
    private DataEntry prevEntry;
    private int prevKey;

    /**
     * Constructs an IndexScanPhysicalOperator
     *
     * @param tableName Name of table to scan
     * @param searchKey Attribute we are scanning
     * @param lowkey Lower bound of search range
     * @param highkey Upper bound of search range
     * @param isClustered Whether or not we are using a clustered index
     */
    public IndexScanPhysicalOperator(String tableName, String searchKey, int lowkey, int highkey, boolean isClustered) {
        super(tableName);
        this.column = DBCatalog.getInstance().getTable(tableName).getIndexForColumn(searchKey);
        this.lowkey = lowkey;
        this.highkey = highkey;
        this.isClustered = isClustered;
        this.currKey = lowkey;
        this.searchKey = searchKey;

        String indexPath = Paths.get(Configuration.getInstance().getInputDirPath(), "db", "indexes", tableName + "." + searchKey).toString();
        this.indexReader = new BinaryIndexReader(indexPath);

        if (isClustered) {
            tupleReader = TupleReaderFactory.getReader(DBCatalog.getInstance().getTable(tableName).getFileName());
        }
    }

    @Override
    public Tuple getNextTuple() {
        if (currKey > highkey) {
            return null;
        }

        if (isClustered) {
            if (!initializedClusteredIndex) {
                DataEntry entry = indexReader.findKey(lowkey);
                RecordId firstRecord = entry.getRecordIds().get(0);
                int tupleOffset = firstRecord.getPageId() * tupleReader.getNumTuples() + firstRecord.getTupleId();
                tupleReader.reset(tupleOffset);
                initializedClusteredIndex = true;
            }

            Tuple t = tupleReader.readNextTuple();
            if (t == null) {
                return null;
            }
            if (Integer.parseInt(t.get(column)) <= highkey) {
                return t;
            }

        } else {
            DataEntry entry = indexReader.findKey(currKey);
            if (entry == null && currKey == prevKey) {
                return null;
            } else if (entry == null) {
                prevKey = currKey;
                return getNextTuple();
            }

            if (prevEntry != null) {
                if (entry.equals(prevEntry) && currKey != prevKey) {
                    currKey++;
                    return getNextTuple();
                }

                prevEntry = entry;
                prevKey = currKey;

            } else {
                prevEntry = entry;
                prevKey = currKey;
            }

            RecordId rid = entry.getRecordIds().get(recordIdIndex++);

            if (recordIdIndex >= entry.getRecordIds().size()) {
                recordIdIndex = 0;
                currKey++;
            }

            Tuple t = BinaryIndexReader.loadTuple(rid, getTableName());
            if (t != null) {
                if (Integer.parseInt(t.get(column)) <= highkey) {
                    return t;
                }
            }
        }

        return null;
    }

    @Override
    public void reset() {
        currKey = lowkey;
        if (isClustered) {
            tupleReader.reset();
        }
    }

    @Override
    public void accept(PhysicalPlanVisitor visitor) {
        visitor.visit(this);
    }

    public String getInfo() {
        return getTableName() + "," + searchKey + "," + lowkey + "," + highkey;
    }
}
