package com.prevosql.operator.physical.leaf;

import com.prevosql.config.catalog.DBCatalog;
import com.prevosql.config.catalog.Table;
import com.prevosql.interpreter.query.plan.visitor.PhysicalPlanVisitor;
import com.prevosql.tuple.Tuple;
import com.prevosql.tuple.io.reader.TupleReader;
import com.prevosql.tuple.io.reader.TupleReaderFactory;

/**
 * Handles select operators, most basic operator and leaf node for
 * all operator trees
 */
public class ScanPhysicalOperator extends LeafPhysicalOperator {
    private TupleReader tupleReader;
    private boolean binary = true; // use binary i/o by default

    /**
     * Initializes ScanOperator
     *
     * @param tableName Input table name
     */
    public ScanPhysicalOperator(String tableName) {
        super(tableName);
        setTable(tableName);
    }

    public ScanPhysicalOperator(String tableName, boolean binary) {
        super(tableName);
        this.binary = binary;
    }

    /**
     * Sets filename and file reader for the input table
     *
     * @param tableName Input table name
     */
    private void setTable(String tableName) {
        Table table = DBCatalog.getInstance().getTable(tableName.toLowerCase());
        String fileName;
        try {
            fileName = table.getFileName();
            tupleReader = TupleReaderFactory.getReader(fileName, binary);
            this.numAttributes = tupleReader.getNumAttributes();

        } catch (NullPointerException e) {
            LOG.trace("Invalid table: " + tableName, e);
            System.err.println("Invalid table: " + tableName);
        }
    }

    /**
     * Gets next available tuple in the file
     *
     * @return Next available tuple
     */
    @Override
    public Tuple getNextTuple() {
        if (tupleReader == null) {
            setTable(this.getTableName());
        }
        return tupleReader.readNextTuple();
    }

    /**
     * Resets file location so as to return first tuple
     * upon next call to getNextTuple()
     */
    @Override
    public void reset() {
//        LOG.debug("Resetting scan of " + tableName);
        if (tupleReader != null) {
            tupleReader.reset();
        }
    }

    @Override
    public void accept(PhysicalPlanVisitor visitor) {
        visitor.visit(this);
    }
}
