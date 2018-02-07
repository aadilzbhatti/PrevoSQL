package com.prevosql.operator.physical;

import com.prevosql.interpreter.query.plan.visitor.PhysicalPlanVisitor;
import com.prevosql.tuple.Tuple;
import com.prevosql.tuple.io.writer.TupleWriter;
import com.prevosql.tuple.io.writer.TupleWriterFactory;
import org.apache.log4j.Logger;

/**
 * Abstract class to model relational algebra operators
 */
public abstract class PhysicalOperator {
    private String tableName;
    protected int numAttributes;

    protected static Logger LOG;

    protected PhysicalOperator() {
        LOG = Logger.getLogger(this.getClass());
    }

    /**
     * Initializes an com.cs5321.operator
     *
     * @param tableName Table name for com.cs5321.operator
     */
    protected PhysicalOperator(String tableName) {
        this();
        this.tableName = tableName;
    }

    /**
     * Implemented by other classes, returns next tuple
     *
     * @return Next valid tuple
     */
    public abstract Tuple getNextTuple();

    /**
     * Resets tuple output stream
     */
    public abstract void reset();

    /**
     * Visits this operator in a PhysicalPlanVisitor
     *
     * @param visitor PhysicalPlanVisitor visiting this operator
     */
    public abstract void accept(PhysicalPlanVisitor visitor);

    /**
     * Prints all valid tuples to standard output
     */
    public void dump() {
        LOG.info("Dumping all available valid tuples");
        Tuple t;
        long initialTime = System.nanoTime();
        while ((t = getNextTuple()) != null) {
            LOG.info(t);
        }
        long endingTime = System.nanoTime();
        LOG.info(String.format("SQL query took %f seconds", (endingTime - initialTime) / 1000000000.0));
    }

    /**
     * Writes all valid tuples to file
     *
     * @param filename Name of file
     */
    public void dump(String filename) {
        LOG.info("Dumping all available valid tuples to file " + filename);
        Tuple t = getNextTuple();
        TupleWriter tw;
        if (t == null) {
            LOG.info("No tuples returned from query");
            tw = TupleWriterFactory.getWriter(filename, 0);
            tw.flush();
            return;
        }
        tw = TupleWriterFactory.getWriter(filename, t.length());
//        tw = TupleWriterFactory.getWriter(filename, 0);
        long initialTime = System.nanoTime();
        while (t != null) {
//            LOG.info(t);
            tw.writeTuple(t);
            t = getNextTuple();
        }
        tw.flush();
        long endingTime = System.nanoTime();
        LOG.info(String.format("SQL query took %f seconds", (endingTime - initialTime) / 1000000000.0));
    }

    /**
     * @return Name of table for the operator
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Sets name of table for this operator
     *
     * @param tableName Desired name of table for this operator
     */
    protected void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getNumAttributes() {
        return numAttributes;
    }

    protected void setNumAttributes(int numAttributes) {
        this.numAttributes = numAttributes;
    }
}
