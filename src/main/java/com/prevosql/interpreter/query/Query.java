package com.prevosql.interpreter.query;

import com.prevosql.interpreter.query.plan.LogicalPlan;
import com.prevosql.interpreter.query.plan.visitor.PhysicalPlanBuilderVisitor;
import com.prevosql.interpreter.query.plan.visitor.PhysicalPlanPrinterVisitor;
import com.prevosql.operator.logical.LogicalOperator;
import com.prevosql.operator.physical.PhysicalOperator;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.log4j.Logger;

/**
 * Query plan class, builds query plan
 * from input SQL query string
 */
public class Query {
    private PhysicalOperator root;
    private String logicalPlan;
    private String physicalPlan;
    private static final Logger LOG = Logger.getLogger(Query.class);

    public Query(String query) {
        LOG.info("Query: " + query);
        try {
            LogicalPlan lp = new LogicalPlan(query);
            LogicalOperator logicalOperator = lp.getRoot();
            logicalPlan = lp.getLogicalPlan();
            LOG.info("\n" + logicalPlan);

            PhysicalPlanBuilderVisitor visitor = new PhysicalPlanBuilderVisitor();
            logicalOperator.accept(visitor);
            root = visitor.getResult();
            PhysicalPlanPrinterVisitor physicalPlanPrinter = new PhysicalPlanPrinterVisitor();
            root.accept(physicalPlanPrinter);

            physicalPlan = physicalPlanPrinter.getPhysicalPlan();
            LOG.info("\n" + physicalPlan);

        } catch (JSQLParserException e) {
            LOG.fatal(e);
            System.err.println("Error while parsing SQL query: " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Runs the query and prints to standard output
     */
    public void invoke() {
        root.dump();
    }

    /**
     * Runs the query and writes to a file with the specified
     * filename
     *
     * @param filename Name of file to print to
     */
    public void invoke(String filename) {
        root.dump(filename);
    }

    public PhysicalOperator getRoot() {
        return root;
    }

    public String getLogicalPlan() {
        return logicalPlan;
    }

    public String getPhysicalPlan() {
        return physicalPlan;
    }

    public static void dumpLogicalPlan(String query) {
        try {
            LogicalPlan lp = new LogicalPlan(query);
            LOG.info(lp.getLogicalPlan());

        } catch (JSQLParserException e) {
            LOG.fatal(e);
            System.err.println("Error while parsing SQL query: " + e.getMessage());
            System.exit(1);
        }
    }

    public static void dumpPhysicalPlan(String query) {
        Query q = new Query(query);
        PhysicalPlanPrinterVisitor visitor = new PhysicalPlanPrinterVisitor();
        q.getRoot().accept(visitor);
        LOG.info(visitor.getPhysicalPlan());
    }
}