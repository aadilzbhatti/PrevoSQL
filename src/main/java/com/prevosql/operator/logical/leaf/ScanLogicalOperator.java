package com.prevosql.operator.logical.leaf;

import com.prevosql.interpreter.query.plan.visitor.LogicalPlanVisitor;

/**
 * Models a scan operator
 */
public class ScanLogicalOperator extends LeafLogicalOperator {
    /**
     * Constructs a scan operator
     *
     * @param tableName Name of table to scan
     */
    public ScanLogicalOperator(String tableName) {
        super(tableName);
    }

    @Override
    public void accept(LogicalPlanVisitor visitor) {
        visitor.visit(this);
    }
}
