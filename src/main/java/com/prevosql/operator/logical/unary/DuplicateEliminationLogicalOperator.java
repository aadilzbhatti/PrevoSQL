package com.prevosql.operator.logical.unary;

import com.prevosql.interpreter.query.plan.visitor.LogicalPlanVisitor;
import com.prevosql.operator.logical.LogicalOperator;

/**
 * Models a duplicate elimination operator
 */
public class DuplicateEliminationLogicalOperator extends UnaryLogicalOperator {
    /**
     * Constructs a DuplicateEliminationOperator
     *
     * @param child Child to remove duplicate elements from
     */
    public DuplicateEliminationLogicalOperator(LogicalOperator child) {
        super(child);
    }

    /**
     * This is used to expose the operator in the PhysicalPlanExpressionVisitor
     * so that we can convert it to a physical operator
     *
     * @param visitor ExpressionVisitor to call on this object
     */
    @Override
    public void accept(LogicalPlanVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public boolean equals(LogicalOperator other) {
        return other instanceof DuplicateEliminationLogicalOperator
                && getChild().equals(((DuplicateEliminationLogicalOperator) other).getChild());
    }
}
