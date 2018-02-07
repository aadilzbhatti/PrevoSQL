package com.prevosql.operator.logical.unary;

import com.prevosql.interpreter.query.plan.visitor.LogicalPlanVisitor;
import com.prevosql.operator.logical.LogicalOperator;
import com.prevosql.operator.logical.leaf.ScanLogicalOperator;
import net.sf.jsqlparser.expression.Expression;

/**
 * Models select com.cs5321.operator
 */
public class SelectLogicalOperator extends UnaryLogicalOperator {
    private final Expression selectCondition;

    /**
     * Constructs a select com.cs5321.operator
     *
     * @param child Child com.cs5321.operator to select from
     *
     * @param selectCondition Condition for which to choose which items to select
     */
    public SelectLogicalOperator(ScanLogicalOperator child, Expression selectCondition) {
        super(child);
        this.selectCondition = selectCondition;
    }

    /**
     * @return Select condition
     */
    public Expression getSelectCondition() {
        return selectCondition;
    }

    @Override
    public void accept(LogicalPlanVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public boolean equals(LogicalOperator other) {
        return other instanceof SelectLogicalOperator
                && getChild().equals(((SelectLogicalOperator) other).getChild())
                && selectCondition.toString().equalsIgnoreCase(((SelectLogicalOperator) other).selectCondition.toString());
    }
}
