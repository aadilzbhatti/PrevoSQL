package com.prevosql.operator.logical.unary;

import com.prevosql.operator.logical.LogicalOperator;

public abstract class UnaryLogicalOperator implements LogicalOperator {
    private LogicalOperator child;

    UnaryLogicalOperator(LogicalOperator child) {
        this.child = child;
    }

    /**
     * @return Child to remove duplicate elements from
     */
    public LogicalOperator getChild() {
        return child;
    }

    /**
     * Sets the child for this operator
     *
     * @param child Child operator to replace current
     */
    public void setChild(LogicalOperator child) {
        this.child = child;
    }

    /**
     * @return The table name for this operator
     */
    @Override
    public String getTableName() {
        return child.getTableName();
    }
}
