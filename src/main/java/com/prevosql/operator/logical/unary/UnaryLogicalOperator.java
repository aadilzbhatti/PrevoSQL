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
     * Sets the child for this com.cs5321.operator
     *
     * @param child Child com.cs5321.operator to replace current
     */
    public void setChild(LogicalOperator child) {
        this.child = child;
    }

    /**
     * @return The table name for this com.cs5321.operator
     */
    @Override
    public String getTableName() {
        return child.getTableName();
    }
}
