package com.prevosql.operator.physical.unary;

import com.prevosql.operator.physical.PhysicalOperator;

/**
 * Models unary operators
 */
public abstract class UnaryPhysicalOperator extends PhysicalOperator {
    private PhysicalOperator child;

    /**
     * Constructs a unary operator
     *
     * @param child Child operator
     */
    public UnaryPhysicalOperator(PhysicalOperator child) {
        super(child.getTableName());
        this.numAttributes = child.getNumAttributes();
        this.child = child;
    }

    /**
     * @return Child operator
     */
    public PhysicalOperator getChild() {
        return child;
    }
}
