package com.prevosql.operator.physical.binary;

import com.prevosql.operator.physical.PhysicalOperator;

/**
 * Models operators with two children
 */
public abstract class BinaryPhysicalOperator extends PhysicalOperator {
    protected PhysicalOperator leftChild;
    protected PhysicalOperator rightChild;

    /**
     * Constructs a BinaryPhysicalOperator from two children
     *
     * @param leftChild Left child operator
     * @param rightChild Right child operator
     */
    public BinaryPhysicalOperator(PhysicalOperator leftChild, PhysicalOperator rightChild) {
        super(leftChild.getTableName());
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.setNumAttributes(leftChild.getNumAttributes() + rightChild.getNumAttributes());
    }

    /**
     * @return Left child operator
     */
    public PhysicalOperator getLeftChild() {
        return leftChild;
    }

    /**
     * @return Right child operator
     */
    public PhysicalOperator getRightChild() {
        return rightChild;
    }
}
