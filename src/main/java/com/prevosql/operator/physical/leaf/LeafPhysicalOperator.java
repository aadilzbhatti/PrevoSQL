package com.prevosql.operator.physical.leaf;

import com.prevosql.operator.physical.PhysicalOperator;

public abstract class LeafPhysicalOperator extends PhysicalOperator {
    public LeafPhysicalOperator(String tableName) {
        super(tableName);
    }
}
