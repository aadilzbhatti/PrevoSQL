package com.prevosql.operator.logical.leaf;

import com.prevosql.operator.logical.LogicalOperator;

public abstract class LeafLogicalOperator implements LogicalOperator {
    private final String tableName;

    LeafLogicalOperator(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(LogicalOperator other) {
        return other instanceof LeafLogicalOperator
                && ((LeafLogicalOperator) other).tableName.equalsIgnoreCase(tableName);
    }
}
