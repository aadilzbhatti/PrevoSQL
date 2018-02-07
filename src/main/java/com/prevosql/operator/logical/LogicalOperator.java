package com.prevosql.operator.logical;

import com.prevosql.interpreter.query.plan.visitor.LogicalPlanVisitor;

public interface LogicalOperator {
    void accept(LogicalPlanVisitor visitor);

    String getTableName();

    boolean equals(LogicalOperator other);
}
