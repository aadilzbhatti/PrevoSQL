package com.prevosql.interpreter.query.plan.visitor;

import com.prevosql.operator.logical.nary.JoinLogicalOperator;
import com.prevosql.operator.logical.leaf.ScanLogicalOperator;
import com.prevosql.operator.logical.unary.DuplicateEliminationLogicalOperator;
import com.prevosql.operator.logical.unary.ProjectLogicalOperator;
import com.prevosql.operator.logical.unary.SelectLogicalOperator;
import com.prevosql.operator.logical.unary.SortLogicalOperator;

public interface LogicalPlanVisitor {
    void visit(ScanLogicalOperator scanLogicalOperator);

    void visit(SelectLogicalOperator selectLogicalOperator);

    void visit(DuplicateEliminationLogicalOperator duplicateEliminationLogicalOperator);

    void visit(ProjectLogicalOperator projectLogicalOperator);

    void visit(SortLogicalOperator sortLogicalOperator);

    void visit(JoinLogicalOperator joinLogicalOperator);
}
