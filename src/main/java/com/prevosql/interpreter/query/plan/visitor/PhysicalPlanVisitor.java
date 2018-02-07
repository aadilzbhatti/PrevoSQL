package com.prevosql.interpreter.query.plan.visitor;

import com.prevosql.operator.physical.binary.join.JoinPhysicalOperator;
import com.prevosql.operator.physical.leaf.IndexScanPhysicalOperator;
import com.prevosql.operator.physical.leaf.ScanPhysicalOperator;
import com.prevosql.operator.physical.unary.SelectPhysicalOperator;
import com.prevosql.operator.physical.unary.sort.SortPhysicalOperator;
import com.prevosql.operator.physical.unary.DuplicateEliminationPhysicalOperator;
import com.prevosql.operator.physical.unary.ProjectPhysicalOperator;

/**
 * Visitor for operating on physical query plans
 */
public interface PhysicalPlanVisitor {
    void visit(ScanPhysicalOperator scanPhysicalOperator);

    void visit(SelectPhysicalOperator selectPhysicalOperator);

    void visit(IndexScanPhysicalOperator indexScanPhysicalOperator);

    void visit(ProjectPhysicalOperator projectPhysicalOperator);

    void visit(SortPhysicalOperator sortPhysicalOperator);

    void visit(JoinPhysicalOperator joinPhysicalOperator);

    void visit(DuplicateEliminationPhysicalOperator duplicateEliminationPhysicalOperator);
}
