package com.prevosql.interpreter.query.plan.visitor;

import com.prevosql.operator.physical.binary.join.JoinPhysicalOperator;
import com.prevosql.operator.physical.leaf.IndexScanPhysicalOperator;
import com.prevosql.operator.physical.leaf.ScanPhysicalOperator;
import com.prevosql.operator.physical.unary.SelectPhysicalOperator;
import com.prevosql.operator.physical.unary.sort.SortPhysicalOperator;
import com.prevosql.operator.physical.unary.DuplicateEliminationPhysicalOperator;
import com.prevosql.operator.physical.unary.ProjectPhysicalOperator;

import java.util.stream.Collectors;

/**
 * Visitor that prints physical query plans
 */
public class PhysicalPlanPrinterVisitor implements PhysicalPlanVisitor {
    private StringBuilder builder;
    private int level = 0;

    /**
     * Constructs a PhysicalPlanPrinter
     */
    public PhysicalPlanPrinterVisitor() {
        builder = new StringBuilder();
    }

    /**
     * @return A string representation of the physical query plan
     */
    public String getPhysicalPlan() {
        return builder.toString();
    }

    @Override
    public void visit(ScanPhysicalOperator scanPhysicalOperator) {
        initializeLine();
        builder.append("TableScan[").append(scanPhysicalOperator.getTableName()).append("]\n");
    }

    @Override
    public void visit(SelectPhysicalOperator selectPhysicalOperator) {
        initializeLine();
        builder.append("Select[").append(selectPhysicalOperator.getSelectCondition()).append("]\n");
        selectPhysicalOperator.getChild().accept(this);
        level--;
    }

    @Override
    public void visit(IndexScanPhysicalOperator indexScanPhysicalOperator) {
        initializeLine();
        builder.append("IndexScan[").append(indexScanPhysicalOperator.getInfo()).append("]\n");
    }

    @Override
    public void visit(ProjectPhysicalOperator projectPhysicalOperator) {
        initializeLine();
        builder.append("Project[");
        String res = projectPhysicalOperator
                .getSelectItems()
                .stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));
        builder.append(res).append("]\n");
        projectPhysicalOperator.getChild().accept(this);
        level--;
    }

    @Override
    public void visit(SortPhysicalOperator sortPhysicalOperator) {
        initializeLine();
        builder.append("ExternalSort[");
        String res = sortPhysicalOperator
                .getOrderByElements()
                .stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));
        builder.append(res).append("]\n");
        sortPhysicalOperator.getChild().accept(this);
        level--;
    }

    @Override
    public void visit(JoinPhysicalOperator joinPhysicalOperator) {
        initializeLine();
        builder.append(joinPhysicalOperator.getJoinType())
                .append("[")
                .append(joinPhysicalOperator.getJoinCondition())
                .append("]\n");
        joinPhysicalOperator.getLeftChild().accept(this);
        level--;
        joinPhysicalOperator.getRightChild().accept(this);
        level--;
    }

    @Override
    public void visit(DuplicateEliminationPhysicalOperator duplicateEliminationPhysicalOperator) {
        initializeLine();
        builder.append("DupElim\n");
        duplicateEliminationPhysicalOperator.getChild().accept(this);
        level--;
    }

    private void initializeLine() {
        for (int i = 0; i < level; i++) {
            builder.append("-");
        }
        level++;
    }
}
