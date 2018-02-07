package com.prevosql.interpreter.query.plan.visitor;

import com.prevosql.operator.logical.nary.JoinLogicalOperator;
import com.prevosql.operator.logical.leaf.ScanLogicalOperator;
import com.prevosql.operator.logical.unary.DuplicateEliminationLogicalOperator;
import com.prevosql.operator.logical.unary.ProjectLogicalOperator;
import com.prevosql.operator.logical.unary.SelectLogicalOperator;
import com.prevosql.operator.logical.unary.SortLogicalOperator;
import com.prevosql.util.disjointset.DisjointSetColumn;
import com.prevosql.util.disjointset.Element;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.Iterator;
import java.util.stream.Collectors;

/**
 * Visitor to print out logical query plans
 */
public class LogicalPlanPrinterVisitor implements LogicalPlanVisitor {
    private StringBuilder builder;
    private int level = 0;

    /**
     * Constructs a LogicalPlanPrinterVisitor
     */
    public LogicalPlanPrinterVisitor() {
        builder = new StringBuilder();
    }

    /**
     * @return String representation of logical query plan
     */
    public String getLogicalPlan() {
        return builder.toString();
    }

    /**
     * Prints a scanLogicalOperator
     *
     * @param scanLogicalOperator input operator
     */
    @Override
    public void visit(ScanLogicalOperator scanLogicalOperator) {
        initializeLine();
        builder.append("Leaf[").append(scanLogicalOperator.getTableName()).append("]\n");
    }

    /**
     * Prints a selectLogicalOperator
     *
     * @param selectLogicalOperator input operator
     */
    @Override
    public void visit(SelectLogicalOperator selectLogicalOperator) {
        initializeLine();
        builder.append("Select[").append(selectLogicalOperator.getSelectCondition()).append("]\n");
        selectLogicalOperator.getChild().accept(this);
        level--;
    }

    /**
     * Prints a DuplicateEliminationOperator
     *
     * @param duplicateEliminationLogicalOperator input operator
     */
    @Override
    public void visit(DuplicateEliminationLogicalOperator duplicateEliminationLogicalOperator) {
        initializeLine();
        builder.append("DupElim\n");
        duplicateEliminationLogicalOperator.getChild().accept(this);
        level--;
    }

    /**
     * Prints a ProjectLogicalOperator
     *
     * @param projectLogicalOperator input operator
     */
    @Override
    public void visit(ProjectLogicalOperator projectLogicalOperator) {
        initializeLine();
        builder.append("Project[");
        String res = projectLogicalOperator
                .getSelectItems()
                .stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));
        builder.append(res).append("]\n");
        projectLogicalOperator.getChild().accept(this);
        level--;
    }

    /**
     * Prints a SortLogicalOperator
     *
     * @param sortLogicalOperator input operator
     */
    @Override
    public void visit(SortLogicalOperator sortLogicalOperator) {
        initializeLine();
        builder.append("Sort[");
        for (OrderByElement element : sortLogicalOperator.getOrderByElementList()) {
            builder.append(element.getExpression());
        }
        builder.append("]\n");
        sortLogicalOperator.getChild().accept(this);
        level--;
    }

    /**
     * Prints a JoinLogicalOperator
     *
     * @param joinLogicalOperator input operator
     */
    @Override
    public void visit(JoinLogicalOperator joinLogicalOperator) {
        initializeLine();
        builder.append("Join[").append(joinLogicalOperator.getJoinCondition()).append("]\n");
        for (Iterator<Element> iter = joinLogicalOperator.getSet().getIterator(); iter.hasNext();) {
            builder.append("[[");
            Element element = iter.next();
            for (Iterator<DisjointSetColumn> iter1 = element.getIterator(); iter1.hasNext();) {
                builder.append(iter1.next());
                if (iter1.hasNext()) {
                    builder.append(", ");
                }
            }
            builder.append("], equals ");
            if (element.hasEqualityConstraint()) {
                builder.append(element.getEqualityConstraint()).append(", ");
            } else {
                builder.append("null, ");
            }
            builder.append("min ");
            if (element.hasLowerBound()) {
                int bound = element.getLowerBound();
                if (element.lowerBoundExclusive()) {
                    bound++;
                }
                builder.append(bound).append(", ");
            } else {
                builder.append("null, ");
            }
            builder.append("max ");
            if (element.hasUpperBound()) {
                int bound = element.getUpperBound();
                if (element.upperBoundExclusive()) {
                    bound--;
                }
                builder.append(bound);
            } else {
                builder.append("null");
            }
            builder.append("]\n");
        }
        joinLogicalOperator.getChildren().forEach(op -> {
            op.accept(this);
            level--;
        });
    }

    /**
     * Initializes the line for printing
     */
    private void initializeLine() {
        for (int i = 0; i < level; i++) {
            builder.append("-");
        }
        level++;
    }
}
