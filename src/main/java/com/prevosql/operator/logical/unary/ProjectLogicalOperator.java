package com.prevosql.operator.logical.unary;

import com.prevosql.interpreter.query.plan.visitor.LogicalPlanVisitor;
import com.prevosql.operator.logical.LogicalOperator;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.List;

/**
 * Models a project operator
 */
public class ProjectLogicalOperator extends UnaryLogicalOperator {
    private final List<SelectItem> selectItems;

    /**
     * Constructs a project operator
     *
     * @param child Child to project from
     * @param selectItems Items to project from child
     */
    public ProjectLogicalOperator(LogicalOperator child, List<SelectItem> selectItems) {
        super(child);
        this.selectItems = selectItems;
    }

    /**
     * For use in PhysicalPlanExpressionVisitor
     *
     * @param visitor ExpressionVisitor to call on this object
     */
    @Override
    public void accept(LogicalPlanVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public boolean equals(LogicalOperator other) {
        return other instanceof ProjectLogicalOperator
                && getChild().equals(((ProjectLogicalOperator) other).getChild())
                && selectItems.stream().allMatch(e -> {
                    int index = selectItems.indexOf(e);
                    return ((ProjectLogicalOperator) other).selectItems.get(index).toString().equalsIgnoreCase(e.toString());
                });
    }

    /**
     * @return Items to project from child
     */
    public List<SelectItem> getSelectItems() {
        return selectItems;
    }
}
