package com.prevosql.operator.logical.unary;

import com.prevosql.interpreter.query.plan.visitor.LogicalPlanVisitor;
import com.prevosql.operator.logical.LogicalOperator;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.List;

/**
 * Models a sort operator
 */
public class SortLogicalOperator extends UnaryLogicalOperator {
    private final List<OrderByElement> orderByElementList;

    /**
     * Constructs a sort operator
     *
     * @param child Child operator which will be sorted
     * @param orderByElementList List of elements to sort the child operator by
     */
    public SortLogicalOperator(LogicalOperator child, List<OrderByElement> orderByElementList) {
        super(child);
        this.orderByElementList = orderByElementList;
    }

    @Override
    public void accept(LogicalPlanVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * @return List of elements to sort child operator by
     */
    public List<OrderByElement> getOrderByElementList() {
        return orderByElementList;
    }

    @Override
    public boolean equals(LogicalOperator other) {
        return other instanceof SortLogicalOperator
                && getChild().equals(((SortLogicalOperator) other).getChild())
                && orderByElementList.stream().allMatch(e -> {
                    int index = orderByElementList.indexOf(e);
                    return ((SortLogicalOperator) other).orderByElementList.get(index).toString()
                            .equalsIgnoreCase(e.toString());
                });
    }
}
