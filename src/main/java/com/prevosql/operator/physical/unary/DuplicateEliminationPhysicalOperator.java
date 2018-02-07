package com.prevosql.operator.physical.unary;

import com.prevosql.config.Configuration;
import com.prevosql.interpreter.query.plan.visitor.PhysicalPlanVisitor;
import com.prevosql.operator.physical.PhysicalOperator;
import com.prevosql.operator.physical.unary.sort.ExternalSortPhysicalOperator;
import com.prevosql.operator.physical.unary.sort.SortPhysicalOperator;
import com.prevosql.tuple.Tuple;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.List;

/**
 * Handles distinct elements
 */
public class DuplicateEliminationPhysicalOperator extends UnaryPhysicalOperator {
    private final SortPhysicalOperator sortedChild;
    private Tuple lastSeenTuple;

    /**
     * Builds a DuplicateEliminationOperator
     *
     * @param child Input operator
     */
    public DuplicateEliminationPhysicalOperator(PhysicalOperator child) {
        super(child);

        int numSortBufferPages = Configuration.getPhysicalConfig().getNumSortBufferPages();
        if (child instanceof SortPhysicalOperator) {
            List<OrderByElement> orderByElements = ((SortPhysicalOperator) child).getOrderByElements();
            this.sortedChild = new ExternalSortPhysicalOperator(
                    child, orderByElements, numSortBufferPages
            );
        } else {
            this.sortedChild = new ExternalSortPhysicalOperator(
                    child, null, numSortBufferPages
            );
        }
    }

    /**
     * Gets the next distinct tuple from the output
     *
     * @return Next distinct tuple
     */
    @Override
    public Tuple getNextTuple() {
        Tuple t = sortedChild.getNextTuple();
        if (t == null) {
            return null;
        }
        if (lastSeenTuple == null) {
            lastSeenTuple = t;
            return t;
        } else {
            if (lastSeenTuple.equals(t)) {
                return getNextTuple();
            } else {
                lastSeenTuple = t;
                return t;
            }
        }
    }

    /**
     * Resets the operator
     */
    @Override
    public void reset() {
        sortedChild.reset();
    }

    @Override
    public void accept(PhysicalPlanVisitor visitor) {
        visitor.visit(this);
    }
}
