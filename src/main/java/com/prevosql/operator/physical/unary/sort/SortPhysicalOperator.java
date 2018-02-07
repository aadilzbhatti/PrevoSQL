package com.prevosql.operator.physical.unary.sort;

import com.prevosql.config.catalog.Catalog;
import com.prevosql.operator.physical.PhysicalOperator;
import com.prevosql.operator.physical.unary.UnaryPhysicalOperator;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.List;

/**
 * Models sort operators
 */
public abstract class SortPhysicalOperator extends UnaryPhysicalOperator {
    final PhysicalOperator child;
    final List<OrderByElement> orderByElements;
    Catalog catalog;

    /**
     * Constructs a SortPhysicalOperator
     *
     * @param child Operator to sort
     * @param orderByElements Elements to sort child by
     */
    SortPhysicalOperator(PhysicalOperator child, List<OrderByElement> orderByElements) {
        super(child);
        this.child = child;
        this.orderByElements = orderByElements;
    }

    /**
     * Resets the operator to a particular tuple
     *
     * @param index Tuple index to reset to
     */
    public abstract void reset(int index);

    /**
     * @return Elements to sort by
     */
    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    /**
     * @return Child operator
     */
    public PhysicalOperator getChild() {
        return child;
    }

    /**
     * @return Column/Table catalog
     */
    public Catalog getCatalog() {
        return catalog;
    }

    /**
     * Set the column/table catalog
     *
     * @param catalog Catalog to replace current one with
     */
    public void setCatalog(Catalog catalog) {
        this.catalog = catalog;
    }
}
