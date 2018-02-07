package com.prevosql.operator.physical.unary.sort;

import com.prevosql.config.catalog.DBCatalog;
import com.prevosql.interpreter.query.plan.visitor.PhysicalPlanVisitor;
import com.prevosql.operator.physical.PhysicalOperator;
import com.prevosql.tuple.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.ArrayList;
import java.util.List;

/**
 * Handles sort operators for Order By queries
 */
public class InMemorySortPhysicalOperator extends SortPhysicalOperator {
    private int index;
    private final List<Tuple> tuples;

    /**
     * Initializes SortOperator. Collects all tuples from output
     * and sorts them based on input elements
     *
     * @param child Child operator
     * @param orderByElements List of elements to order by
     */
    public InMemorySortPhysicalOperator(PhysicalOperator child, List<OrderByElement> orderByElements) {
        super(child, orderByElements);
        tuples = new ArrayList<>();

        Tuple t = child.getNextTuple();
        while (t != null) {
            tuples.add(t);
            t = child.getNextTuple();
        }

        tuples.sort((Tuple o1, Tuple o2) -> {
            if (orderByElements == null) {
                return o1.toString().compareTo(o2.toString());
            }

            OrderByElement e0 = orderByElements.get(0);
            int ind0 = getIndex(e0);
            int val = Integer.parseInt(o1.get(ind0)) - Integer.parseInt(o2.get(ind0));
            if (val == 0) {
                for (int i = 1; i < orderByElements.size(); i++) {
                    int index = getIndex(orderByElements.get(i));
                    int value = Integer.parseInt(o1.get(index)) - Integer.parseInt(o2.get(index));

                    if (value != 0) {
                        return value;
                    }
                }
            }
            return val;
        });

        index = 0;
    }

    /**
     * Gets next sorted tuple in output
     *
     * @return Next sorted tuple
     */
    @Override
    public Tuple getNextTuple() {
        if (index == tuples.size()) {
            return null;
        }
        return tuples.get(index++);
    }

    /**
     * Resets child operator
     */
    @Override
    public void reset() {
        child.reset();
    }

    @Override
    public void accept(PhysicalPlanVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Gets column index in tuple based on input order by element
     *
     * @param e Input order by element
     * @return Column index in tuples
     */
    private int getIndex(OrderByElement e) {
        Column col = (Column) e.getExpression();
        String tableName = col.getTable().getName();
        String columnName = col.getColumnName();
        return DBCatalog.getInstance().getTable(tableName).getIndexForColumn(columnName);
    }

    @Override
    public void reset(int index) {
        this.index = index;
    }
}
