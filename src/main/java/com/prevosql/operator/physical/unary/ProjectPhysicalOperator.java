package com.prevosql.operator.physical.unary;

import com.prevosql.config.catalog.Catalog;
import com.prevosql.config.catalog.DBCatalog;
import com.prevosql.config.catalog.Table;
import com.prevosql.interpreter.query.plan.visitor.PhysicalPlanVisitor;
import com.prevosql.operator.physical.PhysicalOperator;
import com.prevosql.operator.physical.binary.join.JoinPhysicalOperator;
import com.prevosql.operator.physical.unary.sort.SortPhysicalOperator;
import com.prevosql.tuple.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.List;

/**
 * Handles project operations
 */
public class ProjectPhysicalOperator extends UnaryPhysicalOperator {
    private final PhysicalOperator child;
    private final List<SelectItem> selectItems;
    private Catalog catalog;

    /**
     * Initializes a ProjectOperator
     *
     * @param child Child operator to project
     * @param selectItems List of items to project from child
     */
    public ProjectPhysicalOperator(PhysicalOperator child, List<SelectItem> selectItems) {
        super(child);
        this.child = child;
        this.numAttributes = selectItems.size();
        this.selectItems = selectItems;
    }

    /**
     * Gets next valid tuple with select constraints
     *
     * @return Next valid tuple
     */
    @Override
    public Tuple getNextTuple() {
        setCatalog();
        Tuple t = child.getNextTuple();
        if (t == null) {
            return null;
        }

        int columns = selectItems.size();
        String[] res = new String[columns];
        int index = 0;
        for (SelectItem item : selectItems) {
            if (item instanceof AllColumns) {
                return t;
            }
            SelectExpressionItem exp = (SelectExpressionItem) item;
            Column column = (Column) exp.getExpression();
            String name = column.getColumnName();
            Table table = catalog.getTable(child.getTableName());
            int columnIndex = table.getIndexForColumn(name);
            res[index++] = t.get(columnIndex);
        }

        return new Tuple(res);
    }

    /**
     * Resets child output
     */
    @Override
    public void reset() {
        child.reset();
    }

    @Override
    public void accept(PhysicalPlanVisitor visitor) {
        visitor.visit(this);
    }

    public List<SelectItem> getSelectItems() {
        return selectItems;
    }

    private void setCatalog() {
        if (child instanceof JoinPhysicalOperator) {
            ((JoinPhysicalOperator) child).createJoinedTable();
            catalog = ((JoinPhysicalOperator) child).getCatalog();
        } else if (child instanceof SortPhysicalOperator) {
            catalog = ((SortPhysicalOperator) child).getCatalog();
        } else {
            catalog = DBCatalog.getInstance();
        }
    }
}
