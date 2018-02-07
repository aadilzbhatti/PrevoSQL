package com.prevosql.operator.physical.binary.join;

import com.prevosql.config.catalog.DBCatalog;
import com.prevosql.config.catalog.JoinCatalog;
import com.prevosql.config.operator.JoinOperator;
import com.prevosql.operator.physical.PhysicalOperator;
import com.prevosql.operator.physical.binary.BinaryPhysicalOperator;
import com.prevosql.operator.physical.unary.sort.SortPhysicalOperator;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;

import java.util.*;

/**
 * Models join physical operators
 */
public abstract class JoinPhysicalOperator extends BinaryPhysicalOperator {
    final Expression joinCondition;
    String leftTableName;
    String rightTableName;
    private JoinOperator joinType;
    boolean joinedTableCreated = false;
    boolean isFinished = true;
    JoinCatalog catalog;

    JoinPhysicalOperator(PhysicalOperator leftChild, PhysicalOperator rightChild, Expression joinCondition) {
        super(leftChild, rightChild);
        this.joinCondition = joinCondition;
        this.catalog = new JoinCatalog(DBCatalog.getInstance());
        getTableNames();
    }

    public JoinCatalog getCatalog() {
        return catalog;
    }

    public String getLeftTableName() {
        return leftTableName;
    }

    public String getRightTableName() {
        return rightTableName;
    }

    public String getJoinType() {
        if (joinType == JoinOperator.SORT_MERGE) {
            return "SMJ";
        } else {
            return "BNLJ";
        }
    }

    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
    }

    /**
     * Creates a joined table from the child tables. If
     * there is a join as one of the children, creates their
     * joined table as well.
     */
    public void createJoinedTable() {
        if (!joinedTableCreated) {
            if (leftChild instanceof JoinPhysicalOperator && rightChild instanceof JoinPhysicalOperator) {
                ((JoinPhysicalOperator) leftChild).createJoinedTable();
                ((JoinPhysicalOperator) rightChild).createJoinedTable();
                catalog = JoinCatalog.combineCatalogs(
                        ((JoinPhysicalOperator) leftChild).getCatalog(),
                        ((JoinPhysicalOperator) rightChild).getCatalog(),
                        leftChild.getTableName(),
                        rightChild.getTableName());

            } else if (rightChild instanceof JoinPhysicalOperator) {
                JoinPhysicalOperator right = (JoinPhysicalOperator) rightChild;
                right.createJoinedTable();
                catalog = JoinCatalog.addToCatalog(right.getCatalog(), right, leftChild.getTableName());

            } else if (leftChild instanceof JoinPhysicalOperator) {
                JoinPhysicalOperator left = (JoinPhysicalOperator) leftChild;
                left.createJoinedTable();
                catalog = JoinCatalog.addToCatalog(left.getCatalog(), left, rightChild.getTableName());
            } else if (leftChild instanceof SortPhysicalOperator && rightChild instanceof SortPhysicalOperator) {
                if (((SortPhysicalOperator) leftChild).getChild() instanceof JoinPhysicalOperator
                        && ((SortPhysicalOperator) rightChild).getChild() instanceof JoinPhysicalOperator) {
                    ((JoinPhysicalOperator) ((SortPhysicalOperator) leftChild).getChild()).createJoinedTable();
                    ((JoinPhysicalOperator) ((SortPhysicalOperator) rightChild).getChild()).createJoinedTable();
                    catalog = JoinCatalog.combineCatalogs(
                            ((JoinPhysicalOperator) ((SortPhysicalOperator) leftChild).getChild()).getCatalog(),
                            ((JoinPhysicalOperator) ((SortPhysicalOperator) rightChild).getChild()).getCatalog(),
                            ((SortPhysicalOperator) leftChild).getChild().getTableName(),
                            ((SortPhysicalOperator) rightChild).getChild().getTableName()
                    );
                } else if (((SortPhysicalOperator) leftChild).getChild() instanceof JoinPhysicalOperator) {
                    JoinPhysicalOperator left = (JoinPhysicalOperator) ((SortPhysicalOperator) leftChild).getChild();
                    left.createJoinedTable();
                    catalog = JoinCatalog.addToCatalog(left.getCatalog(), left, ((SortPhysicalOperator) rightChild).getChild().getTableName());
                } else if (((SortPhysicalOperator) leftChild).getChild() instanceof JoinPhysicalOperator) {
                    JoinPhysicalOperator right = (JoinPhysicalOperator) ((SortPhysicalOperator) rightChild).getChild();
                    right.createJoinedTable();
                    catalog = JoinCatalog.addToCatalog(right.getCatalog(), right, ((SortPhysicalOperator) leftChild).getChild().getTableName());
                } else {
                    catalog.createJoinedTable(leftChild.getTableName(), rightChild.getTableName());
                }

            } else {
                catalog.createJoinedTable(leftChild.getTableName(), rightChild.getTableName());
            }
            joinedTableCreated = true;
        }
    }

    private void getTableNames() {
        if (leftChild instanceof JoinPhysicalOperator) {
            ((JoinPhysicalOperator) leftChild).getTableNames();
        }

        if (joinCondition instanceof AndExpression) {
            Set<String> tablesInExpression = getTablesInExpression((BinaryExpression) joinCondition);
            if (tablesInExpression.contains(leftChild.getTableName()) && tablesInExpression.size() > 2) {
                tablesInExpression.remove(leftChild.getTableName());
            }
            int i = 0;
            for (Iterator<String> iter = tablesInExpression.iterator(); iter.hasNext();) {
                if (i == 2) {
                    break;
                }
                String table = iter.next();
                if (i == 0) {
                    leftTableName = table;
                } else {
                    rightTableName = table;
                }
                i++;
            }
        } else {
            leftTableName = leftChild.getTableName();
            rightTableName = rightChild.getTableName();
        }
    }

    /**
     * Sets the type of this join operator
     *
     * @param joinType A JoinOperator, either SORT_MERGE or BLOCK_NESTED_LOOP
     */
    void setJoinType(JoinOperator joinType) {
        this.joinType = joinType;
    }

    /**
     * @return The condition we are joining on
     */
    public Expression getJoinCondition() {
        return joinCondition;
    }

    /**
     * Finds all tables used in an expression
     *
     * @param expression Expression to extract tables from
     * @return Set of tables in expression
     */
    private Set<String> getTablesInExpression(BinaryExpression expression) {
        Set<String> ret = new HashSet<>();
        if (expression instanceof AndExpression) {
            ret.addAll(getTablesInExpression((BinaryExpression) expression.getLeftExpression()));
            ret.addAll(getTablesInExpression((BinaryExpression) expression.getRightExpression()));

        } else {
            Column leftColumn = (Column) expression.getLeftExpression();
            Column rightColumn = (Column) expression.getRightExpression();
            String leftTable = leftColumn.getTable().getName();
            String rightTable = rightColumn.getTable().getName();
            ret.add(leftTable);
            ret.add(rightTable);
        }

        return ret;
    }
}
