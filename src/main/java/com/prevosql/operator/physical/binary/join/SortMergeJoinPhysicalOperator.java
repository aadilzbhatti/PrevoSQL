package com.prevosql.operator.physical.binary.join;

import com.prevosql.config.catalog.DBCatalog;
import com.prevosql.config.operator.JoinOperator;
import com.prevosql.interpreter.query.plan.visitor.PhysicalPlanVisitor;
import com.prevosql.operator.physical.unary.sort.SortPhysicalOperator;
import com.prevosql.operator.visitor.PhysicalOperatorExpressionVisitor;
import com.prevosql.operator.visitor.SortMergeJoinExpressionVisitor;
import com.prevosql.tuple.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

import java.util.List;

/**
 * Implements the sort-merge join algorithm for performing an
 * equijoin on two relations
 */
public class SortMergeJoinPhysicalOperator extends JoinPhysicalOperator {
    private final List<BinaryExpression> expressions;
    private int rightIndex = 0;
    private int origRightIndex = -1;
    private final SortPhysicalOperator leftChild;
    private final SortPhysicalOperator rightChild;
    private Tuple savedLeft;

    /**
     * Constructs a SMJ physical com.cs5321.operator
     *
     * @param leftChild Left relation, sorted
     * @param rightChild Right relation, sorted
     * @param joinCondition Condition to join relations on
     */
    public SortMergeJoinPhysicalOperator(SortPhysicalOperator leftChild, SortPhysicalOperator rightChild, Expression joinCondition) {
        super(leftChild, rightChild, joinCondition);
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        SortMergeJoinExpressionVisitor smj = new SortMergeJoinExpressionVisitor();
        joinCondition.accept(smj);
        expressions = smj.getExpressions();
        setJoinType(JoinOperator.SORT_MERGE);
    }

    /**
     * Returns the next tuple in the joined relations
     *
     * @return The next tuple in the joined relations
     */
    @Override
    public Tuple getNextTuple() {
        Tuple l;
        if (isFinished) {
            isFinished = false;
            l = leftChild.getNextTuple();
        } else {
            l = savedLeft;
        }

        Tuple r = rightChild.getNextTuple();

        if (!joinedTableCreated) {
            createJoinedTable();
        }

        if (r == null && l != null) {
            rightChild.reset(origRightIndex);
            r = rightChild.getNextTuple();
            rightIndex = origRightIndex;
            origRightIndex = -1;
            l = leftChild.getNextTuple();
        }

        while (l != null && r != null) {
            while (compareTuples(l, r) < 0) {
                if (origRightIndex >= 0) {
                    rightChild.reset(origRightIndex);
                    r = rightChild.getNextTuple();
                    rightIndex = origRightIndex;
                    origRightIndex = -1;
                    l = leftChild.getNextTuple();
                    break;
                }
                l = leftChild.getNextTuple();
            }

            while (compareTuples(l, r) > 0) {
                r = rightChild.getNextTuple();
                rightIndex++;
            }

            while (compareTuples(l, r) == 0) {
                if (l == null && r == null) {
                    break;
                }
                if (origRightIndex < 0) {
                    origRightIndex = rightIndex;
                }
                Tuple newTuple = new Tuple(l, r);
                if (joinCondition == null) {
                    rightIndex++;
                    savedLeft = l;
                    return newTuple;
                }

                PhysicalOperatorExpressionVisitor ov = new PhysicalOperatorExpressionVisitor(newTuple, catalog);
                joinCondition.accept(ov);
                if (ov.getResult()) {
                    rightIndex++;
                    savedLeft = l;
                    return newTuple;
                }
                r = rightChild.getNextTuple();
            }

            if (origRightIndex >= 0) {
                rightIndex = origRightIndex;
                rightChild.reset(origRightIndex);
                r = rightChild.getNextTuple();
                l = leftChild.getNextTuple();
                origRightIndex = -1;
            }
        }

        isFinished = true;
        return null;
    }

    @Override
    public void accept(PhysicalPlanVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Compares two tuples for use in sorting and merging based on
     * join condition
     *
     * @param o1 First tuple to compare
     * @param o2 Second tuple to compare
     * @return 1 if o1 comes after o2, -1 if o2 comes after o1, 0 if o1 == o2 on join condition
     */
    private int compareTuples(Tuple o1, Tuple o2) {
        if (o1 == null && o2 == null) {
            return 0;
        } else if (o1 == null) {
            return 1;
        } else if (o2 == null) {
            return -1;
        }

        if (o1.equals(o2)) {
            return 0;
        }

        BinaryExpression exp = expressions.get(0);
        Column leftColumn = (Column) exp.getLeftExpression();
        Column rightColumn = (Column) exp.getRightExpression();

        String leftColumnTable = leftColumn.getTable().getName();
        String rightColumnTable = rightColumn.getTable().getName();

        Tuple combined = new Tuple(o1, o2);

        int val;
        if (leftColumnTable.equalsIgnoreCase(leftTableName) && rightColumnTable.equalsIgnoreCase(rightTableName)) {
            int indLeft = getIndex(leftColumn);
            int indRight = getIndex(rightColumn);
            val = Integer.parseInt(combined.get(indLeft)) - Integer.parseInt(combined.get(indRight));

        } else {
            int indLeft = getIndex(rightColumn);
            int indRight = getIndex(leftColumn);
            val = Integer.parseInt(combined.get(indLeft)) - Integer.parseInt(combined.get(indRight));
        }

        if (val == 0) {
            for (int i = 1; i < expressions.size(); i++) {
                if (leftColumnTable.equalsIgnoreCase(leftTableName) && rightColumnTable.equalsIgnoreCase(rightTableName)) {
                    int indLeft = getIndex(leftColumn);
                    int indRight = getIndex(rightColumn);
                    val = Integer.parseInt(combined.get(indLeft)) - Integer.parseInt(combined.get(indRight));

                } else {
                    int indLeft = getIndex(rightColumn);
                    int indRight = getIndex(leftColumn);
                    val = Integer.parseInt(combined.get(indLeft)) - Integer.parseInt(combined.get(indRight));
                }

                if (val != 0) {
                    return val;
                }
            }
        }

        return val;
    }

    /**
     * Finds the tuple index of a column in a table
     *
     * @param col Input column
     * @return The tuple index for the column
     */
    private int getIndex(Column col) {
        String tableName = col.getTable().getName();
        String columnName = col.getColumnName();
        String fullName;
        if ((fullName = DBCatalog.getInstance().getTableName(tableName)) != null) {
            return catalog.getTable(fullName).getIndexForColumn(columnName);
        }
        return catalog.getTable(tableName).getIndexForColumn(columnName);
    }
}
