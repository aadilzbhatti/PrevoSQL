package com.prevosql.util.expression;

import com.prevosql.util.disjointset.DisjointSetColumn;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.List;

/**
 * Some utility functions for use when dealing with expressions
 */
public class ExpressionUtils {

    /**
     * Builds an AndExpression from a list of BinaryExpressions
     *
     * @param expressions Input list of expressions to join
     * @return AndExpression built from expressions in the input list
     */
    public static Expression buildExpressionFromList(List<BinaryExpression> expressions) {
        return buildExpressionFromList(expressions, 0);
    }

    /**
     * Builds a list of columns used in an expression
     *
     * @param expression Input expression to extract columns from
     * @return List of columns used in expression
     */
    public static List<DisjointSetColumn> getColumnsFromExpression(BinaryExpression expression) {
        List<DisjointSetColumn> columns = new ArrayList<>();
        if (expression instanceof AndExpression) {
            List<DisjointSetColumn> leftColumns = getColumnsFromExpression((BinaryExpression) expression.getLeftExpression());
            List<DisjointSetColumn> rightColumns = getColumnsFromExpression((BinaryExpression) expression.getRightExpression());
            columns.addAll(leftColumns);
            columns.addAll(rightColumns);

        } else {
            if (expression != null) {
                Column leftColumn = (Column) expression.getLeftExpression();
                Column rightColumn = (Column) expression.getRightExpression();
                columns.add(new DisjointSetColumn(leftColumn));
                columns.add(new DisjointSetColumn(rightColumn));
            }
        }
        return columns;
    }

    /**
     * Find columns in the expression which equate to the input
     * column
     *
     * @param column Input column
     * @param expression Expression which might equate to input column
     * @return List of columns in expression which equate to input column
     */
    public static List<DisjointSetColumn> findEquatingColumns(DisjointSetColumn column, BinaryExpression expression) {
        List<DisjointSetColumn> columns = new ArrayList<>();
        if (expression instanceof AndExpression) {
            List<DisjointSetColumn> leftColumns = findEquatingColumns(column, (BinaryExpression) expression.getLeftExpression());
            List<DisjointSetColumn> rightColumns = findEquatingColumns(column, (BinaryExpression) expression.getRightExpression());
            columns.addAll(leftColumns);
            columns.addAll(rightColumns);

        } else {
            if (expression instanceof EqualsTo) {
                Column leftColumn = (Column) expression.getLeftExpression();
                Column rightColumn = (Column) expression.getRightExpression();
                DisjointSetColumn leftDisjointColumn = new DisjointSetColumn(leftColumn);
                DisjointSetColumn rightDisjointColumn = new DisjointSetColumn(rightColumn);

                if (leftDisjointColumn.equals(column)) {
                    columns.add(rightDisjointColumn);

                } else if (rightDisjointColumn.equals(column)) {
                    columns.add(leftDisjointColumn);
                }

            }
        }

        return columns;
    }

    /**
     * All binary expressions in this expression
     *
     * @param expression Input expression
     * @return All binary expressions in this expression
     */
    public static List<BinaryExpression> getAllExpressions(BinaryExpression expression) {
        List<BinaryExpression> ret = new ArrayList<>();
        if (expression instanceof AndExpression) {
            ret.addAll(getAllExpressions((BinaryExpression) expression.getLeftExpression()));
            ret.addAll(getAllExpressions((BinaryExpression) expression.getRightExpression()));
        } else {
            ret.add(expression);
        }
        return ret;
    }

    /**
     * Builds an AndExpression from a list of BinaryExpressions from index i to
     * n, where n is (size of the list) - 1
     *
     * @param expressions Input list of expressions to join
     * @param startIndex Index of first expression to add to AndExpression
     * @return AndExpression built from expressions in the input list
     */
    private static Expression buildExpressionFromList(List<BinaryExpression> expressions, int startIndex) {
        if (startIndex == expressions.size() - 1) {
            return expressions.get(startIndex);
        }
        if (expressions.size() == 0) {
            return null;
        }
        return new AndExpression(expressions.get(startIndex), buildExpressionFromList(expressions, startIndex + 1));
    }
}
