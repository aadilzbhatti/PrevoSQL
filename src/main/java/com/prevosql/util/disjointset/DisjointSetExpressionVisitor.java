package com.prevosql.util.disjointset;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.ArrayList;
import java.util.List;

/**
 * Visitor to build our disjoint set and find expressions
 * that can't be used in the union-find
 */
public class DisjointSetExpressionVisitor implements ExpressionVisitor {
    private DisjointSet set;
    private List<BinaryExpression> unusableExpressions;

    public DisjointSetExpressionVisitor() {
        set = new DisjointSet();
        unusableExpressions = new ArrayList<>();
    }

    public DisjointSet getDisjointSet() {
        return set;
    }

    public List<BinaryExpression> getUnusableExpressions() {
        return unusableExpressions;
    }

    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        andExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        Expression leftExpression = equalsTo.getLeftExpression();
        Expression rightExpression = equalsTo.getRightExpression();
        if (leftExpression instanceof Column && rightExpression instanceof Column) {
            if (((Column) leftExpression).getTable().getName().equalsIgnoreCase(((Column) rightExpression).getTable().getName())) {
                unusableExpressions.add(equalsTo);

            } else {
                mergeColumns((Column) leftExpression, (Column) rightExpression);
            }

        } else {
            if (leftExpression instanceof Column) {
                addEqualityConstraint((Column) leftExpression, (LongValue) rightExpression);

            } else if (rightExpression instanceof Column) {
                addEqualityConstraint((Column) rightExpression, (LongValue) leftExpression);
            }
        }
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        Expression leftExpression = greaterThan.getLeftExpression();
        Expression rightExpression = greaterThan.getRightExpression();
        if (leftExpression instanceof Column && rightExpression instanceof Column) {
            unusableExpressions.add(greaterThan);

        } else if (leftExpression instanceof Column) {
            setLowerBound((Column) leftExpression, (LongValue) rightExpression, true);

        } else if (rightExpression instanceof Column) {
            setLowerBound((Column) rightExpression, (LongValue) leftExpression, true);
        }
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        Expression leftExpression = greaterThanEquals.getLeftExpression();
        Expression rightExpression = greaterThanEquals.getRightExpression();
        if (leftExpression instanceof Column && rightExpression instanceof Column) {
            unusableExpressions.add(greaterThanEquals);

        } else if (leftExpression instanceof Column) {
            setLowerBound((Column) leftExpression, (LongValue) rightExpression, false);

        } else if (rightExpression instanceof Column) {
            setLowerBound((Column) rightExpression, (LongValue) leftExpression, false);
        }
    }

    @Override
    public void visit(MinorThan minorThan) {
        Expression leftExpression = minorThan.getLeftExpression();
        Expression rightExpression = minorThan.getRightExpression();
        if (leftExpression instanceof Column && rightExpression instanceof Column) {
            unusableExpressions.add(minorThan);

        } else if (leftExpression instanceof Column) {
            setUpperBound((Column) leftExpression, (LongValue) rightExpression, true);

        } else if (rightExpression instanceof Column) {
            setUpperBound((Column) rightExpression, (LongValue) leftExpression, true);
        }
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        Expression leftExpression = minorThanEquals.getLeftExpression();
        Expression rightExpression = minorThanEquals.getRightExpression();
        if (leftExpression instanceof Column && rightExpression instanceof Column) {
            unusableExpressions.add(minorThanEquals);

        } else if (leftExpression instanceof Column) {
            setUpperBound((Column) leftExpression, (LongValue) rightExpression, false);

        } else if (rightExpression instanceof Column) {
            setUpperBound((Column) rightExpression, (LongValue) leftExpression, false);
        }
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        unusableExpressions.add(notEqualsTo);
    }


    private void mergeColumns(Column leftColumn, Column rightColumn) {
        if (set.find(leftColumn) == null) {
            set.makeSet(leftColumn);
        }
        if (set.find(rightColumn) == null) {
            set.makeSet(rightColumn);
        }
        set.mergeSets(leftColumn, rightColumn);
    }

    private void addEqualityConstraint(Column columnExp, LongValue valueExp) {
        Element element;
        int val = (int) valueExp.getValue();
        if ((element = set.find(columnExp)) == null) {
            element = set.makeSet(columnExp);
        }
        element.setEqualityConstraint(val);
    }

    private void setLowerBound(Column columnExp, LongValue valueExp, boolean exclusive) {
        Element element;
        if ((element = set.find(columnExp)) == null) {
            element = set.makeSet(columnExp);
        }
        int val = (int) (valueExp).getValue();
        if (val > element.getLowerBound()) {
            element.setLowerBound(val);
            if (exclusive) {
                element.setLowerBoundExclusive(true);
            }
        }
    }

    private void setUpperBound(Column columnExp, LongValue valueExp, boolean exclusive) {
        Element element;
        if ((element = set.find(columnExp)) == null) {
            element = set.makeSet(columnExp);
        }
        int val = (int) (valueExp).getValue();
        if (val < element.getUpperBound()) {
            element.setUpperBound(val);
            if (exclusive) {
                element.setUpperBoundExclusive(true);
            }
        }
    }

    // ---------------------------------------

    @Override
    public void visit(NullValue nullValue) {

    }

    @Override
    public void visit(Function function) {

    }

    @Override
    public void visit(SignedExpression signedExpression) {

    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {

    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {

    }

    @Override
    public void visit(DoubleValue doubleValue) {

    }

    @Override
    public void visit(LongValue longValue) {

    }

    @Override
    public void visit(HexValue hexValue) {

    }

    @Override
    public void visit(DateValue dateValue) {

    }

    @Override
    public void visit(TimeValue timeValue) {

    }

    @Override
    public void visit(TimestampValue timestampValue) {

    }

    @Override
    public void visit(Parenthesis parenthesis) {

    }

    @Override
    public void visit(StringValue stringValue) {

    }

    @Override
    public void visit(Addition addition) {

    }

    @Override
    public void visit(Division division) {

    }

    @Override
    public void visit(Multiplication multiplication) {

    }

    @Override
    public void visit(Subtraction subtraction) {

    }

    @Override
    public void visit(OrExpression orExpression) {

    }

    @Override
    public void visit(Between between) {

    }

    @Override
    public void visit(InExpression inExpression) {

    }

    @Override
    public void visit(IsNullExpression isNullExpression) {

    }

    @Override
    public void visit(LikeExpression likeExpression) {

    }

    @Override
    public void visit(Column tableColumn) {

    }

    @Override
    public void visit(SubSelect subSelect) {

    }

    @Override
    public void visit(CaseExpression caseExpression) {

    }

    @Override
    public void visit(WhenClause whenClause) {

    }

    @Override
    public void visit(ExistsExpression existsExpression) {

    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {

    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {

    }

    @Override
    public void visit(Concat concat) {

    }

    @Override
    public void visit(Matches matches) {

    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {

    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {

    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {

    }

    @Override
    public void visit(CastExpression cast) {

    }

    @Override
    public void visit(Modulo modulo) {

    }

    @Override
    public void visit(AnalyticExpression aexpr) {

    }

    @Override
    public void visit(WithinGroupExpression wgexpr) {

    }

    @Override
    public void visit(ExtractExpression eexpr) {

    }

    @Override
    public void visit(IntervalExpression iexpr) {

    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {

    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {

    }

    @Override
    public void visit(JsonExpression jsonExpr) {

    }

    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator) {

    }

    @Override
    public void visit(UserVariable var) {

    }

    @Override
    public void visit(NumericBind bind) {

    }

    @Override
    public void visit(KeepExpression aexpr) {

    }

    @Override
    public void visit(MySQLGroupConcat groupConcat) {

    }

    @Override
    public void visit(RowConstructor rowConstructor) {

    }

    @Override
    public void visit(OracleHint hint) {

    }
}
