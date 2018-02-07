package com.prevosql.interpreter.query.visitor;

import com.prevosql.config.catalog.DBCatalog;
import com.prevosql.operator.physical.PhysicalOperator;
import com.prevosql.operator.physical.binary.join.JoinPhysicalOperator;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.ArrayList;
import java.util.List;

/**
 * Processes join condition and builds a list of conditions for
 * sorting the left child and for sorting the right child
 */
public class SortExpressionVisitor implements ExpressionVisitor {
    private final PhysicalOperator leftChild;
    private final PhysicalOperator rightChild;
    private final List<OrderByElement> leftElements;
    private final List<OrderByElement> rightElements;

    /**
     * Constructs a SortExpressionVisitor
     *
     * @param leftChild Left operator to sort
     * @param rightChild Right operator to sort
     */
    public SortExpressionVisitor(PhysicalOperator leftChild, PhysicalOperator rightChild) {
        this.leftElements = new ArrayList<>();
        this.rightElements = new ArrayList<>();
        this.leftChild = leftChild;
        this.rightChild = rightChild;
    }

    /**
     * @return The sort conditions corresponding to the left operator
     */
    public List<OrderByElement> getLeftElements() {
        return leftElements;
    }

    /**
     * @return The sort conditions corresponding to the right operator
     */
    public List<OrderByElement> getRightElements() {
        return rightElements;
    }

    /**
     * Processes an and-expression. Breaks up the expression and sends
     * it back to the visitor.
     *
     * @param andExpression And-expression to process
     */
    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getRightExpression().accept(this);
        andExpression.getLeftExpression().accept(this);
    }

    /**
     * Processes an equals-to expression, which by our setup is
     * a join condition. Will determine which elements belong to
     * left operator and which belong to right and add the columns
     * to respective lists
     *
     * @param equalsTo Equals-to expression to process
     */
    @Override
    public void visit(EqualsTo equalsTo) {
        Expression left = equalsTo.getLeftExpression();
        Expression right = equalsTo.getRightExpression();
        if (left instanceof Column && right instanceof Column) {
            Column leftColumn = (Column) left;
            String leftTableAlias = leftColumn.getTable().getName();
            String leftTableName = DBCatalog.getInstance().getTableName(leftTableAlias);

            Column rightColumn = (Column) right;
            String rightTableAlias = rightColumn.getTable().getName();
            String rightTableName = DBCatalog.getInstance().getTableName(rightTableAlias);

            if (leftChild.getTableName().equalsIgnoreCase(leftTableName) &&
                    rightChild.getTableName().equalsIgnoreCase(rightTableName)) {
                OrderByElement l = new OrderByElement();
                l.setExpression(left);
                OrderByElement r = new OrderByElement();
                r.setExpression(right);
                leftElements.add(l);
                rightElements.add(r);

            } else if (leftChild.getTableName().equalsIgnoreCase(rightTableName) &&
                    rightChild.getTableName().equalsIgnoreCase(leftTableName)) {
                OrderByElement l = new OrderByElement();
                l.setExpression(right);
                OrderByElement r = new OrderByElement();
                r.setExpression(left);
                leftElements.add(l);
                rightElements.add(r);

            } else {
                // the expression belongs to one of the children, and we have to examine them :(
                // left child is the only one that can be a join operator, so we check it
                // we can also assume that all extraneous join conditions will be handled higher up
                if (rightChild.getTableName().equalsIgnoreCase(rightTableName) && leftChild instanceof JoinPhysicalOperator) {
                    OrderByElement l = new OrderByElement();
                    l.setExpression(left);
                    OrderByElement r = new OrderByElement();
                    r.setExpression(right);
                    leftElements.add(l);
                    rightElements.add(r);

                } else if (leftChild.getTableName().equalsIgnoreCase(leftTableName) && rightChild instanceof JoinPhysicalOperator) {
                    OrderByElement l = new OrderByElement();
                    l.setExpression(left);
                    OrderByElement r = new OrderByElement();
                    r.setExpression(right);
                    leftElements.add(l);
                    rightElements.add(r);
                }
            }
        }
    }

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
    public void visit(GreaterThan greaterThan) {

    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {

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
    public void visit(MinorThan minorThan) {

    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {

    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {

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
