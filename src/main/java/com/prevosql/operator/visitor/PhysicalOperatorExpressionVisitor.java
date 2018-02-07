package com.prevosql.operator.visitor;

import com.prevosql.config.catalog.Catalog;
import com.prevosql.config.catalog.DBCatalog;
import com.prevosql.config.catalog.Table;
import com.prevosql.tuple.Tuple;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * Visitor to build join tree
 */
public class PhysicalOperatorExpressionVisitor implements ExpressionVisitor {
    private final Tuple tuple;
    private boolean satisfied;
    private Catalog catalog;

    /**
     * Initializes OperatorExpressionVisitor
     *
     * @param tuple Input tuple to get information from
     */
    public PhysicalOperatorExpressionVisitor(Tuple tuple, Catalog catalog) {
        this(tuple);
        this.catalog = catalog;
    }

    public PhysicalOperatorExpressionVisitor(Tuple tuple) {
        this.tuple = tuple;
        this.satisfied = false;
        catalog = DBCatalog.getInstance();
    }

    /**
     * Returns whether or not the input leftTuple satisfies the expression
     *
     * @return Whether or not the input leftTuple satisfies the expression
     */
    public boolean getResult() {
        return satisfied;
    }

    public void visit(LongValue longValue) { }

    /**
     * Processes less than expression
     *
     * @param minorThan Input expression
     */
    public void visit(MinorThan minorThan) {
        String value = evaluateExpression(minorThan.getLeftExpression());
        String queried = evaluateExpression(minorThan.getRightExpression());
        if (value == null) {
            satisfied = false;
            return;
        }
        float storedVal = Float.parseFloat(value);
        assert queried != null;
        float queryVal = Float.parseFloat(queried);
        satisfied = storedVal < queryVal;
    }

    public void visit(MinorThanEquals minorThanEquals) {
        String value = evaluateExpression(minorThanEquals.getLeftExpression());
        String queried = evaluateExpression(minorThanEquals.getRightExpression());
        if (value == null) {
            satisfied = false;
            return;
        }
        float storedVal = Float.parseFloat(value);
        assert queried != null;
        float queryVal = Float.parseFloat(queried);
        satisfied = storedVal <= queryVal;
    }

    public void visit(NotEqualsTo notEqualsTo) {
        String value = evaluateExpression(notEqualsTo.getLeftExpression());
        String queried = evaluateExpression(notEqualsTo.getRightExpression());
        if (value == null) {
            satisfied = false;
            return;
        }
        satisfied = !value.equalsIgnoreCase(queried);
    }

    public void visit(Column tableColumn) { }

    public void visit(AndExpression andExpression) {
        PhysicalOperatorExpressionVisitor ov1 = new PhysicalOperatorExpressionVisitor(tuple, catalog);
        andExpression.getLeftExpression().accept(ov1);
        PhysicalOperatorExpressionVisitor ov2;
        ov2 = new PhysicalOperatorExpressionVisitor(tuple, catalog);
        andExpression.getRightExpression().accept(ov2);
        satisfied = ov1.getResult() && ov2.getResult();
    }

    public void visit(EqualsTo equalsTo) {
        String value = evaluateExpression(equalsTo.getLeftExpression());
        String queried = evaluateExpression(equalsTo.getRightExpression());
        if (value == null) {
            satisfied = false;
            return;
        }
        satisfied = value.equalsIgnoreCase(queried);
    }

    public void visit(GreaterThan greaterThan) {
        String value = evaluateExpression(greaterThan.getLeftExpression());
        String queried = evaluateExpression(greaterThan.getRightExpression());
        if (value == null) {
            satisfied = false;
            return;
        }
        float storedValue = Float.parseFloat(value);
        assert queried != null;
        float queryValue = Float.parseFloat(queried);
        satisfied = storedValue > queryValue;
    }

    public void visit(GreaterThanEquals greaterThanEquals) {
        String value = evaluateExpression(greaterThanEquals.getLeftExpression());
        String queried = evaluateExpression(greaterThanEquals.getRightExpression());
        if (value == null) {
            satisfied = false;
            return;
        }
        float storedValue = Float.parseFloat(value);
        assert queried != null;
        float queryValue = Float.parseFloat(queried);
        satisfied = storedValue >= queryValue;
    }

    /**
     * Evaluates an expression and extracts leftTuple information based
     * on the expression
     *
     * @param e Input expression
     * @return Tuple information based on expression
     */
    private String evaluateExpression(Expression e) {
        if (e instanceof LongValue) {
            return e.toString();
        }

        Column column = (Column) e;
        String columnVal = column.getColumnName();

        if (column.getTable() != null) {
            String tableName = column.getTable().getName();
            String fullName;
            if ((fullName = DBCatalog.getInstance().getTableName(tableName)) == null) {
                fullName = tableName;
            }
            Table table = catalog.getTable(fullName);
            int index;
            try {
                index = table.getIndexForColumn(columnVal);
            } catch (NullPointerException n) {
                System.err.println("Invalid column selected");
                return null;
            }

            try {
                return tuple.get(index);

            } catch (NullPointerException n) {
                return null;
            }
        } else {
            return column.toString();
        }
    }

    //----------------------------------------------

    public void visit(NullValue nullValue) {}
    public void visit(Function function) {}
    public void visit(SignedExpression signedExpression) {}
    public void visit(JdbcParameter jdbcParameter) {}
    public void visit(JdbcNamedParameter jdbcNamedParameter) {}
    public void visit(DoubleValue doubleValue) {}
    public void visit(HexValue hexValue) {}
    public void visit(DateValue dateValue) {}
    public void visit(TimeValue timeValue) {}
    public void visit(TimestampValue timestampValue) {}
    public void visit(Parenthesis parenthesis) {}
    public void visit(StringValue stringValue) {}
    public void visit(Addition addition) {}
    public void visit(Division division) {}
    public void visit(Multiplication multiplication) {}
    public void visit(Subtraction subtraction) {}
    public void visit(OrExpression orExpression) {}
    public void visit(Between between) {}
    public void visit(InExpression inExpression) {}
    public void visit(IsNullExpression isNullExpression) {}
    public void visit(LikeExpression likeExpression) {}
    public void visit(SubSelect subSelect) {}
    public void visit(CaseExpression caseExpression) {}
    public void visit(WhenClause whenClause) {}
    public void visit(ExistsExpression existsExpression) {}
    public void visit(AllComparisonExpression allComparisonExpression) {}
    public void visit(AnyComparisonExpression anyComparisonExpression) {}
    public void visit(Concat concat) {}
    public void visit(Matches matches) {}
    public void visit(BitwiseAnd bitwiseAnd) {}
    public void visit(BitwiseOr bitwiseOr) {}
    public void visit(BitwiseXor bitwiseXor) {}
    public void visit(CastExpression cast) {}
    public void visit(Modulo modulo) {}
    public void visit(AnalyticExpression aexpr) {}
    public void visit(WithinGroupExpression wgexpr) {}
    public void visit(ExtractExpression eexpr) {}
    public void visit(IntervalExpression iexpr) {}
    public void visit(OracleHierarchicalExpression oexpr) {}
    public void visit(RegExpMatchOperator rexpr) {}
    public void visit(JsonExpression jsonExpr) {}
    public void visit(RegExpMySQLOperator regExpMySQLOperator) {}
    public void visit(UserVariable var) {}
    public void visit(NumericBind bind) {}
    public void visit(KeepExpression aexpr) {}
    public void visit(MySQLGroupConcat groupConcat) {}
    public void visit(RowConstructor rowConstructor) {}
    public void visit(OracleHint hint) {}
}
