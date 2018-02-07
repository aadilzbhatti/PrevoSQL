package com.prevosql.interpreter.query.plan;

import com.prevosql.config.catalog.DBCatalog;
import com.prevosql.interpreter.query.plan.visitor.LogicalPlanPrinterVisitor;
import com.prevosql.operator.logical.LogicalOperator;
import com.prevosql.operator.logical.nary.JoinLogicalOperator;
import com.prevosql.operator.logical.leaf.ScanLogicalOperator;
import com.prevosql.operator.logical.unary.SelectLogicalOperator;
import com.prevosql.util.disjointset.DisjointSet;
import com.prevosql.util.disjointset.DisjointSetColumn;
import com.prevosql.util.disjointset.Element;
import com.prevosql.util.disjointset.DisjointSetExpressionVisitor;
import com.prevosql.operator.logical.unary.DuplicateEliminationLogicalOperator;
import com.prevosql.operator.logical.unary.ProjectLogicalOperator;
import com.prevosql.operator.logical.unary.SortLogicalOperator;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.List;

/**
 * Builds a logical query plan
 */
public class LogicalPlan {
    private LogicalOperator root;

    private static final Logger LOG = Logger.getLogger(LogicalPlan.class);

    /**
     * @return Root of logical query plan
     */
    public LogicalOperator getRoot() {
        return root;
    }

    /**
     * Builds LogicalQueryPlan by parsing a SQL query
     *
     * @param query Input SQL string
     * @throws JSQLParserException If there is an error while parsing
     */
    public LogicalPlan(String query) throws JSQLParserException {
        Statement stmt = CCJSqlParserUtil.parse(query);
        Select select = (Select) stmt;
        PlainSelect ps = (PlainSelect) select.getSelectBody();
        FromItem fromItem = ps.getFromItem();
        String tableName;
        if (fromItem.getAlias() != null) {
            tableName = addAliasToCatalog(fromItem);
        } else {
            tableName = ps.getFromItem().toString();
        }
        Expression exp = ps.getWhere();
        List<Join> joins = ps.getJoins();
        List<SelectItem> selectItems = ps.getSelectItems();

        if (joins != null) {
            addJoinAlias(joins);
            root = processCrossProduct(tableName, joins);
            if (exp != null) {
                DisjointSetExpressionVisitor visitor = new DisjointSetExpressionVisitor();
                exp.accept(visitor);
                DisjointSet set = visitor.getDisjointSet();
                List<BinaryExpression> unusableExpressions = visitor.getUnusableExpressions();
                JoinLogicalOperator joinOp = new JoinLogicalOperator();
                joinOp.setSet(set);
                for (Iterator<Element> iter = set.getIterator(); iter.hasNext();) {
                    Element element = iter.next();
                    for (Iterator<DisjointSetColumn> djIter = element.getIterator(); djIter.hasNext();) {
                        DisjointSetColumn disjointSetColumn = djIter.next();
                        Column column = set.getColumn(disjointSetColumn.getFullName());
                        BinaryExpression result = null;

                        if (element.hasEqualityConstraint()) {
                            EqualsTo equalsTo = new EqualsTo();
                            equalsTo.setLeftExpression(column);
                            equalsTo.setRightExpression(new LongValue(element.getEqualityConstraint()));
                            result = equalsTo;

                        } else if (element.hasLowerBound() && element.hasUpperBound()) {
                            BinaryExpression leftExp;
                            BinaryExpression rightExp;

                            if (element.lowerBoundExclusive()) {
                                element.setLowerBound(element.getLowerBound() + 1);
                                element.setLowerBoundExclusive(false);
                            }
                            GreaterThanEquals greaterThanEquals = new GreaterThanEquals();
                            greaterThanEquals.setLeftExpression(column);
                            greaterThanEquals.setRightExpression(new LongValue(element.getLowerBound()));
                            leftExp = greaterThanEquals;

                            if (element.upperBoundExclusive()) {
                                element.setUpperBound(element.getUpperBound() - 1);
                                element.setUpperBoundExclusive(false);
                            }
                            MinorThanEquals minorThanEquals = new MinorThanEquals();
                            minorThanEquals.setLeftExpression(column);
                            minorThanEquals.setRightExpression(new LongValue(element.getUpperBound()));
                            rightExp = minorThanEquals;

                            result = new AndExpression(leftExp, rightExp);

                        } else if (element.hasLowerBound()) {
                            if (element.lowerBoundExclusive()) {
                                element.setLowerBound(element.getLowerBound() + 1);
                                element.setLowerBoundExclusive(false);
                            }
                            GreaterThanEquals greaterThanEquals = new GreaterThanEquals();
                            greaterThanEquals.setLeftExpression(column);
                            greaterThanEquals.setRightExpression(new LongValue(element.getLowerBound()));
                            result = greaterThanEquals;

                        } else if (element.hasUpperBound()) {
                            if (element.upperBoundExclusive()) {
                                element.setUpperBound(element.getUpperBound() - 1);
                                element.setUpperBoundExclusive(false);
                            }
                            MinorThanEquals minorThanEquals = new MinorThanEquals();
                            minorThanEquals.setLeftExpression(column);
                            minorThanEquals.setRightExpression(new LongValue(element.getUpperBound()));
                            result = minorThanEquals;
                        }

                        if (result == null) {
                            continue;
                        }

                        String fullName = DBCatalog.getInstance().getTableName(column.getTable().getName());
                        if (fullName == null) {
                            fullName = column.getTable().getName();
                        }
                        SelectLogicalOperator op = new SelectLogicalOperator(
                                new ScanLogicalOperator(capitalizeFirstLetter(fullName)), result);
                        joinOp.addChild(op);
                    }
                }
                for (BinaryExpression expression : unusableExpressions) {
                    Expression leftExpression = expression.getLeftExpression();
                    Expression rightExpression = expression.getRightExpression();
                    if (leftExpression instanceof Column && rightExpression instanceof Column) {
                        Column c1 = (Column) leftExpression;
                        Column c2 = (Column) rightExpression;
                        if (!c1.getTable().getName().equalsIgnoreCase(c2.getTable().getName())) {
                            if (joinOp.getJoinCondition() == null) {
                                joinOp.setJoinCondition(expression);
                            } else {
                                Expression joinCondition = joinOp.getJoinCondition();
                                joinOp.setJoinCondition(new AndExpression(joinCondition, expression));
                            }
                        } else {
                            String fullName = DBCatalog.getInstance().getTableName(c1.getTable().getName());
                            if (fullName == null) {
                                fullName = c1.getTable().getName();
                            }
                            SelectLogicalOperator op = new SelectLogicalOperator(
                                    new ScanLogicalOperator(capitalizeFirstLetter(fullName)), expression
                            );
                            joinOp.addChild(op);
                        }
                    } else {
                        Column col;
                        if (leftExpression instanceof Column) {
                            col = (Column) leftExpression;
                        } else {
                            col = (Column) rightExpression;
                        }
                        String fullName = DBCatalog.getInstance().getTableName(col.getTable().getName());
                        if (fullName == null) {
                            fullName = col.getTable().getName();
                        }
                        SelectLogicalOperator op = new SelectLogicalOperator(
                                new ScanLogicalOperator(capitalizeFirstLetter(fullName)), expression
                        );
                        joinOp.addChild(op);
                    }
                }

                String tname = ((Table) fromItem).getName();
                if (!joinOp.containsTable(tname.toLowerCase())) {
                    String fullName = DBCatalog.getInstance().getTableName(tname);
                    if (fullName == null) {
                        fullName = tname;
                    }
                    joinOp.addChild(new ScanLogicalOperator(fullName));
                }

                for (Join join : joins) {
                    tname = ((Table) join.getRightItem()).getName();
                    if (!joinOp.containsTable(tname.toLowerCase())) {
                        String fullName = DBCatalog.getInstance().getTableName(tname);
                        if (fullName == null) {
                            fullName = tname;
                        }
                        joinOp.addChild(new ScanLogicalOperator(fullName));
                    }
                }

                root = joinOp;
            }

        } else {
            String fullName = DBCatalog.getInstance().getTableName(tableName);
            if (fullName == null) {
                fullName = tableName;
            }
            ScanLogicalOperator scanOperator = new ScanLogicalOperator(fullName);
            SelectLogicalOperator selectOperator = new SelectLogicalOperator(scanOperator, exp);
            ProjectLogicalOperator projectOperator = new ProjectLogicalOperator(selectOperator, selectItems);
            if (selectItems != null) {
                if (selectItems.get(0) instanceof AllColumns && exp == null) {
                    root = scanOperator;
                }  else if (exp != null) {
                    root = selectOperator;
                } else {
                    root = projectOperator;
                }

            } else if (exp != null) {
                root = selectOperator;

            } else {
                root = scanOperator;
            }
        }

        if (!(ps.getSelectItems().get(0) instanceof AllColumns)
                && !(root instanceof ProjectLogicalOperator)) {
            LogicalOperator temp = root;
            root = new ProjectLogicalOperator(temp, ps.getSelectItems());
        }

        if (ps.getOrderByElements() != null) {
            LogicalOperator temp = root;
            root = new SortLogicalOperator(temp, ps.getOrderByElements());
        }

        if (ps.getDistinct() != null) {
            LogicalOperator temp = root;
            root = new DuplicateEliminationLogicalOperator(temp);
        }

    }

    /**
     * @return String representation of logical query plan
     */
    public String getLogicalPlan() {
        LogicalPlanPrinterVisitor visitor = new LogicalPlanPrinterVisitor();
        root.accept(visitor);
        return visitor.getLogicalPlan();
    }

    private LogicalOperator processCrossProduct(String leftTable, List<Join> joins) {
        JoinLogicalOperator op = new JoinLogicalOperator();
        op.addChild(new ScanLogicalOperator(leftTable));
        for (Join join : joins) {
            Table t = (Table) join.getRightItem();
            String tableName;
            if (t.getAlias() != null) {
                tableName = t.getAlias().toString().replaceAll("\\s","");
            } else {
                tableName = t.getName();
            }
            op.addChild(new ScanLogicalOperator(tableName));
        }
        return op;
    }

    /**
     * Processes a FromItem and copies any aliased table information into
     * an aliased entry in the config.catalog
     *
     * @param fromItem Holds information about aliased table
     * @return The alias name
     */
    private String addAliasToCatalog(FromItem fromItem) {
        String tableName;
        tableName = ((Table) fromItem).getName();
        String alias = fromItem.getAlias().toString().replaceAll("\\s","");
        DBCatalog.getInstance().copyTable(alias, tableName);
        return alias;
    }

    /**
     * Processes a list of join arguments and adds any alias information
     * to the config.catalog
     *
     * @param joins List of join arguments
     */
    private void addJoinAlias(List<Join> joins) {
        for (Join j : joins) {
            if (j.getRightItem().getAlias() != null) {
                addAliasToCatalog(j.getRightItem());
            }
        }
    }

    private String capitalizeFirstLetter(String original) {
        if (original == null || original.length() == 0) {
            return original;
        }
        return original.substring(0, 1).toUpperCase() + original.substring(1);
    }
}