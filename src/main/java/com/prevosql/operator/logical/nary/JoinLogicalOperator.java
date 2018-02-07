package com.prevosql.operator.logical.nary;

import com.prevosql.config.catalog.DBCatalog;
import com.prevosql.interpreter.query.plan.visitor.LogicalPlanVisitor;
import com.prevosql.operator.logical.LogicalOperator;
import com.prevosql.util.disjointset.DisjointSet;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinLogicalOperator implements LogicalOperator {
    private List<LogicalOperator> children;
    private Expression joinCondition;
    private Map<String, Boolean> tableMap;
    private DisjointSet set;

    public JoinLogicalOperator() {
        children = new ArrayList<>();
        tableMap = new HashMap<>();
        set = new DisjointSet();
    }

    public void addChild(LogicalOperator operator) {
        children.add(operator);
        String fullName;
        if (operator instanceof JoinLogicalOperator) {
            addTables((JoinLogicalOperator) operator);
        } else {
            if ((fullName = DBCatalog.getInstance().getTableName(operator.getTableName())) != null) {
                tableMap.put(fullName.toLowerCase(), true);
            } else {
                tableMap.put(operator.getTableName().toLowerCase(), true);
            }
        }
    }

    @Override
    public void accept(LogicalPlanVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String getTableName() {
        return null;
    }

    @Override
    public boolean equals(LogicalOperator other) {
        return other instanceof JoinLogicalOperator
                && ((JoinLogicalOperator) other).tableMap.equals(tableMap)
                && set.equals(((JoinLogicalOperator) other).set)
                && joinCondition.toString().equalsIgnoreCase(((JoinLogicalOperator) other).joinCondition.toString())
                && ((JoinLogicalOperator) other).children.stream().allMatch(e -> {
                    int index = ((JoinLogicalOperator) other).children.indexOf(e);
                    return children.get(index).equals(e);
                });
    }

    public List<LogicalOperator> getChildren() {
        return children;
    }

    public void setJoinCondition(Expression joinCondition) {
        this.joinCondition = joinCondition;
    }

    public Expression getJoinCondition() {
        return joinCondition;
    }

    public boolean containsTable(String tableName) {
        String fullName;
        if ((fullName = DBCatalog.getInstance().getTableName(tableName)) != null) {
            if (tableMap.get(fullName.toLowerCase()) != null) {
                return true;
            }
        }
        if (tableMap.get(tableName.toLowerCase()) == null) {
            return false;
        }
        return tableMap.get(tableName.toLowerCase());
    }

    public DisjointSet getSet() {
        return set;
    }

    public void setSet(DisjointSet set) {
        this.set = set;
    }

    private void addTables(JoinLogicalOperator operator) {
        operator.getChildren().forEach(op -> {
            if (op instanceof JoinLogicalOperator) {
                addTables((JoinLogicalOperator) op);
            } else {
                String fullName;
                if ((fullName = DBCatalog.getInstance().getTableName(op.getTableName())) != null) {
                    tableMap.put(fullName.toLowerCase(), true);
                } else {
                    tableMap.put(op.getTableName().toLowerCase(), true);
                }
            }
        });
    }
}
