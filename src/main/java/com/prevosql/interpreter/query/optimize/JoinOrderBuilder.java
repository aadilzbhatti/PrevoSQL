package com.prevosql.interpreter.query.optimize;

import com.prevosql.config.Configuration;
import com.prevosql.config.statistics.AttributeStatistic;
import com.prevosql.config.statistics.RelationStatistic;
import com.prevosql.operator.logical.LogicalOperator;
import com.prevosql.operator.logical.nary.JoinLogicalOperator;
import com.prevosql.operator.logical.leaf.ScanLogicalOperator;
import com.prevosql.operator.logical.unary.SelectLogicalOperator;
import com.prevosql.util.disjointset.DisjointSet;
import com.prevosql.util.disjointset.DisjointSetColumn;
import com.prevosql.util.disjointset.Element;
import com.prevosql.util.expression.ExpressionUtils;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Builds a join order that minimizes I/O cost
 */
public class JoinOrderBuilder {
    private static Map<List<LogicalOperator>, Integer> costMap;
    private static Map<List<LogicalOperator>, Expression> joinConditionsMap;
    private static JoinLogicalOperator minCostJoinOrder;
    private static JoinLogicalOperator joinLogicalOperator;
    private static Map<DisjointSetColumn, Integer> distinctValues;
    private static Map<List<LogicalOperator>, LogicalOperator> joinOrderMap;
    private static Map<String, BinaryExpression> stringBinaryExpressionMap;

    /**
     * @return Left-deep join tree of minimum cost out of all potential join trees
     */
    public static JoinLogicalOperator getLowestCostJoinOrder() {
        return minCostJoinOrder;
    }

    /**
     * Builds a join order of minimum I/O cost
     *
     * @param operator JoinLogicalOperator to optimize
     */
    public static void buildJoinOrder(JoinLogicalOperator operator) {
        joinLogicalOperator = operator;
        costMap = new HashMap<>();
        joinConditionsMap = new HashMap<>();
        distinctValues = new HashMap<>();
        joinOrderMap = new HashMap<>();
        stringBinaryExpressionMap = new HashMap<>();

        Set<BinaryExpression> waitingConditions = waitingJoinConditions(operator.getSet());
        waitingConditions.forEach(cond -> stringBinaryExpressionMap.put(cond.toString(), cond));

        List<List<LogicalOperator>> ops = new ArrayList<>();

        for (LogicalOperator op1 : operator.getChildren()) {
            ops.add(Collections.singletonList(op1));
            for (LogicalOperator op2 : operator.getChildren()) {
                if (!op1.equals(op2)) {
                    ops.add(Arrays.asList(op1, op2));
                }
            }
        }

        List<LogicalOperator> mutableChildren = new ArrayList<>(operator.getChildren());
        List<List<LogicalOperator>> permutations = permute(mutableChildren);
        ops.addAll(permutations);

        permutations.forEach(p -> costMap.put(p, 0));

        for (List<LogicalOperator> opList : ops) {
            costMap.put(opList, costOfPlan(opList));
        }

        List<LogicalOperator> minCostPlan = null;
        int totalSize = joinLogicalOperator.getChildren().size();
        int minCost = Integer.MAX_VALUE;
        for (List<LogicalOperator> joinOrder : costMap.keySet()) {
            int cost = costMap.get(joinOrder);
            if (cost < minCost && joinOrder.size() == totalSize) {
                minCost = cost;
                minCostPlan = joinOrder;
            }
        }

        minCostJoinOrder = (JoinLogicalOperator) joinOrderMap.get(minCostPlan);
        List<BinaryExpression> allJoinConditions = getAllConditions(minCostJoinOrder);
        List<BinaryExpression> remainingConditions = new ArrayList<>(stringBinaryExpressionMap.values());
        List<BinaryExpression> unusedConditions = new ArrayList<>();
        for (BinaryExpression remaining : remainingConditions) {
            boolean in = false;
            for (BinaryExpression cond : allJoinConditions) {
                if (cond.toString().equalsIgnoreCase(remaining.toString())) {
                    in = true;
                    break;
                }
            }
            if (!in) {
                unusedConditions.add(remaining);
            }
        }
        unusedConditions.add((BinaryExpression) joinLogicalOperator.getJoinCondition());
        Expression joinCondition = ExpressionUtils.buildExpressionFromList(unusedConditions);
        minCostJoinOrder.setJoinCondition(joinCondition);
    }

    /**
     * Builds a list of all permutations of a given list. Taken from
     * https://stackoverflow.com/a/10305419/5964547
     *
     * @param ops List to generate permutations from
     * @return List of permutations of input list
     */
    private static List<List<LogicalOperator>> permute(List<LogicalOperator> ops) {
        if (ops.size() == 0) {
            List<List<LogicalOperator>> result = new ArrayList<>();
            result.add(new ArrayList<>());
            return result;
        }

        LogicalOperator firstElement = ops.remove(0);
        List<List<LogicalOperator>> returnValue = new ArrayList<>();
        List<List<LogicalOperator>> permutations = permute(ops);

        for (List<LogicalOperator> smallerPermutated : permutations) {
            for (int index = 0; index <= smallerPermutated.size(); index++) {
                List<LogicalOperator> temp = new ArrayList<>(smallerPermutated);
                temp.add(index, firstElement);
                returnValue.add(temp);
            }
        }
        return returnValue;
    }

    /**
     * Computes the cost of a plan, where a plan is a list of logical operators.
     * The first two elements of the list are children of one join, the first two
     * and the third are children of another, etc.
     *
     * @param plan Join order plan to evaluate
     * @return The relative cost of executing this plan
     */
    private static int costOfPlan(List<LogicalOperator> plan) {
        if (plan.size() < 3) {
            return 0;
        }

        int totalCost = 0;
        for (int i = 3; i < plan.size() + 1; i++) {
            List<LogicalOperator> first = plan.subList(0, i - 1);
            int cost;
            costMap.putIfAbsent(first, 0);
            if (costMap.get(first) == 0) {
                if (first.size() == 2) {
                    cost = costOfTwoPlans(first.get(0), first.get(1));
                    buildJoinLogicalOperator(first.subList(0, 2));
                } else {
                    JoinLogicalOperator inter = buildJoinLogicalOperator(first.subList(0, i - 2));
                    cost = costOfTwoPlans(inter, first.get(i - 2)); // cost of this element joined with the previous element
                }
            } else {
                cost = costMap.get(first);
            }
            totalCost += cost;
        }

        buildJoinLogicalOperator(plan);
        return totalCost;
    }

    /**
     * Computes the cost of joining leftChild and rightChild
     *
     * @param leftChild Right LogicalOperator to join
     * @param rightChild Left LogicalOperator to join
     * @return Cost of joining leftChild and rightChild
     */
    private static int costOfTwoPlans(LogicalOperator leftChild, LogicalOperator rightChild) {
        // compute v-value for this relation and the right child
        List<Element> leftList;
        if (leftChild instanceof JoinLogicalOperator) {
            leftList = getChildElementsFromJoin((JoinLogicalOperator) leftChild);
        } else {
            leftList = joinLogicalOperator.getSet().setsWithTable(leftChild.getTableName());
        }
        List<Element> rightList = joinLogicalOperator.getSet().setsWithTable(rightChild.getTableName());
        List<BinaryExpression> exps = new ArrayList<>();
        List<List<Column>> joinAttributes = new ArrayList<>();
        for (Element left : leftList) {
            for (Element right : rightList) {
                if (left.equals(right)) {
                    DisjointSetColumn prev = null;
                    for (Iterator<DisjointSetColumn> iter = left.getIterator(); iter.hasNext(); ) {
                        if (prev == null) {
                            prev = iter.next();

                        } else {
                            DisjointSetColumn curr = iter.next();
                            EqualsTo exp = new EqualsTo();
                            exp.setLeftExpression(prev.getColumn());
                            exp.setRightExpression(curr.getColumn());
                            exps.add(exp);
                            prev = curr;
                        }
                    }
                    joinAttributes.addAll(
                            Collections.singletonList(
                                    right.getColumns()
                                            .stream()
                                            .map(DisjointSetColumn::getColumn)
                                            .collect(Collectors.toList()
                                            )
                            )
                    );
                }
            }
        }
        // build expression from the dj set
        Expression joinCondition = ExpressionUtils.buildExpressionFromList(exps);
        if (joinCondition == null) {
            return Integer.MAX_VALUE;
        }

        // this code is supposed to pick the smaller relation to be the outer relation but it doesn't work :(
//        if (!(leftChild instanceof JoinLogicalOperator)) {
//            RelationStatistic stat = Configuration.getInstance().getStatistics(leftChild.getTableName());
//            int numLeftTuples = stat.getNumTuples();
//            stat = Configuration.getInstance().getStatistics(rightChild.getTableName());
//            int numRightTuples = stat.getNumTuples();
//            if (numLeftTuples < numRightTuples) {
//                joinConditionsMap.put(Arrays.asList(leftChild, rightChild), joinCondition);
//            } else {
//                joinConditionsMap.put(Arrays.asList(rightChild, leftChild), joinCondition);
//            }
//        } else {
            joinConditionsMap.put(Arrays.asList(leftChild, rightChild), joinCondition);
//        }

        // compute the v-values
        Set<String> tables = new HashSet<>();
        int denominator = 1;
        for (List<Column> joins : joinAttributes) {
            Column c1 = joins.get(0);
            tables.add(c1.getTable().getName());
            if (joins.size() < 2) {
                denominator *= distinctValues(leftChild, c1.getTable().getName(), c1.getColumnName());

            } else {
                Column c2 = joins.get(1);
                denominator *= Math.max(
                        distinctValues(leftChild, c1.getTable().getName(), c1.getColumnName()),
                        distinctValues(rightChild, c2.getTable().getName(), c2.getColumnName()));
                tables.add(c2.getTable().getName());
            }
        }

        int numerator = tables.stream()
                .map(t -> Configuration.getInstance().getStatistics(t).getNumTuples())
                .reduce(1, (r, s) -> r * s);

        // store the cost in the hash table
        return (int) ((double) numerator / (double) denominator);

    }

    /**
     * Computes the number of distinct values in table tableName of attribute
     * attribute being held by operator
     *
     * @param operator LogicalOperator which deals with this table
     * @param tableName Name of table to evaluate
     * @param attribute Attribute in table to evaluate
     * @return Number of distinct values in table tableName of attribute
     * attribute being held by operator
     */
    private static int distinctValues(LogicalOperator operator, String tableName, String attribute) {
        if (operator instanceof ScanLogicalOperator) {
            RelationStatistic stat = Configuration.getInstance().getStatistics(tableName);
            AttributeStatistic attributeStatistic = stat.getAttributeStatistic(attribute);
            int cost = Math.min(attributeStatistic.getMaxValue() - attributeStatistic.getMinValue() + 1, stat.getNumTuples());
            distinctValues.put(new DisjointSetColumn(tableName, attribute), cost);
            return cost;

        } else if (operator instanceof SelectLogicalOperator && ((SelectLogicalOperator) operator).getSelectCondition() != null) {
            RelationStatistic stat = Configuration.getInstance().getStatistics(tableName);
            AttributeStatistic attributeStatistic = stat.getAttributeStatistic(attribute);
            int upperBound = attributeStatistic.getMaxValue();
            int lowerBound = attributeStatistic.getMinValue();
            Column c = new Column();
            c.setTable(new Table(tableName));
            c.setColumnName(attribute);
            Element element = joinLogicalOperator.getSet().find(c);
            if (element.hasLowerBound()) {
                lowerBound = element.getLowerBound();
                if (element.lowerBoundExclusive()) {
                    lowerBound += 1;
                }
            }
            if (element.hasUpperBound()) {
                upperBound = element.getUpperBound();
                if (element.upperBoundExclusive()) {
                    upperBound -= 1;
                }
            }
            if (element.hasEqualityConstraint()) {
                upperBound = lowerBound = element.getEqualityConstraint();
            }
            int possibleValues = upperBound - lowerBound + 1;
            int totalPossibleValues = attributeStatistic.getMaxValue() - attributeStatistic.getMinValue() + 1;
            if (totalPossibleValues == possibleValues) {
                distinctValues.put(new DisjointSetColumn(tableName, attribute), possibleValues);
                return possibleValues;
            }
            double reductionFactor = (double) possibleValues / (double) totalPossibleValues;
            int cost = Math.min((int) reductionFactor * stat.getNumTuples(), stat.getNumTuples());
            distinctValues.put(new DisjointSetColumn(tableName, attribute), cost);
            return cost;

        } else if (operator instanceof JoinLogicalOperator) {
            BinaryExpression joinCondition = (BinaryExpression) ((JoinLogicalOperator) operator).getJoinCondition();
            List<DisjointSetColumn> columnsInCondition = ExpressionUtils.getColumnsFromExpression(joinCondition);
            DisjointSetColumn column = new DisjointSetColumn(tableName, attribute);
            if (columnsInCondition.contains(column)) {
                // find all equating attributes and determine v-values as above, return min
                List<DisjointSetColumn> equatingColumns = ExpressionUtils.findEquatingColumns(column, joinCondition);
                int minValue = equatingColumns.stream()
                        .map(c -> distinctValues.get(c))
                        .min(Integer::compare)
                        .orElse(Integer.MAX_VALUE);
                distinctValues.put(column, minValue);
                return minValue;

            } else {
                // it's preserved, so return the v-value of the column in the same table
                for (DisjointSetColumn listColumn : columnsInCondition) {
                    if (listColumn.getTableName().equalsIgnoreCase(tableName)) {
                        int cost = distinctValues.get(listColumn);
                        distinctValues.put(column, cost);
                    }
                }
            }
        }
        return Integer.MAX_VALUE;
    }

    /**
     * Builds a list of disjoint set elements being held by this join
     * logical operator
     *
     * @param operator JoinLogicalOperator to extract disjoint set elements
     *                 from
     * @return A list of disjoint set elements held by this join operator
     */
    private static List<Element> getChildElementsFromJoin(JoinLogicalOperator operator) {
        List<Element> ret = new ArrayList<>();
        for (LogicalOperator child :  operator.getChildren()) {
            List<Element> res;
            if (child instanceof JoinLogicalOperator) {
                 res = getChildElementsFromJoin((JoinLogicalOperator) child);
            } else {
                 res = joinLogicalOperator.getSet().setsWithTable(child.getTableName());
            }
            ret.addAll(res);
        }
        return ret;
    }

    /**
     * Builds a join logical operator from an input list plan
     *
     * @param operators List of operators to build left-deep tree from
     * @return Left-deep join tree representing this plan
     */
    private static JoinLogicalOperator buildJoinLogicalOperator(List<LogicalOperator> operators) {
        JoinLogicalOperator jo = new JoinLogicalOperator();
        if (operators.size() <= 2) {
            operators.forEach(jo::addChild);
            if (joinConditionsMap.get(operators) != null) {
                jo.setJoinCondition(joinConditionsMap.get(operators));
            }
            joinOrderMap.put(operators, jo);
            return jo;
        }
        jo.addChild(buildJoinLogicalOperator(operators.subList(0, operators.size() - 1)));
        jo.addChild(operators.get(operators.size() - 1));
        if (joinConditionsMap.get(operators) != null) {
            jo.setJoinCondition(joinConditionsMap.get(operators));
        }
        joinOrderMap.put(operators, jo);
        return jo;
    }

    /**
     * Computes join conditions that need to be used by the operator
     *
     * @param set DisjointSet to pull conditions from
     * @return A set of binary expressions required by the join operator
     */
    private static Set<BinaryExpression> waitingJoinConditions(DisjointSet set) {
        Set<BinaryExpression> ret = new HashSet<>();
        for (Iterator<Element> iter = set.getIterator(); iter.hasNext();) {
            Element element = iter.next();
            List<BinaryExpression> exps = new ArrayList<>();
            DisjointSetColumn prev = null;
            for (Iterator<DisjointSetColumn> iter1 = element.getIterator(); iter1.hasNext(); ) {
                if (prev == null) {
                    prev = iter1.next();

                } else {
                    DisjointSetColumn curr = iter1.next();
                    EqualsTo exp = new EqualsTo();
                    exp.setLeftExpression(prev.getColumn());
                    exp.setRightExpression(curr.getColumn());
                    exps.add(exp);
                    prev = curr;
                }
            }

            BinaryExpression joinCondition = (BinaryExpression) ExpressionUtils.buildExpressionFromList(exps);
            if (joinCondition != null) {
                ret.add(joinCondition);
            }
        }

        return ret;
    }

    /**
     * A list of all join conditions used in the operator at this point
     *
     * @param operator JoinLogicalOperator to extract conditions from
     * @return A list of binary expressions used by the join operator
     */
    private static List<BinaryExpression> getAllConditions(LogicalOperator operator) {
        List<BinaryExpression> ret = new ArrayList<>();
        if (operator instanceof JoinLogicalOperator) {
            BinaryExpression cond = (BinaryExpression) ((JoinLogicalOperator) operator).getJoinCondition();
            if (cond != null) {
                ret.add((BinaryExpression) ((JoinLogicalOperator) operator).getJoinCondition());
            }
            ret.addAll(getAllConditions(((JoinLogicalOperator) operator).getChildren().get(0)));
            ret.addAll(getAllConditions(((JoinLogicalOperator) operator).getChildren().get(1)));
        }
        return ret;
    }
}
