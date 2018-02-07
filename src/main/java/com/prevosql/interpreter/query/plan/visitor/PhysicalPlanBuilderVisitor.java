package com.prevosql.interpreter.query.plan.visitor;

import com.prevosql.config.Configuration;
import com.prevosql.config.catalog.DBCatalog;
import com.prevosql.config.index.IndexInfo;
import com.prevosql.config.operator.PhysicalConfigParser;
import com.prevosql.config.statistics.AttributeStatistic;
import com.prevosql.config.statistics.RelationStatistic;
import com.prevosql.config.index.IndexResult;
import com.prevosql.interpreter.query.optimize.JoinOrderBuilder;
import com.prevosql.interpreter.query.visitor.SortExpressionVisitor;
import com.prevosql.operator.logical.LogicalOperator;
import com.prevosql.operator.logical.nary.JoinLogicalOperator;
import com.prevosql.operator.logical.leaf.ScanLogicalOperator;
import com.prevosql.operator.logical.unary.DuplicateEliminationLogicalOperator;
import com.prevosql.operator.logical.unary.ProjectLogicalOperator;
import com.prevosql.operator.logical.unary.SelectLogicalOperator;
import com.prevosql.operator.logical.unary.SortLogicalOperator;
import com.prevosql.operator.physical.binary.join.BlockNestedLoopJoinPhysicalOperator;
import com.prevosql.operator.physical.binary.join.SortMergeJoinPhysicalOperator;
import com.prevosql.operator.physical.leaf.IndexScanPhysicalOperator;
import com.prevosql.operator.physical.leaf.ScanPhysicalOperator;
import com.prevosql.operator.physical.unary.SelectPhysicalOperator;
import com.prevosql.operator.physical.unary.sort.ExternalSortPhysicalOperator;
import com.prevosql.operator.physical.unary.sort.SortPhysicalOperator;
import com.prevosql.operator.physical.unary.DuplicateEliminationPhysicalOperator;
import com.prevosql.operator.physical.unary.ProjectPhysicalOperator;
import com.prevosql.util.disjointset.DisjointSet;
import com.prevosql.util.disjointset.DisjointSetColumn;
import com.prevosql.util.disjointset.Element;
import com.prevosql.util.disjointset.DisjointSetExpressionVisitor;
import com.prevosql.util.expression.ExpressionUtils;
import com.prevosql.operator.physical.PhysicalOperator;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.*;

/**
 * Visitor to build physical query plan from logical query plan and configuration file
 */
public class PhysicalPlanBuilderVisitor implements LogicalPlanVisitor {
    private PhysicalOperator root;
    private boolean child = false;

    private static final int PAGE_SIZE = 4096;
    private static final int ROOT_TO_LEAF_COST = 3;

    private PhysicalPlanBuilderVisitor(boolean child) {
        this.child = child;
    }

    public PhysicalPlanBuilderVisitor() {
        this(false);
    }

    /**
     * @return Root of physical query plan
     */
    public PhysicalOperator getResult() {
        return root;
    }

    /**
     * Creates a ScanPhysicalOperator
     *
     * @param scanOperator Input ScanLogicalOperator
     */
    @Override
    public void visit(ScanLogicalOperator scanOperator) {
        String fullTableName = DBCatalog.getInstance().getTableName(scanOperator.getTableName());
        if (fullTableName == null) {
            fullTableName = scanOperator.getTableName();
        }
        root = new ScanPhysicalOperator(fullTableName);
    }

    @Override
    public void visit(SelectLogicalOperator selectLogicalOperator) {
        // need to separate the useful index stuff from the not-useful stuff. maybe with a DJS
        LogicalOperator child = selectLogicalOperator.getChild();

        if (child instanceof ScanLogicalOperator) {
            DisjointSetExpressionVisitor visitor = new DisjointSetExpressionVisitor();
            selectLogicalOperator.getSelectCondition().accept(visitor);
            DisjointSet set = visitor.getDisjointSet();
            List<BinaryExpression> unusableExpressions = visitor.getUnusableExpressions();

            // compute the full scan cost
            RelationStatistic stat = Configuration.getInstance().getStatistics(child.getTableName());
            int numTuples = stat.getNumTuples();
            int numAttributes = stat.getNumAttributes();
            int fullScanCost = (int) Math.ceil((numAttributes * 4.0 * numTuples) / PAGE_SIZE);

            double minimumCost = (double) Integer.MAX_VALUE;
            IndexResult minimumResult = null;
            BinaryExpression minimumSelectCondition = null;

            // compute cost of using each index
            for (Iterator<Element> iter = set.getIterator(); iter.hasNext();) {
                Element element = iter.next();

                // by def there's only 1 element per DJS in a vanilla select (no join), but this works too
                for (Iterator<DisjointSetColumn> iter1 = element.getIterator(); iter1.hasNext();) {
                    DisjointSetColumn attribute = iter1.next();
                    Column column = set.getColumn(attribute.getFullName());
                    String tableName = column.getTable().getName();
                    String attributeName = column.getColumnName();

                    BinaryExpression selectCondition;
                    int upperBound, lowerBound;
                    AttributeStatistic attributeStatistic = stat.getAttributeStatistic(attributeName);

                    if (element.hasUpperBound() && element.hasLowerBound()) {
                        BinaryExpression leftExpression, rightExpression;
                        upperBound = element.getUpperBound();
                        lowerBound = element.getLowerBound();
                        if (element.upperBoundExclusive()) {
                            leftExpression = new MinorThan();
                            upperBound = upperBound - 1;
                        } else {
                            leftExpression = new MinorThanEquals();
                        }
                        leftExpression.setLeftExpression(column);
                        leftExpression.setRightExpression(new LongValue(element.getUpperBound()));
                        if (element.lowerBoundExclusive()) {
                            rightExpression = new GreaterThan();
                            lowerBound = lowerBound + 1;
                        } else {
                            rightExpression = new GreaterThanEquals();
                        }
                        rightExpression.setLeftExpression(column);
                        rightExpression.setRightExpression(new LongValue(element.getLowerBound()));
                        selectCondition = new AndExpression(leftExpression, rightExpression);

                    } else if (element.hasLowerBound()) {
                        lowerBound = element.getLowerBound();
                        if (element.lowerBoundExclusive()) {
                            selectCondition = new GreaterThan();
                            lowerBound = lowerBound + 1;
                        } else {
                            selectCondition = new GreaterThanEquals();
                        }
                        upperBound = attributeStatistic.getMaxValue();
                        selectCondition.setLeftExpression(column);
                        selectCondition.setRightExpression(new LongValue(element.getLowerBound()));

                    } else if (element.hasUpperBound()) {
                        upperBound = element.getUpperBound();
                        if (element.upperBoundExclusive()) {
                            selectCondition = new MinorThan();
                            upperBound = upperBound - 1;
                        } else {
                            selectCondition = new MinorThanEquals();
                        }
                        lowerBound = attributeStatistic.getMinValue();
                        selectCondition.setLeftExpression(column);
                        selectCondition.setRightExpression(new LongValue(element.getUpperBound()));

                    } else if (element.hasEqualityConstraint()) {
                        selectCondition = new EqualsTo();
                        selectCondition.setLeftExpression(column);
                        selectCondition.setRightExpression(new LongValue(element.getEqualityConstraint()));
                        upperBound = lowerBound = element.getEqualityConstraint();

                    } else {
                        continue; // this should never happen
                    }

                    IndexInfo info;
                    if ((info = Configuration.getIndexConfig().getIndex(tableName, attributeName)) != null) {
                        int numPossibleValues = upperBound - lowerBound + 1;
                        int totalPossibleValues = attributeStatistic.getMaxValue() - attributeStatistic.getMinValue() + 1;
                        double reductionFactor = (double) numPossibleValues / (double) totalPossibleValues;

                        IndexResult result = new IndexResult(
                                lowerBound, upperBound, attributeName, reductionFactor, info.isClustered());

                        double cost;
                        cost = ROOT_TO_LEAF_COST + reductionFactor * info.getNumLeafPages();

                        if (!info.isClustered()) {
                            cost += reductionFactor * numTuples;
                        }

                        if (cost < minimumCost) {
                            if (minimumSelectCondition != null) {
                                unusableExpressions.add(minimumSelectCondition);
                            }
                            minimumCost = cost;
                            minimumResult = result;
                            minimumSelectCondition = selectCondition;
                            continue;
                        }
                    }
                    unusableExpressions.add(selectCondition);
                }
            }

            if (minimumCost < fullScanCost && minimumResult != null) {
                IndexScanPhysicalOperator ispo = new IndexScanPhysicalOperator(
                        selectLogicalOperator.getTableName(), minimumResult.getSearchKey(),
                        minimumResult.getLowkey(), minimumResult.getHighkey(), minimumResult.isClustered()
                );
                if (unusableExpressions.size() > 0) {
                    Expression finalSelectCond = ExpressionUtils.buildExpressionFromList(unusableExpressions);
                    root = new SelectPhysicalOperator(ispo, finalSelectCond);

                } else {
                    root = ispo;
                }
                return;
            }
        }

        // either we couldn't use the index or it cost more than a full scan
        PhysicalPlanBuilderVisitor v1 = new PhysicalPlanBuilderVisitor();
        child.accept(v1);
        PhysicalOperator scanPhysicalOperator = v1.getResult();
        root = new SelectPhysicalOperator(scanPhysicalOperator, selectLogicalOperator.getSelectCondition());
    }

    /**
     * Creates a ProjectPhysicalOperator
     *
     * @param projectOperator Input ProjectPhysicalOperator
     */
    @Override
    public void visit(ProjectLogicalOperator projectOperator) {
        PhysicalPlanBuilderVisitor v1 = new PhysicalPlanBuilderVisitor();
        projectOperator.getChild().accept(v1);
        root = new ProjectPhysicalOperator(v1.getResult(), projectOperator.getSelectItems());
    }

    /**
     * Creates a SortPhysicalOperator. If the config file specifies
     * that external sorting should be used, creates an ExternalSortPhysicalOperator.
     * If the file specifies that in-memory sorting should be used, creates
     * an InMemorySortPhysicalOperator
     *
     * @param sortOperator Input SortLogicalOperator
     */
    @Override
    public void visit(SortLogicalOperator sortOperator) {
        PhysicalPlanBuilderVisitor v1 = new PhysicalPlanBuilderVisitor();
        sortOperator.getChild().accept(v1);
        root = new ExternalSortPhysicalOperator(v1.getResult(), sortOperator.getOrderByElementList(), 100, false);
    }

    /**
     * Builds a JoinPhysicalOperator. Computes the cost of all possible
     * query plans and picks minimum-cost plan, and then builds a left-deep
     * tree for that plan. Then, it decides whether or not to use sort-merge join
     * or block-nested loop join depending on whether or not the join condition
     * uses equijoins or something else, respectively.
     *
     * @param joinLogicalOperator Input JoinLogicalOperator
     */
    @Override
    public void visit(JoinLogicalOperator joinLogicalOperator) {
        JoinLogicalOperator operator;
        if (!child) {
            JoinOrderBuilder.buildJoinOrder(joinLogicalOperator);
            operator = JoinOrderBuilder.getLowestCostJoinOrder();
        } else {
            operator = joinLogicalOperator;
        }

        LogicalOperator leftChild = operator.getChildren().get(0);
        LogicalOperator rightChild = operator.getChildren().get(1);
        PhysicalPlanBuilderVisitor v1 = new PhysicalPlanBuilderVisitor(true);
        PhysicalPlanBuilderVisitor v2 = new PhysicalPlanBuilderVisitor(true);
        leftChild.accept(v1);
        rightChild.accept(v2);
        PhysicalOperator leftPhysicalOperator = v1.getResult();
        PhysicalOperator rightPhysicalOperator = v2.getResult();
        setJoinRoot(operator, leftPhysicalOperator, rightPhysicalOperator);
    }

    /**
     * Creates a DuplicateEliminationPhysicalOperator
     *
     * @param duplicateEliminationOperator Input DuplicateEliminationLogicalOperator
     */
    @Override
    public void visit(DuplicateEliminationLogicalOperator duplicateEliminationOperator) {
        PhysicalPlanBuilderVisitor v1 = new PhysicalPlanBuilderVisitor();
        duplicateEliminationOperator.getChild().accept(v1);
        root = new DuplicateEliminationPhysicalOperator(v1.getResult());
    }

    /**
     * Picks SMJ or BNLJ depending on whether the join condition uses equijoins
     * or other expressions, respectively.
     *
     * @param joinLogicalOperator Input JoinLogicalOperator
     * @param leftPhysicalOperator PhysicalOperator which will be left child
     * @param rightPhysicalOperator PhysicalOperator which will be right child
     */
    private void setJoinRoot(JoinLogicalOperator joinLogicalOperator,
                             PhysicalOperator leftPhysicalOperator, PhysicalOperator rightPhysicalOperator) {

        BinaryExpression joinCondition = (BinaryExpression) joinLogicalOperator.getJoinCondition();
        List<BinaryExpression> allExpressions = ExpressionUtils.getAllExpressions(joinCondition);
        boolean allEquals = allExpressions.stream().allMatch(e -> e instanceof EqualsTo);
        PhysicalConfigParser config = Configuration.getPhysicalConfig();

        if (allEquals) {
            // use SMJ
            int numBufferPages = config.getNumSortBufferPages();
            SortPhysicalOperator leftSort, rightSort;

            SortExpressionVisitor s1 = new SortExpressionVisitor(leftPhysicalOperator, rightPhysicalOperator);
            joinCondition.accept(s1);
            List<OrderByElement> leftElements = s1.getLeftElements();
            List<OrderByElement> rightElements = s1.getRightElements();

            leftSort = new ExternalSortPhysicalOperator(leftPhysicalOperator, leftElements, numBufferPages, false);
            rightSort = new ExternalSortPhysicalOperator(rightPhysicalOperator, rightElements, numBufferPages, false);

            root = new SortMergeJoinPhysicalOperator(leftSort, rightSort, joinCondition);

        } else {
            // use BNLJ
            root = new BlockNestedLoopJoinPhysicalOperator(leftPhysicalOperator, rightPhysicalOperator,
                    joinCondition, config.getNumOuterRelationBufferPages());
        }
    }
}
