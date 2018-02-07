package com.prevosql.operator.physical.binary.join;

import com.prevosql.interpreter.query.plan.visitor.PhysicalPlanVisitor;
import com.prevosql.operator.physical.PhysicalOperator;
import com.prevosql.operator.visitor.PhysicalOperatorExpressionVisitor;
import com.prevosql.tuple.Tuple;
import net.sf.jsqlparser.expression.Expression;

/**
 * Handles join operators
 */
public class TupleNestedLoopJoinPhysicalOperator extends JoinPhysicalOperator {
    private Tuple saved = null;

    /**
     * Initializes join operator with left and right children operators
     *
     * @param leftChild Left child operator
     * @param rightChild Right child operator
     * @param joinCondition Condition for join
     */
    public TupleNestedLoopJoinPhysicalOperator(PhysicalOperator leftChild,
                                               PhysicalOperator rightChild,
                                               Expression joinCondition) {
        super(leftChild, rightChild, joinCondition);
    }


    /**
     * Gets next valid tuple from joined left and right operators, uses
     * naive tuple nested join algorithm
     *
     * @return Next valid tuple from joined left and right operators
     */
    @Override
    public Tuple getNextTuple() {
        if (!joinedTableCreated) {
            createJoinedTable();
        }
        Tuple l;
        if (isFinished) {
            l = leftChild.getNextTuple();
        } else {
            l = saved;
        }
        while (l != null) {
            Tuple r;
            while ((r = rightChild.getNextTuple()) != null) {
                Tuple newTuple = new Tuple(l, r);
                if (joinCondition == null) {
                    saved = l;
                    isFinished = false;
                    return newTuple;
                }
                PhysicalOperatorExpressionVisitor ov = new PhysicalOperatorExpressionVisitor(newTuple, catalog);
                joinCondition.accept(ov);
                if (ov.getResult()) {
                    saved = l;
                    isFinished = false;
                    return newTuple;
                }
            }
            rightChild.reset();
            l = leftChild.getNextTuple();
            isFinished = true;
        }
        leftChild.reset();
        return null;
    }

    @Override
    public void accept(PhysicalPlanVisitor visitor) {
        visitor.visit(this);
    }
}
