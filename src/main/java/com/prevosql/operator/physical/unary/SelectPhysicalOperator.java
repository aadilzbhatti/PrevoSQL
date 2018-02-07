package com.prevosql.operator.physical.unary;

import com.prevosql.interpreter.query.plan.visitor.PhysicalPlanVisitor;
import com.prevosql.operator.physical.PhysicalOperator;
import com.prevosql.operator.visitor.PhysicalOperatorExpressionVisitor;
import com.prevosql.tuple.Tuple;
import net.sf.jsqlparser.expression.Expression;

/**
 * Handles select operators
 */
public class SelectPhysicalOperator extends UnaryPhysicalOperator {
    private final PhysicalOperator child;
    private final Expression expression;

    /**
     * Initializes a SelectOperator
     *
     * @param child ScanOperator to pick valid tuples from
     * @param e Expression to judge whether a tuple is valid
     */
    public SelectPhysicalOperator(PhysicalOperator child, Expression e) {
        super(child);
        this.expression = e;
        this.child = child;
    }

    /**
     * Gets next valid tuple from child
     *
     * @return Next valid tuple from child
     */
    @Override
    public Tuple getNextTuple() {
        while (true) {
            Tuple t = child.getNextTuple();
            if (t == null) {
                return null;
            }
            if (expression != null) {
                PhysicalOperatorExpressionVisitor ov = new PhysicalOperatorExpressionVisitor(t);
                expression.accept(ov);
                if (ov.getResult()) {
                    return t;
                }
            }
        }
    }

    /**
     * Resets child operator
     */
    @Override
    public void reset() {
        child.reset();
    }

    @Override
    public void accept(PhysicalPlanVisitor visitor) {
        visitor.visit(this);
    }

    public Expression getSelectCondition() {
        return expression;
    }
}
