package com.prevosql.operator.physical.binary.join;

import com.prevosql.config.operator.JoinOperator;
import com.prevosql.interpreter.query.plan.visitor.PhysicalPlanVisitor;
import com.prevosql.operator.physical.PhysicalOperator;
import com.prevosql.operator.visitor.PhysicalOperatorExpressionVisitor;
import com.prevosql.tuple.Tuple;
import net.sf.jsqlparser.expression.Expression;

/**
 * Implements the block-nested loop join algorithm
 */
public class BlockNestedLoopJoinPhysicalOperator extends JoinPhysicalOperator {
    private final int numTuples;
    private final Tuple[] buffer;
    private static final int PAGE_SIZE = 4096;
    private int numOuterBlocks = 0;
    private int index = 0;
    private Tuple saved;

    /**
     * Constructs a block-nested loop join operator with condition
     * joinCondition, using numPages pages as internal buffer pages
     *
     * @param leftChild Left operator to join
     * @param rightChild Right operator to join
     * @param joinCondition Condition to join on
     * @param numPages Number of buffer pages to use
     */
    public BlockNestedLoopJoinPhysicalOperator(PhysicalOperator leftChild,
                                               PhysicalOperator rightChild,
                                               Expression joinCondition,
                                               int numPages) {
        super(leftChild, rightChild, joinCondition);
        numTuples = (numPages * PAGE_SIZE) / (4 * leftChild.getNumAttributes());
        buffer = new Tuple[numTuples];
        setJoinType(JoinOperator.BLOCK_NESTED_LOOP);
    }

    @Override
    public Tuple getNextTuple() {
        if (!joinedTableCreated) {
            createJoinedTable();
        }

        int read;
        if (index == numTuples) {
            index = 0;
            if ((read = readOuterBlock()) < 0) {
                return null;
            }
            isFinished = false;

        } else if (isFinished) {
            read = readOuterBlock();

        } else {
            read = 1;
        }

        Tuple l;
        Tuple r;
        if (!isFinished) {
            r = saved;
            if (index == numTuples - 1) {
                index = 0;
                l = buffer[index];
                r = rightChild.getNextTuple();
            } else {
                l = buffer[++index];
            }
        } else {
            r = rightChild.getNextTuple();
            l = buffer[index++];
        }

        while (read > 0) {
            while (r != null) {
                while (l != null && index < numTuples) {
                    Tuple newTuple = new Tuple(l, r);
                    if (joinCondition == null) {
                        isFinished = false;
                        saved = r;
                        return newTuple;
                    }
                    PhysicalOperatorExpressionVisitor ov = new PhysicalOperatorExpressionVisitor(newTuple, catalog);
                    joinCondition.accept(ov);
                    if (ov.getResult()) {
                        isFinished = false;
                        saved = r;
                        return newTuple;
                    }
                    index++;
                    if (index == numTuples) {
                        break;
                    }
                    l = buffer[index];
                }
                r = rightChild.getNextTuple();
                index = 0;
                l = buffer[index];
            }
            index = 0;
            read = readOuterBlock();
            rightChild.reset();
            r = rightChild.getNextTuple();
            l = buffer[index];
        }

        isFinished = true;
        return null;
    }

    @Override
    public void accept(PhysicalPlanVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Reads a block of numBufferPages size into memory of
     * the outer relation
     *
     * @return 1 if we read a whole block, -1 if there are no
     * more blocks left to read
     */
    private int readOuterBlock() {
        for (int i = 0; i < numTuples; i++) {
            Tuple t = leftChild.getNextTuple();
            if (t == null && i == 0) {
                return -1;
            } else if (t == null) {
                for (int j = i; j < numTuples; j++) {
                    buffer[j] = null;
                }
               break;
            }
            buffer[i] = t;
        }
        LOG.info("Block " + numOuterBlocks + " of outer relation " + leftTableName + " read into buffer");
        numOuterBlocks++;
        return 1;
    }
}
