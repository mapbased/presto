package com.facebook.presto.sql.truffle;

import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DriverNode
        extends RootNode
{
    private static final Logger log = Logger.get(DriverNode.class);

    private final DriverContext driverContext;
    private final List<Operator> operators;
    private final Map<PlanNodeId, SourceOperator> sourceOperators;

    public DriverNode(DriverContext driverContext, Operator firstOperator, Operator... otherOperators)
    {
        this(checkNotNull(driverContext, "driverContext is null"),
                ImmutableList.<Operator>builder()
                        .add(checkNotNull(firstOperator, "firstOperator is null"))
                        .add(checkNotNull(otherOperators, "otherOperators is null"))
                        .build());
    }

    public DriverNode(DriverContext driverContext, List<Operator> operators)
    {
        this.driverContext = checkNotNull(driverContext, "driverContext is null");
        this.operators = ImmutableList.copyOf(checkNotNull(operators, "operators is null"));
        checkArgument(!operators.isEmpty(), "There must be at least one operator");

        ImmutableMap.Builder<PlanNodeId, SourceOperator> sourceOperators = ImmutableMap.builder();
        for (Operator operator : operators) {
            if (operator instanceof SourceOperator) {
                SourceOperator sourceOperator = (SourceOperator) operator;
                sourceOperators.put(sourceOperator.getSourceId(), sourceOperator);
            }
        }
        this.sourceOperators = sourceOperators.build();
    }

    @Override
    public synchronized Object execute(VirtualFrame virtualFrame)
    {
        // todo move this loop to the benchmark
        while (!isFinished()) {
            process();
        }
        return null;
    }

    private boolean isFinished()
    {
        boolean finished = driverContext.isDone() || operators.get(operators.size() - 1).isFinished();
        if (finished) {
            close();
        }
        return finished;
    }

    private ListenableFuture<?> process()
    {
        driverContext.start();

        try {
            for (int i = 0; i < operators.size() - 1 && !driverContext.isDone(); i++) {
                // check if current operator is blocked
                Operator current = operators.get(i);
                ListenableFuture<?> blocked = current.isBlocked();
                if (!blocked.isDone()) {
                    current.getOperatorContext().recordBlocked(blocked);
                    return blocked;
                }

                // check if next operator is blocked
                Operator next = operators.get(i + 1);
                blocked = next.isBlocked();
                if (!blocked.isDone()) {
                    next.getOperatorContext().recordBlocked(blocked);
                    return blocked;
                }

                // if current operator is finished...
                if (current.isFinished()) {
                    // let next operator know there will be no more data
                    next.getOperatorContext().startIntervalTimer();
                    next.finish();
                    next.getOperatorContext().recordFinish();
                }
                else {
                    // if next operator needs input...
                    if (next.needsInput()) {
                        // get an output page from current operator
                        current.getOperatorContext().startIntervalTimer();
                        Page page = current.getOutput();
                        current.getOperatorContext().recordGetOutput(page);

                        // if we got an output page, add it to the next operator
                        if (page != null) {
                            next.getOperatorContext().startIntervalTimer();
                            next.addInput(page);
                            next.getOperatorContext().recordAddInput(page);
                        }
                    }
                }
            }
            return NOT_BLOCKED;
        }
        catch (Throwable t) {
            driverContext.failed(t);
            throw t;
        }
    }

    private void close()
    {
        try {
            for (Operator operator : operators) {
                operator.finish();
            }
        }
        finally {
            for (Operator operator : operators) {
                if (operator instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) operator).close();
                    }
                    catch (Exception e) {
                        if (e instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        log.error(e, "Error closing operator %s for task %s", operator.getOperatorContext().getOperatorId(), driverContext.getTaskId());
                    }
                }
            }
            driverContext.finished();
        }
    }
}
