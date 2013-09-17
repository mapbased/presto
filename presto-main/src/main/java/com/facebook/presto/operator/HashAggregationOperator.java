/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.aggregation.FixedWidthAggregationFunction;
import com.facebook.presto.operator.aggregation.VariableWidthAggregationFunction;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import it.unimi.dsi.fastutil.longs.Long2IntMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;

public class HashAggregationOperator
        implements Operator
{
    public static class HashAggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final List<TupleInfo> groupByTupleInfos;
        private final List<Integer> groupByChannels;
        private final Step step;
        private final List<AggregationFunctionDefinition> functionDefinitions;
        private final int expectedGroups;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public HashAggregationOperatorFactory(
                int operatorId,
                List<TupleInfo> groupByTupleInfos,
                List<Integer> groupByChannels,
                Step step,
                List<AggregationFunctionDefinition> functionDefinitions,
                int expectedGroups)
        {
            this.operatorId = operatorId;
            this.groupByTupleInfos = groupByTupleInfos;
            this.groupByChannels = groupByChannels;
            this.step = step;
            this.functionDefinitions = functionDefinitions;
            this.expectedGroups = expectedGroups;

            this.tupleInfos = toTupleInfos(groupByTupleInfos, step, functionDefinitions);
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, HashAggregationOperator.class.getSimpleName());
            return new HashAggregationOperator(
                    operatorContext,
                    groupByTupleInfos,
                    groupByChannels,
                    step,
                    functionDefinitions,
                    expectedGroups
            );
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final List<TupleInfo> groupByTupleInfos;
    private final List<Integer> groupByChannels;
    private final Step step;
    private final List<AggregationFunctionDefinition> functionDefinitions;
    private final int expectedGroups;

    private final List<TupleInfo> tupleInfos;
    private final HashMemoryManager memoryManager;

    private GroupByHashAggregationBuilder aggregationBuilder;
    private Iterator<Page> outputIterator;
    private boolean finishing;

    public HashAggregationOperator(
            OperatorContext operatorContext,
            List<TupleInfo> groupByTupleInfos,
            List<Integer> groupByChannels,
            Step step,
            List<AggregationFunctionDefinition> functionDefinitions,
            int expectedGroups)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        Preconditions.checkNotNull(step, "step is null");
        Preconditions.checkNotNull(functionDefinitions, "functionDefinitions is null");
        Preconditions.checkNotNull(operatorContext, "operatorContext is null");

        this.groupByTupleInfos = groupByTupleInfos;
        this.groupByChannels = groupByChannels;
        this.functionDefinitions = ImmutableList.copyOf(functionDefinitions);
        this.step = step;
        this.expectedGroups = expectedGroups;
        this.memoryManager = new HashMemoryManager(operatorContext);

        this.tupleInfos = toTupleInfos(groupByTupleInfos, step, functionDefinitions);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && aggregationBuilder == null && (outputIterator == null || !outputIterator.hasNext());
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && outputIterator == null && (aggregationBuilder == null || !aggregationBuilder.isFull());
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        checkNotNull(page, "page is null");
        if (aggregationBuilder == null) {
            aggregationBuilder = new GroupByHashAggregationBuilder(
                    functionDefinitions,
                    step,
                    expectedGroups,
                    groupByTupleInfos,
                    groupByChannels,
                    memoryManager);

            // assume initial aggregationBuilder is not full
        }
        else {
            checkState(!aggregationBuilder.isFull(), "Aggregation buffer is full");
        }
        aggregationBuilder.processPage(page);
    }

    @Override
    public Page getOutput()
    {
        if (outputIterator == null || !outputIterator.hasNext()) {
            // no data
            if (aggregationBuilder == null) {
                return null;
            }

            // only flush if we are finishing or the aggregation builder is full
            if (!finishing && !aggregationBuilder.isFull()) {
                return null;
            }

            // Only partial aggregation can flush early. Also, check that we are not flushing tiny bits at a time
            checkState(finishing || step == Step.PARTIAL, "Task exceeded max memory size of %s", memoryManager.getMaxMemorySize());

            outputIterator = aggregationBuilder.build();
            aggregationBuilder = null;

            if (!outputIterator.hasNext()) {
                return null;
            }
        }

        return outputIterator.next();
    }

    private static List<TupleInfo> toTupleInfos(List<TupleInfo> groupByTupleInfo, Step step, List<AggregationFunctionDefinition> functionDefinitions)
    {
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        tupleInfos.addAll(groupByTupleInfo);
        for (AggregationFunctionDefinition functionDefinition : functionDefinitions) {
            if (step != Step.PARTIAL) {
                tupleInfos.add(functionDefinition.getFunction().getFinalTupleInfo());
            }
            else {
                tupleInfos.add(functionDefinition.getFunction().getIntermediateTupleInfo());
            }
        }
        return tupleInfos.build();
    }

    private static class GroupByHashAggregationBuilder
    {
        private final GroupByHash groupByHash;
        private final List<Aggregator> aggregates;
        private final HashMemoryManager memoryManager;

        private GroupByHashAggregationBuilder(
                List<AggregationFunctionDefinition> functionDefinitions,
                Step step,
                int expectedGroups,
                List<TupleInfo> groupByTupleInfos,
                List<Integer> groupByChannels,
                HashMemoryManager memoryManager)
        {


            ImmutableList<Type> groupByTypes = ImmutableList.copyOf(transform(groupByTupleInfos, new Function<TupleInfo, Type>()
            {
                public Type apply(TupleInfo tupleInfo)
                {
                    checkArgument(tupleInfo.getFieldCount() == 1);
                    return tupleInfo.getTypes().get(0);
                }
            }));

            this.groupByHash = new GroupByHash(groupByTypes, Ints.toArray(groupByChannels), expectedGroups);
            this.memoryManager = memoryManager;

            // wrapper each function with an aggregator
            ImmutableList.Builder<Aggregator> builder = ImmutableList.builder();
            for (AggregationFunctionDefinition functionDefinition : checkNotNull(functionDefinitions, "functionDefinitions is null")) {
                builder.add(createAggregator(functionDefinition, step, expectedGroups));
            }
            aggregates = builder.build();
        }

        private void processPage(Page page)
        {
            Page groupedPage = groupByHash.group(page);

            for (Aggregator aggregate : aggregates) {
                aggregate.processPage(groupedPage);
            }
        }

        public boolean isFull()
        {
            long memorySize = groupByHash.getEstimatedSize();
            for (Aggregator aggregate : aggregates) {
                memorySize += aggregate.getEstimatedSize();
            }
            return memoryManager.canUse(memorySize);
        }

        public Iterator<Page> build()
        {
            List<Type> types = groupByHash.getTypes();
            ImmutableList.Builder<TupleInfo> tupleInfos =  ImmutableList.builder();
            for (Type type : types) {
                tupleInfos.add(new TupleInfo(type));
            }
            for (Aggregator aggregator : aggregates) {
                tupleInfos.add(aggregator.getTupleInfo());
            }

            final PageBuilder pageBuilder = new PageBuilder(tupleInfos.build());
            return new AbstractIterator<Page>()
            {

                private final ObjectIterator<Entry> pagePositionToGroup = groupByHash.getPagePositionToGroupId().long2IntEntrySet().fastIterator();

                @Override
                protected Page computeNext()
                {
                    if (!pagePositionToGroup.hasNext()) {
                        return endOfData();
                    }

                    pageBuilder.reset();

                    List<Type> types = groupByHash.getTypes();
                    BlockBuilder[] groupByBlockBuilders = new BlockBuilder[types.size()];
                    for (int i = 0; i < types.size(); i++) {
                        groupByBlockBuilders[i] = pageBuilder.getBlockBuilder(i);
                    }

                    while (!pageBuilder.isFull() && pagePositionToGroup.hasNext()) {
                        Entry next = pagePositionToGroup.next();
                        long pagePosition = next.getLongKey();
                        int groupId = next.getIntValue();

                        groupByHash.getValues(pagePosition, groupByBlockBuilders);

                        for (int i = 0; i < aggregates.size(); i++) {
                            Aggregator aggregator = aggregates.get(i);
                            aggregator.evaluate(groupId, pageBuilder.getBlockBuilder(types.size() + i));
                        }
                    }

                    Page page = pageBuilder.build();
                    return page;
                }
            };
        }
    }

    public static class HashMemoryManager
    {
        private final OperatorContext operatorContext;
        private long currentMemoryReservation;

        public HashMemoryManager(OperatorContext operatorContext)
        {
            this.operatorContext = operatorContext;
        }

        public boolean canUse(long memorySize)
        {
            // remove the pre-allocated memory from this size
            memorySize -= operatorContext.getOperatorPreAllocatedMemory().toBytes();

            long delta = memorySize - currentMemoryReservation;
            if (delta <= 0) {
                return false;
            }

            if (!operatorContext.reserveMemory(delta)) {
                return true;
            }

            // reservation worked, record the reservation
            currentMemoryReservation = Math.max(currentMemoryReservation, memorySize);
            return false;
        }

        public Object getMaxMemorySize()
        {
            return operatorContext.getMaxMemorySize();
        }
    }

    @SuppressWarnings("rawtypes")
    private static Aggregator createAggregator(AggregationFunctionDefinition functionDefinition, Step step, int expectedGroups)
    {
        AggregationFunction function = functionDefinition.getFunction();
        if (function instanceof VariableWidthAggregationFunction) {
            return new VariableWidthAggregator((VariableWidthAggregationFunction) functionDefinition.getFunction(), functionDefinition.getInputs(), step, expectedGroups);
        }
        else {
            Input input = null;
            if (!functionDefinition.getInputs().isEmpty()) {
                input = Iterables.getOnlyElement(functionDefinition.getInputs());
            }

            return new FixedWidthAggregator((FixedWidthAggregationFunction) functionDefinition.getFunction(), input, step);
        }
    }

    private interface Aggregator
    {
        long getEstimatedSize();

        TupleInfo getTupleInfo();

        void processPage(Page page);

        void evaluate(int groupId, BlockBuilder output);
    }

    private static class FixedWidthAggregator
            implements Aggregator
    {
        private final FixedWidthAggregationFunction function;
        private final Input input;
        private final Step step;
        private final int fixedWidthSize;
        private final int sliceSize;
        private final List<Slice> slices = new ArrayList<>();

        private int largestGroupId;
        private int currentMaxGroupId;

        private FixedWidthAggregator(FixedWidthAggregationFunction function, Input input, Step step)
        {
            this.function = function;
            this.input = input;
            this.step = step;
            this.fixedWidthSize = this.function.getFixedSize();
            this.sliceSize = (int) (BlockBuilder.DEFAULT_MAX_BLOCK_SIZE.toBytes() / fixedWidthSize) * fixedWidthSize;
            Slice slice = Slices.allocate(sliceSize);
            slices.add(slice);
            currentMaxGroupId = sliceSize / fixedWidthSize;
        }

        @Override
        public long getEstimatedSize()
        {
            return slices.size() * sliceSize;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            // if this is a partial, the output is an intermediate value
            if (step == Step.PARTIAL) {
                return function.getIntermediateTupleInfo();
            }
            else {
                return function.getFinalTupleInfo();
            }
        }

        @Override
        public void processPage(Page page)
        {
            BlockCursor groupIdCursor = page.getBlock(page.getChannelCount() - 1).cursor();
            BlockCursor valueCursor = null;
            if (input != null) {
                valueCursor = page.getBlock(input.getChannel()).cursor();
            }

            // process row at a time
            int rows = page.getPositionCount();
            for (int i = 0; i < rows; i++) {
                checkState(groupIdCursor.advanceNextPosition());
                checkState(valueCursor == null || valueCursor.advanceNextPosition());

                int groupId = Ints.checkedCast(groupIdCursor.getLong(0));
                initializeGroup(groupId);

                // process the row
                processRow(groupId, valueCursor, 0);
            }

            // verify cursors are complete
            checkState(!groupIdCursor.advanceNextPosition());
            checkState(valueCursor == null || !valueCursor.advanceNextPosition());
        }

        public void initializeGroup(int groupId)
        {
            // add more slices if necessary
            while (groupId >= currentMaxGroupId) {
                Slice slice = Slices.allocate(sliceSize);
                slices.add(slice);
                currentMaxGroupId += sliceSize / fixedWidthSize;
            }

            while (groupId >= largestGroupId) {
                int globalOffset = largestGroupId * fixedWidthSize;

                int sliceIndex = globalOffset / sliceSize; // todo do this with shifts?
                Slice slice = slices.get(sliceIndex);
                int sliceOffset = globalOffset - (sliceIndex * sliceSize);
                function.initialize(slice, sliceOffset);

                largestGroupId++;
            }
        }

        private void processRow(int groupId, BlockCursor cursor, int field)
        {
            int globalOffset = groupId * fixedWidthSize;

            int sliceIndex = globalOffset / sliceSize; // todo do this with shifts?
            Slice slice = slices.get(sliceIndex);
            int sliceOffset = globalOffset - (sliceIndex * sliceSize);

            // if this is a final aggregation, the input is an intermediate value
            if (step == Step.FINAL) {
                function.addIntermediate(cursor, field, slice, sliceOffset);
            }
            else {
                function.addInput(cursor, field, slice, sliceOffset);
            }
        }

        @Override
        public void evaluate(int groupId, BlockBuilder output)
        {
            int offset = groupId * fixedWidthSize;

            int sliceIndex = offset / sliceSize; // todo do this with shifts
            Slice slice = slices.get(sliceIndex);
            int sliceOffset = offset - (sliceIndex * sliceSize);

            // if this is a partial, the output is an intermediate value
            if (step == Step.PARTIAL) {
                function.evaluateIntermediate(slice, sliceOffset, output);
            }
            else {
                function.evaluateFinal(slice, sliceOffset, output);
            }
        }
    }

    private static class VariableWidthAggregator<T>
            implements Aggregator
    {
        private final VariableWidthAggregationFunction<T> function;
        private final List<Input> inputs;
        private final Step step;
        private final ObjectArrayList<T> intermediateValues;
        private long totalElementSizeInBytes;

        private final int[] fields;

        private VariableWidthAggregator(VariableWidthAggregationFunction<T> function, List<Input> inputs, Step step, int expectedGroups)
        {
            this.function = function;
            this.inputs = inputs;
            this.step = step;
            this.intermediateValues = new ObjectArrayList<>(expectedGroups);

            this.fields = new int[inputs.size()];

            for (int i = 0; i < fields.length; i++) {
                fields[i] = inputs.get(i).getField();
            }
        }

        @Override
        public long getEstimatedSize()
        {
            return SizeOf.sizeOf(intermediateValues.elements()) + totalElementSizeInBytes;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            // if this is a partial, the output is an intermediate value
            if (step == Step.PARTIAL) {
                return function.getIntermediateTupleInfo();
            }
            else {
                return function.getFinalTupleInfo();
            }
        }

        @Override
        public void processPage(Page page)
        {
            BlockCursor[] blockCursors = new BlockCursor[inputs.size()];

            BlockCursor groupIdCursor = page.getBlock(page.getChannelCount() - 1).cursor();
            for (int i = 0; i < blockCursors.length; i++) {
                blockCursors[i] = page.getBlock(inputs.get(i).getChannel()).cursor();
            }

            // process row at a time
            int rows = page.getPositionCount();
            for (int i = 0; i < rows; i++) {
                checkState(groupIdCursor.advanceNextPosition());
                for (BlockCursor blockCursor : blockCursors) {
                    checkState(blockCursor.advanceNextPosition());
                }

                int groupId = Ints.checkedCast(groupIdCursor.getLong(0));
                while (groupId >= intermediateValues.size()) {
                    intermediateValues.add(function.initialize());
                }

                processRow(groupId, blockCursors);
            }

            // verify cursors are complete
            checkState(!groupIdCursor.advanceNextPosition());
            for (BlockCursor blockCursor : blockCursors) {
                checkState(!blockCursor.advanceNextPosition());
            }
        }

        private void processRow(int groupId, BlockCursor[] blockCursors)
        {
            // if this is a final aggregation, the input is an intermediate value
            T oldValue = intermediateValues.get(groupId);
            long oldSize = 0;
            if (oldValue != null) {
                oldSize = function.estimateSizeInBytes(oldValue);
            }

            T newValue;
            if (step == Step.FINAL) {
                newValue = function.addIntermediate(blockCursors, fields, oldValue);
            }
            else {
                newValue = function.addInput(blockCursors, fields, oldValue);
            }
            intermediateValues.set(groupId, newValue);

            long newSize = 0;
            if (newValue != null) {
                newSize = function.estimateSizeInBytes(newValue);
            }
            totalElementSizeInBytes += newSize - oldSize;
        }

        @Override
        public void evaluate(int groupId, BlockBuilder output)
        {
            T value = intermediateValues.get(groupId);
            // if this is a partial, the output is an intermediate value
            if (step == Step.PARTIAL) {
                function.evaluateIntermediate(value, output);
            }
            else {
                function.evaluateFinal(value, output);
            }
        }
    }
}
