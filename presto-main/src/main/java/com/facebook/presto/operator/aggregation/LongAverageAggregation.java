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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.GroupByIdBlock;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrays;
import it.unimi.dsi.fastutil.longs.LongBigArrays;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class LongAverageAggregation
        extends SimpleAggregationFunction
{
    public static final LongAverageAggregation LONG_AVERAGE = new LongAverageAggregation();

    public LongAverageAggregation()
    {
        super(SINGLE_DOUBLE, SINGLE_VARBINARY, FIXED_INT_64);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(long expectedSize, int valueChannel)
    {
        return new LongSumGroupedAccumulator(expectedSize, valueChannel);
    }

    public static class LongSumGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private long[][] counts;
        private double[][] sums;

        public LongSumGroupedAccumulator(long expectedSize, int valueChannel)
        {
            super(valueChannel, SINGLE_DOUBLE, SINGLE_VARBINARY);
            this.counts = LongBigArrays.newBigArray(expectedSize);
            this.sums = DoubleBigArrays.newBigArray(expectedSize);
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            counts = LongBigArrays.grow(counts, groupIdsBlock.getMaxGroupId() + 1);
            sums = DoubleBigArrays.grow(sums, groupIdsBlock.getMaxGroupId() + 1);

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                long groupId = groupIdsBlock.getLong(position);

                if (!values.isNull(0)) {
                    LongBigArrays.incr(counts, groupId);

                    long value = values.getLong(0);
                    DoubleBigArrays.add(sums, groupId, value);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        protected void processIntermediate(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            counts = LongBigArrays.grow(counts, groupIdsBlock.getMaxGroupId() + 1);
            sums = DoubleBigArrays.grow(sums, groupIdsBlock.getMaxGroupId() + 1);

            BlockCursor intermediateValues = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(intermediateValues.advanceNextPosition());

                long groupId = groupIdsBlock.getLong(position);

                Slice value = intermediateValues.getSlice(0);
                long count = value.getLong(0);
                LongBigArrays.add(counts, groupId, count);

                double sum = value.getDouble(SIZE_OF_LONG);
                DoubleBigArrays.add(sums, groupId, sum);
            }
            checkState(!intermediateValues.advanceNextPosition());
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            long count = LongBigArrays.get(counts, groupId);
            double sum = DoubleBigArrays.get(sums, groupId);

            // todo replace this when general fixed with values are supported
            Slice value = Slices.allocate(SIZE_OF_LONG + SIZE_OF_DOUBLE);
            value.setLong(0, count);
            value.setDouble(SIZE_OF_LONG, sum);
            output.append(value);
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            long count = LongBigArrays.get(counts, groupId);
            if (count != 0) {
                double value = DoubleBigArrays.get(sums, groupId);
                output.append(value / count);
            }
            else {
                output.appendNull();
            }
        }
    }

    @Override
    protected Accumulator createAccumulator(int valueChannel)
    {
        return new LongAverageAccumulator(valueChannel);
    }

    public static class LongAverageAccumulator
            extends SimpleAccumulator
    {
        private long count;
        private double sum;

        public LongAverageAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_DOUBLE, SINGLE_VARBINARY);
        }

        @Override
        protected void processInput(Block block)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                if (!values.isNull(0)) {
                    count++;
                    sum += values.getLong(0);
                }
            }
        }

        @Override
        protected void processIntermediate(Block block)
        {
            BlockCursor intermediates = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition());
                Slice value = intermediates.getSlice(0);
                count += value.getLong(0);
                sum += value.getDouble(SIZE_OF_LONG);
            }
        }

        @Override
        public void evaluateIntermediate(BlockBuilder out)
        {
            // todo replace this when general fixed with values are supported
            Slice value = Slices.allocate(SIZE_OF_LONG + SIZE_OF_DOUBLE);
            value.setLong(0, count);
            value.setDouble(SIZE_OF_LONG, sum);
            out.append(value);
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            if (count != 0) {
                out.append(sum / count);
            }
            else {
                out.appendNull();
            }
        }
    }
}
