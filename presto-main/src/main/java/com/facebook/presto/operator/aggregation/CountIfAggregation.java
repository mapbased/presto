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
import it.unimi.dsi.fastutil.longs.LongBigArrays;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.Type.BOOLEAN;
import static com.google.common.base.Preconditions.checkState;

public class CountIfAggregation
        extends SimpleAggregationFunction
{
    public static final CountIfAggregation COUNT_IF = new CountIfAggregation();

    public CountIfAggregation()
    {
        super(SINGLE_LONG, SINGLE_LONG, BOOLEAN);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(long expectedSize, int valueChannel)
    {
        return new CountIfGroupedAccumulator(expectedSize, valueChannel);
    }

    public static class CountIfGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private long[][] counts;

        public CountIfGroupedAccumulator(long expectedSize, int valueChannel)
        {
            super(valueChannel, SINGLE_LONG, SINGLE_LONG);
            this.counts = LongBigArrays.newBigArray(expectedSize);
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            counts = LongBigArrays.grow(counts, groupIdsBlock.getMaxGroupId() + 1);

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                if (!values.isNull() && values.getBoolean()) {
                    long groupId = groupIdsBlock.getLong(position);
                    LongBigArrays.incr(counts, groupId);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        protected void processIntermediate(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            counts = LongBigArrays.grow(counts, groupIdsBlock.getMaxGroupId() + 1);

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                if (!values.isNull()) {
                    long groupId = groupIdsBlock.getLong(position);
                    LongBigArrays.add(counts, groupId, values.getLong());
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            long value = LongBigArrays.get(counts, groupId);
            output.append(value);
        }
    }

    @Override
    protected Accumulator createAccumulator(int valueChannel)
    {
        return new CountIfAccumulator(valueChannel);
    }

    public static class CountIfAccumulator
            extends SimpleAccumulator
    {
        private long count;

        public CountIfAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_LONG, SINGLE_LONG);
        }

        @Override
        protected void processInput(Block block)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                if (!values.isNull() && values.getBoolean()) {
                    count++;
                }
            }
        }

        @Override
        protected void processIntermediate(Block block)
        {
            BlockCursor intermediates = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition());
                count += intermediates.getLong();
            }
        }

        @Override
        public void evaluateIntermediate(BlockBuilder out)
        {
            evaluateFinal(out);
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            out.append(count);
        }
    }
}
