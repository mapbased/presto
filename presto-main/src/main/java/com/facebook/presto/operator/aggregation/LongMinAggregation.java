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
import it.unimi.dsi.fastutil.booleans.BooleanBigArrays;
import it.unimi.dsi.fastutil.longs.LongBigArrays;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.google.common.base.Preconditions.checkState;

public class LongMinAggregation
        extends SimpleAggregationFunction
{
    public static final LongMinAggregation LONG_MIN = new LongMinAggregation();


    public LongMinAggregation()
    {
        super(SINGLE_LONG, SINGLE_LONG, FIXED_INT_64);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(long expectedSize, int valueChannel)
    {
        return new LongMinGroupedAccumulator(expectedSize, valueChannel);
    }

    public static class LongMinGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private long maxGroupId = -1;
        private boolean[][] notNull;
        private long[][] minValues;

        public LongMinGroupedAccumulator(long expectedSize, int valueChannel)
        {
            super(valueChannel, SINGLE_LONG, SINGLE_LONG);

            this.notNull = BooleanBigArrays.newBigArray(expectedSize);

            this.minValues = LongBigArrays.newBigArray(expectedSize);
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            long newMinGroupId = groupIdsBlock.getMaxGroupId();
            if (newMinGroupId > maxGroupId) {
                notNull = BooleanBigArrays.grow(notNull, newMinGroupId + 1);
                minValues = LongBigArrays.grow(minValues, newMinGroupId + 1);
                LongBigArrays.fill(minValues, maxGroupId + 1, newMinGroupId + 1, Long.MAX_VALUE);
                maxGroupId = newMinGroupId;
            }

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                long groupId = groupIdsBlock.getLong(position);

                if (!values.isNull()) {
                    BooleanBigArrays.set(notNull, groupId, true);

                    long value = values.getLong();
                    value = Math.min(value, LongBigArrays.get(minValues, groupId));
                    LongBigArrays.set(minValues, groupId, value);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            if (BooleanBigArrays.get(notNull, groupId)) {
                long value = LongBigArrays.get(minValues, groupId);
                output.append(value);
            }
            else {
                output.appendNull();
            }
        }
    }

    @Override
    protected Accumulator createAccumulator(int valueChannel)
    {
        return new LongMinAccumulator(valueChannel);
    }

    public static class LongMinAccumulator
            extends SimpleAccumulator
    {
        private boolean notNull;
        private long min = Long.MAX_VALUE;

        public LongMinAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_LONG, SINGLE_LONG);
        }

        @Override
        protected void processInput(Block block)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                if (!values.isNull()) {
                    notNull = true;
                    min = Math.min(min, values.getLong());
                }
            }
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            if (notNull) {
                out.append(min);
            }
            else {
                out.appendNull();
            }
        }
    }
}
