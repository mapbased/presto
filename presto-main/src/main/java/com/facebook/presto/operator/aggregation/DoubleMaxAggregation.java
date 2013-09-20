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
import it.unimi.dsi.fastutil.doubles.DoubleBigArrays;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.google.common.base.Preconditions.checkState;

public class DoubleMaxAggregation
        extends SimpleAggregationFunction
{
    public static final DoubleMaxAggregation DOUBLE_MAX = new DoubleMaxAggregation();

    public DoubleMaxAggregation()
    {
        super(SINGLE_DOUBLE, SINGLE_DOUBLE, DOUBLE);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(long expectedSize, int valueChannel)
    {
        return new DoubleMaxGroupedAccumulator(expectedSize, valueChannel);
    }

    public static class DoubleMaxGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private long maxGroupId = -1;
        private boolean[][] notNull;
        private double[][] maxValues;

        public DoubleMaxGroupedAccumulator(long expectedSize, int valueChannel)
        {
            super(valueChannel, SINGLE_DOUBLE, SINGLE_DOUBLE);

            this.notNull = BooleanBigArrays.newBigArray(expectedSize);

            this.maxValues = DoubleBigArrays.newBigArray(expectedSize);
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            long newMaxGroupId = groupIdsBlock.getMaxGroupId();
            if (newMaxGroupId > maxGroupId) {
                notNull = BooleanBigArrays.grow(notNull, newMaxGroupId + 1);
                maxValues = DoubleBigArrays.grow(maxValues, newMaxGroupId + 1);
                DoubleBigArrays.fill(maxValues, maxGroupId + 1, newMaxGroupId + 1, Double.NEGATIVE_INFINITY);
                maxGroupId = newMaxGroupId;
            }

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                long groupId = groupIdsBlock.getLong(position);

                if (!values.isNull(0)) {
                    BooleanBigArrays.set(notNull, groupId, true);

                    double value = values.getDouble(0);
                    value = Math.max(value, DoubleBigArrays.get(maxValues, groupId));
                    DoubleBigArrays.set(maxValues, groupId, value);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            if (BooleanBigArrays.get(notNull, groupId)) {
                double value = DoubleBigArrays.get(maxValues, groupId);
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
        return new DoubleMaxAccumulator(valueChannel);
    }

    public static class DoubleMaxAccumulator
            extends SimpleAccumulator
    {
        private boolean notNull;
        private double max = Double.NEGATIVE_INFINITY;

        public DoubleMaxAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_DOUBLE, SINGLE_DOUBLE);
        }

        @Override
        protected void processInput(Block block)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                if (!values.isNull(0)) {
                    notNull = true;
                    max = Math.max(max, values.getDouble(0));
                }
            }
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            if (notNull) {
                out.append(max);
            }
            else {
                out.appendNull();
            }
        }
    }
}
