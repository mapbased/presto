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

public class DoubleMinAggregation
        extends SimpleAggregationFunction
{
    public static final DoubleMinAggregation DOUBLE_MIN = new DoubleMinAggregation();

    public DoubleMinAggregation()
    {
        super(SINGLE_DOUBLE, SINGLE_DOUBLE, DOUBLE);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(long expectedSize, int valueChannel)
    {
        return new DoubleMinGroupedAccumulator(expectedSize, valueChannel);
    }

    public static class DoubleMinGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private long maxGroupId = -1;
        private boolean[][] notNull;
        private double[][] minValues;

        public DoubleMinGroupedAccumulator(long expectedSize, int valueChannel)
        {
            super(valueChannel, SINGLE_DOUBLE, SINGLE_DOUBLE);

            this.notNull = BooleanBigArrays.newBigArray(expectedSize);

            this.minValues = DoubleBigArrays.newBigArray(expectedSize);
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            long newMinGroupId = groupIdsBlock.getMaxGroupId();
            if (newMinGroupId > maxGroupId) {
                notNull = BooleanBigArrays.grow(notNull, newMinGroupId + 1);
                minValues = DoubleBigArrays.grow(minValues, newMinGroupId + 1);
                DoubleBigArrays.fill(minValues, maxGroupId + 1, newMinGroupId + 1, Double.POSITIVE_INFINITY);
                maxGroupId = newMinGroupId;
            }

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                long groupId = groupIdsBlock.getLong(position);

                if (!values.isNull()) {
                    BooleanBigArrays.set(notNull, groupId, true);

                    double value = values.getDouble();
                    value = Math.min(value, DoubleBigArrays.get(minValues, groupId));
                    DoubleBigArrays.set(minValues, groupId, value);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            if (BooleanBigArrays.get(notNull, groupId)) {
                double value = DoubleBigArrays.get(minValues, groupId);
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
        return new DoubleMinAccumulator(valueChannel);
    }

    public static class DoubleMinAccumulator
            extends SimpleAccumulator
    {
        private boolean notNull;
        private double min = Double.POSITIVE_INFINITY;

        public DoubleMinAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_DOUBLE, SINGLE_DOUBLE);
        }

        @Override
        protected void processInput(Block block)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                if (!values.isNull()) {
                    notNull = true;
                    min = Math.min(min, values.getDouble());
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
