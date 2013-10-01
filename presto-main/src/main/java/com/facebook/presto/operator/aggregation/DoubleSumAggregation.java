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

public class DoubleSumAggregation
        extends SimpleAggregationFunction
{
    public static final DoubleSumAggregation DOUBLE_SUM = new DoubleSumAggregation();

    public DoubleSumAggregation()
    {
        super(SINGLE_DOUBLE, SINGLE_DOUBLE, DOUBLE);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(long expectedSize, int valueChannel)
    {
        return new DoubleSumGroupedAccumulator(expectedSize, valueChannel);
    }

    public static class DoubleSumGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private boolean[][] notNull;
        private double[][] sums;

        public DoubleSumGroupedAccumulator(long expectedSize, int valueChannel)
        {
            super(valueChannel, SINGLE_DOUBLE, SINGLE_DOUBLE);
            this.notNull = BooleanBigArrays.newBigArray(expectedSize);
            this.sums = DoubleBigArrays.newBigArray(expectedSize);
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            notNull = BooleanBigArrays.grow(notNull, groupIdsBlock.getMaxGroupId() + 1);
            sums = DoubleBigArrays.grow(sums, groupIdsBlock.getMaxGroupId() + 1);

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                long groupId = groupIdsBlock.getLong(position);

                if (!values.isNull()) {
                    BooleanBigArrays.set(notNull, groupId, true);

                    double value = values.getDouble();
                    DoubleBigArrays.add(sums, groupId, value);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            if (BooleanBigArrays.get(notNull, groupId)) {
                double value = DoubleBigArrays.get(sums, groupId);
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
        return new DoubleSumAccumulator(valueChannel);
    }

    public static class DoubleSumAccumulator
            extends SimpleAccumulator
    {
        private boolean notNull;
        private double sum;

        public DoubleSumAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_DOUBLE, SINGLE_DOUBLE);
        }

        @Override
        protected void processInput(Block block)
        {
            BlockCursor intermediates = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition());
                if (!intermediates.isNull()) {
                    notNull = true;
                    sum += intermediates.getDouble();
                }
            }
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            if (notNull) {
                out.append(sum);
            }
            else {
                out.appendNull();
            }
        }
    }
}
