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
import io.airlift.slice.SizeOf;
import it.unimi.dsi.fastutil.bytes.ByteBigArrays;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_BOOLEAN;
import static com.facebook.presto.tuple.TupleInfo.Type.BOOLEAN;
import static com.google.common.base.Preconditions.checkState;

public class BooleanMaxAggregation
        extends SimpleAggregationFunction
{
    public static final BooleanMaxAggregation BOOLEAN_MAX = new BooleanMaxAggregation();

    public BooleanMaxAggregation()
    {
        super(SINGLE_BOOLEAN, SINGLE_BOOLEAN, BOOLEAN);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(long expectedSize, int valueChannel)
    {
        return new BooleanMinGroupedAccumulator(expectedSize, valueChannel);
    }

    public static class BooleanMinGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private static final byte NULL_VALUE = 0;
        private static final byte TRUE_VALUE = 1;
        private static final byte FALSE_VALUE = -1;

        private byte[][] maxValues;

        public BooleanMinGroupedAccumulator(long expectedSize, int valueChannel)
        {
            super(valueChannel, SINGLE_BOOLEAN, SINGLE_BOOLEAN);
            this.maxValues = ByteBigArrays.newBigArray(expectedSize);
        }

        @Override
        public long getEstimatedSize()
        {
            return SizeOf.SIZE_OF_BYTE * ByteBigArrays.length(maxValues);
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            maxValues = ByteBigArrays.grow(maxValues, groupIdsBlock.getMaxGroupId() + 1);

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                // skip null values
                if (!values.isNull()) {
                    long groupId = groupIdsBlock.getLong(position);

                    // if value is true, update the max to true
                    if (values.getBoolean()) {
                        ByteBigArrays.set(maxValues, groupId, TRUE_VALUE);
                    }
                    else {
                        // if the current value is null, set the max to false
                        if (ByteBigArrays.get(maxValues, groupId) == NULL_VALUE) {
                            ByteBigArrays.set(maxValues, groupId, FALSE_VALUE);
                        }
                    }
                }
            }
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            byte value = ByteBigArrays.get(maxValues, groupId);
            if (value == NULL_VALUE) {
                output.appendNull();
            }
            else {
                output.append(value == TRUE_VALUE);
            }
        }
    }

    @Override
    protected Accumulator createAccumulator(int valueChannel)
    {
        return new BooleanMaxAccumulator(valueChannel);
    }

    public static class BooleanMaxAccumulator
            extends SimpleAccumulator
    {
        private boolean notNull;
        private boolean max;

        public BooleanMaxAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_BOOLEAN, SINGLE_BOOLEAN);
        }

        @Override
        protected void processInput(Block block)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                if (!values.isNull()) {
                    notNull = true;

                    // if value is true, update the max to true
                    if (values.getBoolean()) {
                        max = true;
                    }
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
