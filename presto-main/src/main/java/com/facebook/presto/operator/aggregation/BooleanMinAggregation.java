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
import com.facebook.presto.util.array.ByteBigArray;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_BOOLEAN;
import static com.facebook.presto.tuple.TupleInfo.Type.BOOLEAN;
import static com.google.common.base.Preconditions.checkState;

public class BooleanMinAggregation
        extends SimpleAggregationFunction
{
    public static final BooleanMinAggregation BOOLEAN_MIN = new BooleanMinAggregation();

    public BooleanMinAggregation()
    {
        super(SINGLE_BOOLEAN, SINGLE_BOOLEAN, BOOLEAN);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(int valueChannel)
    {
        return new BooleanMinGroupedAccumulator(valueChannel);
    }

    public static class BooleanMinGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        // null flag is zero so the default value in the min array is null
        private static final byte NULL_VALUE = 0;
        private static final byte TRUE_VALUE = 1;
        private static final byte FALSE_VALUE = -1;

        private final ByteBigArray minValues;

        public BooleanMinGroupedAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_BOOLEAN, SINGLE_BOOLEAN);
            this.minValues = new ByteBigArray();
        }

        @Override
        public long getEstimatedSize()
        {
            return minValues.sizeOf();
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            minValues.ensureCapacity(groupIdsBlock.getMaxGroupId() + 1);

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                // skip null values
                if (!values.isNull(0)) {
                    long groupId = groupIdsBlock.getGroupId(position);

                    // if value is false, update the min to false
                    if (!values.getBoolean(0)) {
                        minValues.set(groupId, FALSE_VALUE);
                    }
                    else {
                        // if the current value is null, set the min to true
                        if (minValues.get(groupId) == NULL_VALUE) {
                            minValues.set(groupId, TRUE_VALUE);
                        }
                    }
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            byte value = minValues.get((long) groupId);
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
        return new BooleanMinAccumulator(valueChannel);
    }

    public static class BooleanMinAccumulator
            extends SimpleAccumulator
    {
        private boolean notNull;
        private boolean min = true;

        public BooleanMinAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_BOOLEAN, SINGLE_BOOLEAN);
        }

        @Override
        protected void processInput(Block block)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                if (!values.isNull(0)) {
                    notNull = true;

                    // if value is false, update the max to false
                    if (!values.getBoolean(0)) {
                        min = false;
                    }
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
