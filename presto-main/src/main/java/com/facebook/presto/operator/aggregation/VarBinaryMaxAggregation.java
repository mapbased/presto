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
import it.unimi.dsi.fastutil.objects.ObjectBigArrays;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkState;

public class VarBinaryMaxAggregation
        extends SimpleAggregationFunction
{
    public static final VarBinaryMaxAggregation VAR_BINARY_MAX = new VarBinaryMaxAggregation();

    public VarBinaryMaxAggregation()
    {
        super(SINGLE_VARBINARY, SINGLE_VARBINARY, VARIABLE_BINARY);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(long expectedSize, int valueChannel)
    {
        return new VarBinaryMaxGroupedAccumulator(expectedSize, valueChannel);
    }

    public static class VarBinaryMaxGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private Slice[][] maxValues;

        public VarBinaryMaxGroupedAccumulator(long expectedSize, int valueChannel)
        {
            super(valueChannel, SINGLE_VARBINARY, SINGLE_VARBINARY);

            this.maxValues = ObjectBigArrays.newBigArray(new Slice[0][], expectedSize);
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            maxValues = ObjectBigArrays.grow(maxValues, groupIdsBlock.getMaxGroupId() + 1);

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                // skip null values
                if (!values.isNull(0)) {
                    long groupId = groupIdsBlock.getLong(position);

                    Slice value = values.getSlice(0);

                    Slice max = max(value, ObjectBigArrays.get(maxValues, groupId));
                    ObjectBigArrays.set(maxValues, groupId, max);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            Slice value = ObjectBigArrays.get(maxValues, groupId);
            if (value == null) {
                output.appendNull();
            }
            else {
                output.append(value);
            }
        }
    }

    @Override
    protected Accumulator createAccumulator(int valueChannel)
    {
        return new VarBinaryMaxAccumulator(valueChannel);
    }

    public static class VarBinaryMaxAccumulator
            extends SimpleAccumulator
    {
        private Slice max;

        public VarBinaryMaxAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_VARBINARY, SINGLE_VARBINARY);
        }

        @Override
        protected void processInput(Block block)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                if (!values.isNull(0)) {
                    max = max(max, values.getSlice(0));
                }
            }
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            if (max != null) {
                out.append(max);
            }
            else {
                out.appendNull();
            }
        }
    }

    private static Slice max(Slice a, Slice b)
    {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return a.compareTo(b) > 0 ? a : b;
    }
}
