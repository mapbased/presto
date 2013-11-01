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
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.stats.QuantileDigest;
import it.unimi.dsi.fastutil.objects.ObjectBigArrays;

import java.util.List;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;

public class DoubleApproximatePercentileAggregation
        implements AggregationFunction
{
    public static final DoubleApproximatePercentileAggregation INSTANCE = new DoubleApproximatePercentileAggregation();

    @Override
    public List<Type> getParameterTypes()
    {
        return ImmutableList.of(DOUBLE, DOUBLE);
    }

    @Override
    public TupleInfo getFinalTupleInfo()
    {
        return TupleInfo.SINGLE_DOUBLE;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        return TupleInfo.SINGLE_VARBINARY;
    }

    @Override
    public DoubleApproximatePercentileGroupedAccumulator createGroupedAggregation(long expectedSize, int[] argumentChannels)
    {
        return new DoubleApproximatePercentileGroupedAccumulator(expectedSize, argumentChannels[0], argumentChannels[1]);
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAggregation(long expectedSize)
    {
        return new DoubleApproximatePercentileGroupedAccumulator(expectedSize, -1);
    }

    public static class DoubleApproximatePercentileGroupedAccumulator
            implements GroupedAccumulator
    {
        private final int valueChannel;
        private final int percentileChannel;
        private DigestAndPercentile[][] digests;

        public DoubleApproximatePercentileGroupedAccumulator(long expectedSize, int valueChannel, int percentileChannel)
        {
            this.digests = ObjectBigArrays.newBigArray(new DigestAndPercentile[0][], expectedSize);
            this.valueChannel = valueChannel;
            this.percentileChannel = percentileChannel;
        }

        public DoubleApproximatePercentileGroupedAccumulator(long expectedSize, int intermediateChannel)
        {
            this.digests = ObjectBigArrays.newBigArray(new DigestAndPercentile[0][], expectedSize);
            this.valueChannel = intermediateChannel;
            this.percentileChannel = -1;
        }

        @Override
        public long getEstimatedSize()
        {
            return 0;
        }

        @Override
        public TupleInfo getFinalTupleInfo()
        {
            return SINGLE_DOUBLE;
        }

        @Override
        public TupleInfo getIntermediateTupleInfo()
        {
            return SINGLE_VARBINARY;
        }

        @Override
        public void addInput(GroupByIdBlock groupIdsBlock, Page page)
        {
            checkArgument(percentileChannel != -1, "Raw input is not allowed for a final aggregation");

            digests = ObjectBigArrays.grow(digests, groupIdsBlock.getMaxGroupId() + 1);

            BlockCursor values = page.getBlock(valueChannel).cursor();
            BlockCursor percentiles = page.getBlock(percentileChannel).cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(percentiles.advanceNextPosition());

                long groupId = groupIdsBlock.getLong(position);

                // skip null values
                if (!values.isNull(0)) {

                    DigestAndPercentile currentValue = ObjectBigArrays.get(digests, groupId);
                    if (currentValue == null) {
                        currentValue = new DigestAndPercentile(new QuantileDigest(0.01));
                        ObjectBigArrays.set(digests, groupId, currentValue);
                    }

                    double value = values.getDouble(0);
                    currentValue.digest.add(doubleToSortableLong(value));


                    // use last non-null percentile
                    if (!percentiles.isNull(0)) {
                        currentValue.percentile = percentiles.getDouble(0);
                    }
                }
            }
            checkState(!values.advanceNextPosition());
            checkState(!percentiles.advanceNextPosition());
        }

        @Override
        public void addIntermediate(GroupByIdBlock groupIdsBlock, Block block)
        {
            checkArgument(percentileChannel == -1, "Intermediate input is only allowed for a final aggregation");

            digests = ObjectBigArrays.grow(digests, groupIdsBlock.getMaxGroupId() + 1);

            BlockCursor intermediates = block.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition());

                if (!intermediates.isNull(0)) {
                    long groupId = groupIdsBlock.getLong(position);

                    DigestAndPercentile currentValue = ObjectBigArrays.get(digests, groupId);
                    if (currentValue == null) {
                        currentValue = new DigestAndPercentile(new QuantileDigest(0.01));
                        ObjectBigArrays.set(digests, groupId, currentValue);
                    }

                    SliceInput input = intermediates.getSlice(0).getInput();
                    // read digest
                    currentValue.digest.merge(QuantileDigest.deserialize(input));
                    // read percentile
                    currentValue.percentile = input.readDouble();
                }
            }
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            DigestAndPercentile currentValue = ObjectBigArrays.get(digests, groupId);
            if (currentValue == null || currentValue.digest.getCount() == 0.0) {
                output.appendNull();
            }
            else {
                DynamicSliceOutput sliceOutput = new DynamicSliceOutput(currentValue.digest.estimatedSerializedSizeInBytes() + SIZE_OF_DOUBLE);
                // write digest
                currentValue.digest.serialize(sliceOutput);
                // write percentile
                sliceOutput.appendDouble(currentValue.percentile);

                Slice slice = sliceOutput.slice();
                output.append(slice);
            }
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            DigestAndPercentile currentValue = ObjectBigArrays.get(digests, groupId);
            if (currentValue == null || currentValue.digest.getCount() == 0.0) {
                output.appendNull();
            }
            else {
                Preconditions.checkState(currentValue.percentile != -1.0, "Percentile is missing");
                output.append(longToDouble(currentValue.digest.getQuantile(currentValue.percentile)));
            }
        }
    }

    @Override
    public DoubleApproximatePercentileAccumulator createAggregation(int... argumentChannels)
    {
        return new DoubleApproximatePercentileAccumulator(argumentChannels[0], argumentChannels[1]);
    }

    @Override
    public DoubleApproximatePercentileAccumulator createIntermediateAggregation()
    {
        return new DoubleApproximatePercentileAccumulator(-1, -1);
    }

    public static class DoubleApproximatePercentileAccumulator
            implements Accumulator
    {
        private final int valueChannel;
        private final int percentileChannel;

        private QuantileDigest digest = new QuantileDigest(0.01);
        private double percentile = -1;

        public DoubleApproximatePercentileAccumulator(int valueChannel, int percentileChannel)
        {
            this.valueChannel = valueChannel;
            this.percentileChannel = percentileChannel;
        }

        @Override
        public TupleInfo getFinalTupleInfo()
        {
            return SINGLE_DOUBLE;
        }

        @Override
        public TupleInfo getIntermediateTupleInfo()
        {
            return SINGLE_VARBINARY;
        }

        @Override
        public void addInput(Page page)
        {
            checkArgument(valueChannel != -1, "Raw input is not allowed for a final aggregation");

            BlockCursor values = page.getBlock(valueChannel).cursor();
            BlockCursor percentiles = page.getBlock(percentileChannel).cursor();

            for (int position = 0; position < page.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(percentiles.advanceNextPosition());

                if (!values.isNull(0)) {
                    double value = values.getDouble(0);
                    digest.add(doubleToSortableLong(value));


                    // use last non-null percentile
                    if (!percentiles.isNull(0)) {
                        percentile = percentiles.getDouble(0);
                    }
                }
            }
        }

        @Override
        public void addIntermediate(Block block)
        {
            checkArgument(valueChannel == -1, "Intermediate input is only allowed for a final aggregation");

            BlockCursor intermediates = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition());
                if (!intermediates.isNull(0)) {
                    SliceInput input = intermediates.getSlice(0).getInput();
                    // read digest
                    digest.merge(QuantileDigest.deserialize(input));
                    // read percentile
                    percentile = input.readDouble();
                }
            }
        }

        @Override
        public final Block evaluateIntermediate()
        {
            BlockBuilder out = new BlockBuilder(getIntermediateTupleInfo());

            if (digest.getCount() == 0.0) {
                out.appendNull();
            }
            else {
                DynamicSliceOutput sliceOutput = new DynamicSliceOutput(digest.estimatedSerializedSizeInBytes() + SIZE_OF_DOUBLE);
                // write digest
                digest.serialize(sliceOutput);
                // write percentile
                sliceOutput.appendDouble(percentile);

                Slice slice = sliceOutput.slice();
                out.append(slice);
            }

            return out.build();
        }

        @Override
        public final Block evaluateFinal()
        {
            BlockBuilder out = new BlockBuilder(getFinalTupleInfo());

            if (digest.getCount() == 0.0) {
                out.appendNull();
            }
            else {
                Preconditions.checkState(percentile != -1.0, "Percentile is missing");
                out.append(longToDouble(digest.getQuantile(percentile)));
            }

            return out.build();
        }
    }

    private static double longToDouble(long value)
    {
        if (value < 0) {
            value ^= 0x7fffffffffffffffL;
        }

        return Double.longBitsToDouble(value);
    }

    private static long doubleToSortableLong(double value)
    {
        long result = Double.doubleToRawLongBits(value);

        if (result < 0) {
            result ^= 0x7fffffffffffffffL;
        }

        return result;
    }

    public final static class DigestAndPercentile
    {
        private QuantileDigest digest;
        private double percentile = -1;

        public DigestAndPercentile(QuantileDigest digest)
        {
            this.digest = digest;
        }
    }
}
