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
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrays;
import it.unimi.dsi.fastutil.longs.LongBigArrays;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.google.common.base.Preconditions.checkState;

public class ApproximateAverageAggregation
        extends SimpleAggregationFunction
{
    public static final AggregationFunction LONG_APPROXIMATE_AVERAGE_AGGREGATION = new ApproximateAverageAggregation(FIXED_INT_64);
    public static final AggregationFunction DOUBLE_APPROXIMATE_AVERAGE_AGGREGATION = new ApproximateAverageAggregation(DOUBLE);

    /**
     * Describes the tuple used by to calculate the variance.
     */
    private static final TupleInfo VARIANCE_CONTEXT_INFO = new TupleInfo(
            FIXED_INT_64,  // n
            DOUBLE,        // mean
            DOUBLE);       // m2


    private final boolean inputIsLong;

    public ApproximateAverageAggregation(Type parameterType)
    {
        // Intermediate type should be a fixed width structure
        super(SINGLE_VARBINARY, SINGLE_VARBINARY, parameterType);

        if (parameterType == Type.FIXED_INT_64) {
            this.inputIsLong = true;
        }
        else if (parameterType == Type.DOUBLE) {
            this.inputIsLong = false;
        } else {
            throw new IllegalArgumentException("Expected parameter type to be FIXED_INT_64 or DOUBLE, but was " + parameterType);
        }
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(long expectedSize, int valueChannel)
    {
        return new ApproximateAverageGroupedAccumulator(expectedSize, valueChannel, inputIsLong);
    }

    public static class ApproximateAverageGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final boolean inputIsLong;

        private long[][] counts;
        private double[][] means;
        private double[][] m2s;

        private ApproximateAverageGroupedAccumulator(long expectedSize, int valueChannel, boolean inputIsLong)
        {
            super(valueChannel, SINGLE_VARBINARY, SINGLE_VARBINARY);

            this.inputIsLong = inputIsLong;

            this.counts = LongBigArrays.newBigArray(expectedSize);
            this.means = DoubleBigArrays.newBigArray(expectedSize);
            this.m2s = DoubleBigArrays.newBigArray(expectedSize);
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            counts = LongBigArrays.grow(counts, groupIdsBlock.getMaxGroupId() + 1);
            means = DoubleBigArrays.grow(means, groupIdsBlock.getMaxGroupId() + 1);
            m2s = DoubleBigArrays.grow(m2s, groupIdsBlock.getMaxGroupId() + 1);

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                if (!values.isNull(0)) {

                    long groupId = groupIdsBlock.getLong(position);
                    double inputValue;
                    if (inputIsLong) {
                        inputValue = values.getLong(0);
                    }
                    else {
                        inputValue = values.getDouble(0);
                    }

                    long currentCount = LongBigArrays.get(counts, groupId);
                    double currentMean = DoubleBigArrays.get(means, groupId);

                    // Use numerically stable variant
                    currentCount++;
                    double delta = inputValue - currentMean;
                    currentMean += (delta / currentCount);
                    // update m2 inline
                    DoubleBigArrays.add(m2s, groupId, (delta * (inputValue - currentMean)));

                    // write values back out
                    LongBigArrays.set(counts, groupId, currentCount);
                    DoubleBigArrays.set(means, groupId, currentMean);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        protected void processIntermediate(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            counts = LongBigArrays.grow(counts, groupIdsBlock.getMaxGroupId() + 1);
            means = DoubleBigArrays.grow(means, groupIdsBlock.getMaxGroupId() + 1);
            m2s = DoubleBigArrays.grow(m2s, groupIdsBlock.getMaxGroupId() + 1);

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                if (!values.isNull(0)) {
                    long groupId = groupIdsBlock.getLong(position);

                    Slice slice = values.getSlice(0);
                    long inputCount = VARIANCE_CONTEXT_INFO.getLong(slice, 0);
                    double inputMean = VARIANCE_CONTEXT_INFO.getDouble(slice, 1);
                    double inputM2 = VARIANCE_CONTEXT_INFO.getDouble(slice, 2);

                    long currentCount = LongBigArrays.get(counts, groupId);
                    double currentMean = DoubleBigArrays.get(means, groupId);
                    double currentM2 = DoubleBigArrays.get(m2s, groupId);

                    // Use numerically stable variant
                    long newCount = currentCount + inputCount;
                    double newMean = ((currentCount * currentMean) + (inputCount * inputMean)) / newCount;
                    double delta = inputMean - currentMean;
                    double newM2 = currentM2 + inputM2 + ((delta * delta) * (currentCount * inputCount)) / newCount;

                    LongBigArrays.set(counts, groupId, newCount);
                    DoubleBigArrays.set(means, groupId, newMean);
                    DoubleBigArrays.set(m2s, groupId, newM2);

                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            long count = LongBigArrays.get(counts, groupId);
            double mean = DoubleBigArrays.get(means, groupId);
            double m2 = DoubleBigArrays.get(m2s, groupId);

            Slice intermediateValue = Slices.allocate(VARIANCE_CONTEXT_INFO.getFixedSize());
            VARIANCE_CONTEXT_INFO.setNotNull(intermediateValue, 0);
            VARIANCE_CONTEXT_INFO.setLong(intermediateValue, 0, count);
            VARIANCE_CONTEXT_INFO.setDouble(intermediateValue, 1, mean);
            VARIANCE_CONTEXT_INFO.setDouble(intermediateValue, 2, m2);

            output.append(intermediateValue);
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            long count = LongBigArrays.get(counts, groupId);
            if (count == 0) {
                output.appendNull();
            }
            else {
                double mean = DoubleBigArrays.get(means, groupId);
                double m2 = DoubleBigArrays.get(m2s, groupId);
                double variance = m2 / count;

                String result = formatApproximateAverage(count, mean, variance);
                output.append(result);
            }
        }
    }

    @Override
    protected Accumulator createAccumulator(int valueChannel)
    {
        return new ApproximateAverageAccumulator(valueChannel, inputIsLong);
    }

    public static class ApproximateAverageAccumulator
            extends SimpleAccumulator
    {
        private final boolean inputIsLong;

        private long currentCount;
        private double currentMean;
        private double currentM2;

        private ApproximateAverageAccumulator(int valueChannel, boolean inputIsLong)
        {
            super(valueChannel, SINGLE_VARBINARY, SINGLE_VARBINARY);

            this.inputIsLong = inputIsLong;
        }

        @Override
        protected void processInput(Block block)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                if (!values.isNull(0)) {
                    double inputValue;
                    if (inputIsLong) {
                        inputValue = values.getLong(0);
                    }
                    else {
                        inputValue = values.getDouble(0);
                    }

                    // Use numerically stable variant
                    currentCount++;
                    double delta = inputValue - currentMean;
                    currentMean += (delta / currentCount);
                    // update m2 inline
                    currentM2 += (delta * (inputValue - currentMean));
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        protected void processIntermediate(Block block)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                if (!values.isNull(0)) {
                    Slice slice = values.getSlice(0);
                    long inputCount = VARIANCE_CONTEXT_INFO.getLong(slice, 0);
                    double inputMean = VARIANCE_CONTEXT_INFO.getDouble(slice, 1);
                    double inputM2 = VARIANCE_CONTEXT_INFO.getDouble(slice, 2);

                    // Use numerically stable variant
                    long newCount = currentCount + inputCount;
                    double newMean = ((currentCount * currentMean) + (inputCount * inputMean)) / newCount;
                    double delta = inputMean - currentMean;
                    double newM2 = currentM2 + inputM2 + ((delta * delta) * (currentCount * inputCount)) / newCount;

                    currentCount = newCount;
                    currentMean = newMean;
                    currentM2 = newM2;

                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateIntermediate(BlockBuilder output)
        {
            Slice intermediateValue = Slices.allocate(VARIANCE_CONTEXT_INFO.getFixedSize());
            VARIANCE_CONTEXT_INFO.setNotNull(intermediateValue, 0);
            VARIANCE_CONTEXT_INFO.setLong(intermediateValue, 0, currentCount);
            VARIANCE_CONTEXT_INFO.setDouble(intermediateValue, 1, currentMean);
            VARIANCE_CONTEXT_INFO.setDouble(intermediateValue, 2, currentM2);

            output.append(intermediateValue);
        }

        @Override
        public void evaluateFinal(BlockBuilder output)
        {
            if (currentCount == 0) {
                output.appendNull();
            }
            else {
                String result = formatApproximateAverage(currentCount, currentMean, currentM2 / currentCount);
                output.append(result);
            }
        }
    }

    private static String formatApproximateAverage(long count, double mean, double variance)
    {
        // The multiplier 2.575 corresponds to the z-score of 99% confidence interval
        // (http://upload.wikimedia.org/wikipedia/commons/b/bb/Normal_distribution_and_scales.gif)
        double Z_SCORE = 2.575;

        // Error bars at 99% confidence interval
        StringBuilder sb = new StringBuilder();
        sb.append(mean);
        sb.append(" +/- ");
        sb.append(Z_SCORE * Math.sqrt(variance / count));
        return sb.toString();
    }
}
