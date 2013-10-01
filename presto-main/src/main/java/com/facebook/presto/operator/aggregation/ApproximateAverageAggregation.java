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
import com.facebook.presto.tuple.TupleInfo.Type;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrays;
import it.unimi.dsi.fastutil.longs.LongBigArrays;

import static com.facebook.presto.operator.aggregation.VarianceAggregation.createIntermediate;
import static com.facebook.presto.operator.aggregation.VarianceAggregation.getCount;
import static com.facebook.presto.operator.aggregation.VarianceAggregation.getM2;
import static com.facebook.presto.operator.aggregation.VarianceAggregation.getMean;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.google.common.base.Preconditions.checkState;

public class ApproximateAverageAggregation
        extends SimpleAggregationFunction
{
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

                if (!values.isNull()) {

                    long groupId = groupIdsBlock.getLong(position);
                    double inputValue;
                    if (inputIsLong) {
                        inputValue = values.getLong();
                    }
                    else {
                        inputValue = values.getDouble();
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

                if (!values.isNull()) {
                    long groupId = groupIdsBlock.getLong(position);

                    Slice slice = values.getSlice();
                    long inputCount = getCount(slice);
                    double inputMean = getMean(slice);
                    double inputM2 = getM2(slice);

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

            output.append(createIntermediate(count, mean, m2));
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

                if (!values.isNull()) {
                    double inputValue;
                    if (inputIsLong) {
                        inputValue = values.getLong();
                    }
                    else {
                        inputValue = values.getDouble();
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

                if (!values.isNull()) {
                    Slice slice = values.getSlice();
                    long inputCount = getCount(slice);
                    double inputMean = getMean(slice);
                    double inputM2 = getM2(slice);

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
            output.append(createIntermediate(currentCount, currentMean, currentM2));
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
