package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AbstractPageIterator;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkState;

public class HandTpchQuery6
        extends AbstractOperatorBenchmark
{
    public HandTpchQuery6(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "hand_tpch_query_6", 10, 100);
    }

    @Override
    protected Operator createBenchmarkedOperator()
    {
        // select sum(extendedprice * discount) as revenue
        // from lineitem
        // where shipdate >= '1994-01-01'
        //    and shipdate < '1995-01-01'
        //    and discount >= 0.05
        //    and discount <= 0.07
        //    and quantity < 24;

        BlockIterable extendedPrice = getBlockIterable("lineitem", "extendedprice", BlocksFileEncoding.RAW);
        BlockIterable discount = getBlockIterable("lineitem", "discount", BlocksFileEncoding.RAW);
        BlockIterable shipDate = getBlockIterable("lineitem", "shipdate", BlocksFileEncoding.RAW);
        BlockIterable quantity = getBlockIterable("lineitem", "quantity", BlocksFileEncoding.RAW);

        AlignmentOperator alignmentOperator = new AlignmentOperator(extendedPrice, discount, shipDate, quantity);
        TpchQuery6Operator tpchQuery6Operator = new TpchQuery6Operator(alignmentOperator);
        return tpchQuery6Operator;
    }

    public static class TpchQuery6Operator
            implements Operator
    {
        private final Operator source;
        private final List<TupleInfo> tupleInfos;

        public TpchQuery6Operator(Operator source)
        {
            this.source = source;
            this.tupleInfos = ImmutableList.of(TupleInfo.SINGLE_DOUBLE);
        }

        @Override
        public int getChannelCount()
        {
            return 1;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public PageIterator iterator(OperatorStats operatorStats)
        {
            return new TpchQuery6Iterator(source.iterator(operatorStats));
        }

        private static class TpchQuery6Iterator
                extends AbstractPageIterator
        {
            private static final Slice MIN_SHIP_DATE = Slices.copiedBuffer("1994-01-01", UTF_8);
            private static final Slice MAX_SHIP_DATE = Slices.copiedBuffer("1995-01-01", UTF_8);

            private final PageIterator pageIterator;
            private boolean done;

            public TpchQuery6Iterator(PageIterator pageIterator)
            {
                super(ImmutableList.of(TupleInfo.SINGLE_DOUBLE));
                this.pageIterator = pageIterator;
            }

            protected Page computeNext()
            {
                if (done) {
                    return endOfData();
                }
                done = true;


                double sum = 0;
                boolean hasNonNull = false;
                while (pageIterator.hasNext()) {
                    Page page = pageIterator.next();


                    BlockCursor extendedPriceCursor = page.getBlock(0).cursor();
                    BlockCursor discountCursor = page.getBlock(1).cursor();
                    BlockCursor shipDateCursor = page.getBlock(2).cursor();
                    BlockCursor quantityCursor = page.getBlock(3).cursor();

                    int rows = page.getPositionCount();
                    for (int position = 0; position < rows; position++) {
                        checkState(extendedPriceCursor.advanceNextPosition());
                        checkState(discountCursor.advanceNextPosition());
                        checkState(shipDateCursor.advanceNextPosition());
                        checkState(quantityCursor.advanceNextPosition());

                        // where shipdate >= '1994-01-01'
                        //    and shipdate < '1995-01-01'
                        //    and discount >= 0.05
                        //    and discount <= 0.07
                        //    and quantity < 24;
                        if (filter(discountCursor, shipDateCursor, quantityCursor)) {
                            if (!discountCursor.isNull(0) && !extendedPriceCursor.isNull(0)) {
                                sum += extendedPriceCursor.getDouble(0) * discountCursor.getDouble(0);
                                hasNonNull = true;
                            }
                        }
                    }

                    checkState(!extendedPriceCursor.advanceNextPosition());
                    checkState(!discountCursor.advanceNextPosition());
                    checkState(!shipDateCursor.advanceNextPosition());
                    checkState(!quantityCursor.advanceNextPosition());
                }

                BlockBuilder builder = new BlockBuilder(TupleInfo.SINGLE_DOUBLE);

                if (hasNonNull) {
                    builder.append(sum);
                }
                else {
                    builder.appendNull();
                }

                return new Page(builder.build());
            }

            @Override
            protected void doClose()
            {
                pageIterator.close();
            }

            private boolean filter(BlockCursor discountCursor, BlockCursor shipDateCursor, BlockCursor quantityCursor)
            {
                return !shipDateCursor.isNull(0) && shipDateCursor.getSlice(0).compareTo(MIN_SHIP_DATE) >= 0 &&
                        !shipDateCursor.isNull(0) && shipDateCursor.getSlice(0).compareTo(MAX_SHIP_DATE) < 0 &&
                        !discountCursor.isNull(0) && discountCursor.getDouble(0) >= 0.05 &&
                        !discountCursor.isNull(0) && discountCursor.getDouble(0) <= 0.07 &&
                        !quantityCursor.isNull(0) && quantityCursor.getDouble(0) < 24;
            }
        }
    }

    public static void main(String[] args)
    {
        new HandTpchQuery6(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
