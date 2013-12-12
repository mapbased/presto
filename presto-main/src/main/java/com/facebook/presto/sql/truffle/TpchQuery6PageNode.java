package com.facebook.presto.sql.truffle;

import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.RandomAccessPage;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.google.common.base.Charsets.UTF_8;

public class TpchQuery6PageNode
        extends RootNode
{
    private static final Slice MIN_SHIP_DATE = Slices.copiedBuffer("1994-01-01", UTF_8);
    private static final Slice MAX_SHIP_DATE = Slices.copiedBuffer("1995-01-01", UTF_8);
    private static final int PRICE = 0;
    private static final int DISCOUNT = 1;
    private static final int SHIP_DATE = 2;
    private static final int QUANTITY = 3;

    @Override
    public Object execute(VirtualFrame virtualFrame)
    {
        MyArguments arguments = virtualFrame.getArguments(MyArguments.class);
        double sum = 0;
        long processedRows = 0;
        while (true) {
            Operator operator = arguments.getSourceOperator();
            Page sourcePage = operator.getOutput();
            if (sourcePage == null) {
//                System.out.println(sum + " " +  processedRows);
                // 1.2314107822829895E8 114160
                return sum;
            }

            RandomAccessPage page = new RandomAccessPage(sourcePage);
            for (int row = 0; row < page.getPositionCount(); row++) {
                if (filter(page, row)) {
                    sum += (page.getDouble(PRICE, row) * page.getDouble(DISCOUNT, row));
                    processedRows++;
                }
            }
        }
    }

    public static boolean filter(RandomAccessPage page, int row)
    {
        return !page.isNull(SHIP_DATE, row) && page.getSlice(SHIP_DATE, row).compareTo(MIN_SHIP_DATE) >= 0 &&
                !page.isNull(SHIP_DATE, row) && page.getSlice(SHIP_DATE, row).compareTo(MAX_SHIP_DATE) < 0 &&
                !page.isNull(DISCOUNT, row) && page.getDouble(DISCOUNT, row) >= 0.05 &&
                !page.isNull(DISCOUNT, row) && page.getDouble(DISCOUNT, row) <= 0.07 &&
                !page.isNull(QUANTITY, row) && page.getDouble(QUANTITY, row) < 24;
    }
}
