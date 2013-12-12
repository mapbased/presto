package com.facebook.presto.sql.truffle;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.google.common.base.Charsets.UTF_8;

public class TpchQuery6RowNode
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
        Operator operator = arguments.getSourceOperator();

        return executeTpchQuery6(operator);
    }

    public static double executeTpchQuery6(Operator operator)
    {
        double sum = 0;
        long processedRows = 0;
        while (true) {
            Page sourcePage = operator.getOutput();
            if (sourcePage == null) {
//                System.out.println(sum + " " +  processedRows);
                // 1.2314107822829895E8 114160
                return sum;
            }

            BlockCursor price = sourcePage.getBlock(PRICE).cursor();
            BlockCursor discount = sourcePage.getBlock(DISCOUNT).cursor();
            BlockCursor shipDate = sourcePage.getBlock(SHIP_DATE).cursor();
            BlockCursor quantity = sourcePage.getBlock(QUANTITY).cursor();

            for (int row = 0; row < sourcePage.getPositionCount(); row++) {
                price.advanceNextPosition();
                discount.advanceNextPosition();
                shipDate.advanceNextPosition();
                quantity.advanceNextPosition();

                if (filter(discount, shipDate, quantity)) {
                    sum += (price.getDouble() * discount.getDouble());
                    processedRows++;
                }
            }
        }
    }

    private static boolean filter(BlockCursor discount, BlockCursor shipDate, BlockCursor quantity)
    {
        return !shipDate.isNull() && shipDate.getSlice().compareTo(MIN_SHIP_DATE) >= 0 &&
                !shipDate.isNull() && shipDate.getSlice().compareTo(MAX_SHIP_DATE) < 0 &&
                !discount.isNull() && discount.getDouble() >= 0.05 &&
                !discount.isNull() && discount.getDouble() <= 0.07 &&
                !quantity.isNull() && quantity.getDouble() < 24;

    }
}
