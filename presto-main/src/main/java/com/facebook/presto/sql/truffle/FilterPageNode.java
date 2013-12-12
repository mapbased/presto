package com.facebook.presto.sql.truffle;

import com.facebook.presto.operator.RandomAccessPage;
import com.oracle.truffle.api.frame.VirtualFrame;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.google.common.base.Charsets.UTF_8;

public class FilterPageNode
        extends PageNode
{
    private static final Slice MIN_SHIP_DATE = Slices.copiedBuffer("1994-01-01", UTF_8);
    private static final Slice MAX_SHIP_DATE = Slices.copiedBuffer("1995-01-01", UTF_8);
    private static final int SHIP_DATE = 2;
    private static final int DISCOUNT = 1;
    private static final int QUANTITY = 3;

    @Child
    private PageNode source;

    public FilterPageNode(PageNode source)
    {
        this.source = source;
    }

    @Override
    public RandomAccessPage executeRow(VirtualFrame virtualFrame)
    {
        RandomAccessPage page = source.executeRow(virtualFrame);
        if (page == null) {
            return page;
        }

        for (int row = 0; row < page.getPositionCount(); row++) {
            if (!page.isFiltered(row) && !filter(page, row)) {
                page.setFiltered(row);
            }
        }
        return page;
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
