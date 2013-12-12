package com.facebook.presto.sql.truffle;

import com.facebook.presto.operator.RandomAccessPage;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;

public class SumPageNode
        extends RootNode
{
    @Child
    private PageNode source;

    public SumPageNode(PageNode source)
    {
        this.source = source;
    }

    @Override
    public Object execute(VirtualFrame virtualFrame)
    {
        double sum = 0;
        long processedRows = 0;
        while (true) {
            RandomAccessPage page = source.executeRow(virtualFrame);
            if (page == null) {
                System.out.println(sum + " " +  processedRows);
                // 1.2314107822829895E8 114160
                return sum;
            }

            for (int row = 0; row < page.getPositionCount(); row++) {
                if (!page.isFiltered(row)) {
                    sum += (page.getDouble(0, row) * page.getDouble(1, row));
                    processedRows++;
                }
            }
        }
    }
}
