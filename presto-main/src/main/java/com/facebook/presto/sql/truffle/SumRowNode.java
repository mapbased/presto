package com.facebook.presto.sql.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;

public class SumRowNode
        extends RootNode
{
    @Child
    private RowNode segment;

    public SumRowNode(RowNode segment)
    {
        this.segment = segment;
    }

    @Override
    public Object execute(VirtualFrame virtualFrame)
    {
        double sum = 0;
        long processedRows = 0;
        while (true) {
            Row row = segment.executeRow(virtualFrame);
            if (row == null) {
//                System.out.println(sum + " " +  processedRows);
                // 1.2314107822829895E8
                return sum;
            }
            if (!row.isFiltered()) {
                sum += (row.getDouble(0) * row.getDouble(1));
                processedRows++;
            }
        }
    }
}
