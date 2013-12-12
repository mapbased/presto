package com.facebook.presto.sql.truffle;

import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.oracle.truffle.api.frame.VirtualFrame;

public class SourceRowNode
        extends RowNode
{
//    private final Operator operator;
//    private PageRow pageRow;
//
//    public SourceOperatorNode(Operator operator)
//    {
//        this.operator = operator;
//    }

    @Override
    public Row executeRow(VirtualFrame virtualFrame)
    {
        MyArguments arguments = virtualFrame.getArguments(MyArguments.class);

        // Note this loop assures the page row is advanced (including the newly created page rows) Very Awesome!!!
        PageRow pageRow = arguments.getPageRow();
        while (pageRow == null || !pageRow.advanceNext()) {
            Operator operator = arguments.getSourceOperator();

            Page page = operator.getOutput();
            if (page == null) {
                arguments.setPageRow(null);
                return null;
            }
            pageRow = new PageRow(page);
            arguments.setPageRow(pageRow);
        }

        return pageRow;
    }
}
