package com.facebook.presto.sql.truffle;

import com.facebook.presto.operator.Operator;
import com.oracle.truffle.api.Arguments;

public class MyArguments
    extends Arguments
{
    private final Operator sourceOperator;
    private PageRow pageRow;

    public MyArguments(Operator sourceOperator)
    {
        this.sourceOperator = sourceOperator;
    }

    public Operator getSourceOperator()
    {
        return sourceOperator;
    }

    public PageRow getPageRow()
    {
//        // Note this loop assures the page row is advanced (including the newly created page rows) Very Awesome!!!
//        while (pageRow == null || !pageRow.advanceNext()) {
//            Operator operator = sourceOperator;
//            Page page = operator.getOutput();
//            if (page == null) {
//                return null;
//            }
//            pageRow = new PageRow(page);
//        }
        return pageRow;
    }

    public void setPageRow(PageRow pageRow)
    {
        this.pageRow = pageRow;
    }
}
