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
package com.facebook.presto.benchmark;

import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.sql.truffle.BinaryNode.AddNode;
import com.facebook.presto.sql.truffle.BinaryNodeFactory.AddNodeFactory;
import com.facebook.presto.sql.truffle.Filter;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;

import java.util.concurrent.ExecutorService;

import static com.facebook.presto.sql.truffle.FilterTest.newLongLiteral;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TruffleWtf
        extends AbstractTruffleBenchmark
{
    private CallTarget function;

    public TruffleWtf(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
    {
        super(executor, tpchBlocksProvider, "truffle_tpch_query_6", 5, 500);
    }

    @Override
    protected Runnable createRootNode(TaskContext taskContext)
    {
        if (function == null) {
            AddNode addNode = AddNodeFactory.create(newLongLiteral(Long.MAX_VALUE), newLongLiteral(1));
            function = Truffle.getRuntime().createCallTarget(new Filter(addNode));
        }

        return new FunctionRunner(function);
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new TruffleWtf(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }

    private static class FunctionRunner
            implements Runnable
    {
        private final CallTarget function;

        private FunctionRunner(CallTarget function)
        {
            this.function = function;
        }

        @Override
        public void run()
        {
            for (int i = 0; i < 10000; i++) {
                function.call();
            }
        }
    }
}
