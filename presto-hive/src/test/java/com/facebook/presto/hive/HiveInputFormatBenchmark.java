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
package com.facebook.presto.hive;

import com.facebook.hive.orc.OrcSerde;
import com.facebook.hive.orc.lazy.OrcLazyObject;
import com.facebook.hive.orc.lazy.OrcLazyRow;
import com.facebook.presto.hadoop.HadoopNative;
import com.facebook.presto.hive.shaded.org.apache.commons.codec.binary.Base64;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.airlift.log.Logging;
import io.airlift.log.LoggingConfiguration;
import io.airlift.log.LoggingMBean;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.LineItemGenerator;
import io.airlift.tpch.Order;
import io.airlift.tpch.OrderGenerator;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.FSRecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import parquet.Log;
import sun.misc.Unsafe;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.facebook.presto.hive.HiveBooleanParser.isFalse;
import static com.facebook.presto.hive.HiveBooleanParser.isTrue;
import static com.facebook.presto.hive.HiveInputFormatBenchmark.HiveColumn.nameGetter;
import static com.facebook.presto.hive.HiveInputFormatBenchmark.HiveColumn.objectInspectorGetter;
import static com.facebook.presto.hive.HiveInputFormatBenchmark.HiveColumn.typeNameGetter;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_CODEC;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_TYPE;

@SuppressWarnings("deprecation")
public final class HiveInputFormatBenchmark
{
    public static final int LOOPS = 1;
    private static final String NOT_SUPPORTED = "NOT_SUPPORTED";
    private static final File DATA_DIR = new File("target");

    private static final List<HiveColumn> ORDER_COLUMNS = ImmutableList.of(
            new HiveColumn("orderkey", javaLongObjectInspector),
            new HiveColumn("custkey", javaLongObjectInspector),
            new HiveColumn("orderstatus", javaStringObjectInspector),
            new HiveColumn("totalprice", javaDoubleObjectInspector),
            new HiveColumn("orderdate", javaStringObjectInspector),
            new HiveColumn("orderpriority", javaStringObjectInspector),
            new HiveColumn("clerk", javaStringObjectInspector),
            new HiveColumn("shippriority", javaLongObjectInspector),
            new HiveColumn("comment", javaStringObjectInspector)
    );

    private static final List<HiveColumn> LINE_ITEM_COLUMNS = ImmutableList.of(
            new HiveColumn("orderkey", javaLongObjectInspector),
            new HiveColumn("partkey", javaLongObjectInspector),
            new HiveColumn("suppkey", javaLongObjectInspector),
            new HiveColumn("linenumber", javaLongObjectInspector),
            new HiveColumn("quantity", javaLongObjectInspector),
            new HiveColumn("extendedprice", javaDoubleObjectInspector),
            new HiveColumn("discount", javaDoubleObjectInspector),
            new HiveColumn("tax", javaDoubleObjectInspector),
            new HiveColumn("returnflag", javaStringObjectInspector),
            new HiveColumn("linestatus", javaStringObjectInspector),
            new HiveColumn("shipdate", javaStringObjectInspector),
            new HiveColumn("commitdate", javaStringObjectInspector),
            new HiveColumn("receiptdate", javaStringObjectInspector),
            new HiveColumn("shipinstruct", javaStringObjectInspector),
            new HiveColumn("shipmode", javaStringObjectInspector),
            new HiveColumn("comment", javaStringObjectInspector)
    );

    private HiveInputFormatBenchmark()
    {
    }

    public static void main(String[] args)
            throws Exception
    {
        workAroundParquetBrokenLoggingSetup();

        HadoopNative.requireHadoopNative();
        DATA_DIR.mkdirs();

        List<BenchmarkFile> benchmarkFiles = ImmutableList.of(
//                new BenchmarkFile(
//                        "parquet",
//                        "parquet",
//                        new MapredParquetInputFormat(),
//                        new MapredParquetOutputFormat(),
//                        new ParquetHiveSerDe(),
//                        new ParquetHiveSerDe(),
//                        null,
//                        true),
//
//                new BenchmarkFile(
//                        "parquet gzip",
//                        "parquet.gz",
//                        new MapredParquetInputFormat(),
//                        new MapredParquetOutputFormat(),
//                        new ParquetHiveSerDe(),
//                        new ParquetHiveSerDe(),
//                        "gzip",
//                        true),
//
//                new BenchmarkFile(
//                        "parquet snappy",
//                        "parquet.snappy",
//                        new MapredParquetInputFormat(),
//                        new MapredParquetOutputFormat(),
//                        new ParquetHiveSerDe(),
//                        new ParquetHiveSerDe(),
//                        "snappy",
//                        true),

                new BenchmarkFile(
                        "orc",
                        "orc",
                        new org.apache.hadoop.hive.ql.io.orc.OrcInputFormat(),
                        new org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat(),
                        new org.apache.hadoop.hive.ql.io.orc.OrcSerde(),
                        new org.apache.hadoop.hive.ql.io.orc.OrcSerde(),
                        null,
                        true,
                        ImmutableList.of(new BenchmarkLineItemGeneric())),

//                new BenchmarkFile(
//                        "dwrf",
//                        "dwrf",
//                        new OrcInputFormat(),
//                        new OrcOutputFormat(),
//                        new OrcSerde(),
//                        new OrcSerde(),
//                        null,
//                        true),

                new BenchmarkFile(
                        "rc binary gzip",
                        "rc-binary.gz",
                        new RCFileInputFormat<>(),
                        new RCFileOutputFormat(),
                        new LazyBinaryColumnarSerDe(),
                        new LazyBinaryColumnarSerDe(),
                        "gzip",
                        true,
                        ImmutableList.of(new BenchmarkLineItemGeneric(), new BenchmarkLineItemRCBinary()))
//
//                new BenchmarkFile(
//                        "rc text gzip",
//                        "rc.gz",
//                        new RCFileInputFormat<>(),
//                        new RCFileOutputFormat(),
//                        new ColumnarSerDe(),
//                        new ColumnarSerDe(),
//                        "gzip",
//                        true)

//                new BenchmarkFile(
//                        "rc binary",
//                        new File("target/presto_test.rc-binary"),
//                        new RCFileInputFormat<>(),
//                        new RCFileOutputFormat(),
//                        new LazyBinaryColumnarSerDe(),
//                        null,
//                        true),
//
//                new BenchmarkFile(
//                        "rc binary snappy",
//                        new File("target/presto_test.rc-binary.snappy"),
//                        new RCFileInputFormat<>(),
//                        new RCFileOutputFormat(),
//                        new LazyBinaryColumnarSerDe(),
//                        "snappy",
//                        true)
//
//                new BenchmarkFile(
//                        "rc text",
//                        new File("target/presto_test.rc"),
//                        new RCFileInputFormat<>(),
//                        new RCFileOutputFormat(),
//                        new ColumnarSerDe(),
//                        null,
//                        true),
//
//                new BenchmarkFile(
//                        "rc text snappy",
//                        new File("target/presto_test.rc.snappy"),
//                        new RCFileInputFormat<>(),
//                        new RCFileOutputFormat(),
//                        new ColumnarSerDe(),
//                        "snappy",
//                        true),
//
//                new BenchmarkFile(
//                        "text",
//                        new File("target/presto_test.txt"),
//                        new TextInputFormat(),
//                        new HiveIgnoreKeyTextOutputFormat<>(),
//                        new LazySimpleSerDe(),
//                        null,
//                        true),
//
//                new BenchmarkFile(
//                        "text gzip",
//                        new File("target/presto_test.txt.gz"),
//                        new TextInputFormat(),
//                        new HiveIgnoreKeyTextOutputFormat<>(),
//                        new LazySimpleSerDe(),
//                        "gzip",
//                        true),
//
//                new BenchmarkFile(
//                        "text snappy",
//                        new File("target/presto_test.txt.snappy"),
//                        new TextInputFormat(),
//                        new HiveIgnoreKeyTextOutputFormat<>(),
//                        new LazySimpleSerDe(),
//                        "snappy",
//                        true),
//
//                new BenchmarkFile(
//                        "sequence",
//                        new File("target/presto_test.sequence"),
//                        new SequenceFileInputFormat<Object, Writable>(),
//                        new HiveSequenceFileOutputFormat<>(),
//                        new LazySimpleSerDe(),
//                        null,
//                        true),
//
//                new BenchmarkFile(
//                        "sequence gzip",
//                        new File("target/presto_test.sequence.gz"),
//                        new SequenceFileInputFormat<Object, Writable>(),
//                        new HiveSequenceFileOutputFormat<>(),
//                        new LazySimpleSerDe(),
//                        "gzip",
//                        true),
//
//                new BenchmarkFile(
//                        "sequence snappy",
//                        new File("target/presto_test.sequence.snappy"),
//                        new SequenceFileInputFormat<Object, Writable>(),
//                        new HiveSequenceFileOutputFormat<>(),
//                        new LazySimpleSerDe(),
//                        "snappy",
//                        true),
//
        );

        JobConf jobConf = new JobConf();
        System.out.println("============ WARM UP ============");
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            benchmarkLineItem(jobConf, benchmarkFile, 1);
        }

        System.out.println();
        System.out.println();
        System.out.println("============ BENCHMARK ============");
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            benchmarkLineItem(jobConf, benchmarkFile, 4);
        }
    }

    private static void workAroundParquetBrokenLoggingSetup()
            throws IOException
    {
        // unhook out and err while initializing logging or logger will print to them
        PrintStream out = System.out;
        PrintStream err = System.err;
        try {
            System.setOut(new PrintStream(nullOutputStream()));
            System.setErr(new PrintStream(nullOutputStream()));

            Log.getLog(Object.class);
            Logging logging = Logging.initialize();
            logging.configure(new LoggingConfiguration());
            logging.disableConsole();
        }
        finally {
            System.setOut(out);
            System.setErr(err);
        }

        LoggingMBean logging = new LoggingMBean();
        logging.setLevel("com.hadoop", "OFF");
        logging.setLevel("org.apache.hadoop", "OFF");
        logging.setLevel("org.apache.zookeeper", "OFF");
        logging.setLevel("parquet", "OFF");
    }

    private static void benchmarkOrder(JobConf jobConf, BenchmarkFile benchmarkFile, int loopCount)
            throws Exception
    {
        System.out.println();
        System.out.println(benchmarkFile.getName());

        Object value = null;

        long start;

        //
        // orderKey
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = BenchmarkOrderGeneric.orderKey(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
        }
        logDuration("orderKey", start, loopCount, value);

        //
        // customerKey
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = BenchmarkOrderGeneric.customerKey(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
        }
        logDuration("customerKey", start, loopCount, value);

        //
        // orderStatus
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = BenchmarkOrderGeneric.orderStatus(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
        }
        logDuration("orderStatus", start, loopCount, value);

        //
        // totalPrice
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = BenchmarkOrderGeneric.totalPrice(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
        }
        logDuration("totalPrice", start, loopCount, value);

        //
        // orderDate
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = BenchmarkOrderGeneric.orderDate(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
        }
        logDuration("orderDate", start, loopCount, value);

        //
        // orderPriority
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = BenchmarkOrderGeneric.orderPriority(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
        }
        logDuration("orderPriority", start, loopCount, value);

        //
        // clerk
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = BenchmarkOrderGeneric.clerk(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
        }
        logDuration("clerk", start, loopCount, value);

        //
        // shipPriority
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = BenchmarkOrderGeneric.shipPriority(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
        }
        logDuration("shipPriority", start, loopCount, value);

        //
        // comment
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = BenchmarkOrderGeneric.comment(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
        }
        logDuration("comment", start, loopCount, value);
    }

    private static void benchmarkLineItem(JobConf jobConf, BenchmarkFile benchmarkFile, int loopCount)
            throws Exception
    {
        for (BenchmarkLineItem benchmarkLineItem : benchmarkFile.getLineItemBenchmarks()) {
            benchmarkLineItem(jobConf, benchmarkFile, loopCount, benchmarkLineItem);
        }
    }

    private static void benchmarkLineItem(JobConf jobConf, BenchmarkFile benchmarkFile, int loopCount, BenchmarkLineItem benchmarkLineItem)
            throws Exception
    {
        System.out.println();
        System.out.println(benchmarkFile.getName() + " " + benchmarkLineItem.getName());

        Object value = null;

        long start;

        //
        // orderKey
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.orderKey(jobConf, benchmarkFile.getLineItemFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getLineItemDeserializer());
        }
        logDuration("orderKey", start, loopCount, value);

        //
        // orderKey
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.partKey(jobConf, benchmarkFile.getLineItemFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getLineItemDeserializer());
        }
        logDuration("partKey", start, loopCount, value);

        //
        // supplierKey
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.supplierKey(jobConf, benchmarkFile.getLineItemFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getLineItemDeserializer());
        }
        logDuration("supplierKey", start, loopCount, value);

        //
        // lineNumber
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.lineNumber(jobConf, benchmarkFile.getLineItemFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getLineItemDeserializer());
        }
        logDuration("lineNumber", start, loopCount, value);

        //
        // quantity
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.quantity(jobConf, benchmarkFile.getLineItemFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getLineItemDeserializer());
        }
        logDuration("quantity", start, loopCount, value);

        //
        // extendedPrice
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.extendedPrice(jobConf, benchmarkFile.getLineItemFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getLineItemDeserializer());
        }
        logDuration("extendedPrice", start, loopCount, value);

        //
        // discount
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.discount(jobConf, benchmarkFile.getLineItemFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getLineItemDeserializer());
        }
        logDuration("discount", start, loopCount, value);

        //
        // tax
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.tax(jobConf, benchmarkFile.getLineItemFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getLineItemDeserializer());
        }
        logDuration("tax", start, loopCount, value);

        //
        // returnFlag
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.returnFlag(jobConf, benchmarkFile.getLineItemFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getLineItemDeserializer());
        }
        logDuration("returnFlag", start, loopCount, value);

        //
        // status
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.status(jobConf, benchmarkFile.getLineItemFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getLineItemDeserializer());
        }
        logDuration("status", start, loopCount, value);

        //
        // shipDate
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.shipDate(jobConf, benchmarkFile.getLineItemFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getLineItemDeserializer());
        }
        logDuration("shipDate", start, loopCount, value);

        //
        // commitDate
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.commitDate(jobConf, benchmarkFile.getLineItemFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getLineItemDeserializer());
        }
        logDuration("commitDate", start, loopCount, value);

        //
        // receiptDate
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.receiptDate(jobConf, benchmarkFile.getLineItemFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getLineItemDeserializer());
        }
        logDuration("receiptDate", start, loopCount, value);

        //
        // shipInstructions
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.shipInstructions(jobConf, benchmarkFile.getLineItemFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getLineItemDeserializer());
        }
        logDuration("shipInstructions", start, loopCount, value);

        //
        // shipMode
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.shipMode(jobConf, benchmarkFile.getLineItemFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getLineItemDeserializer());
        }
        logDuration("shipMode", start, loopCount, value);

        //
        // comment
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.comment(jobConf, benchmarkFile.getLineItemFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getLineItemDeserializer());
        }
        logDuration("comment", start, loopCount, value);

        //
        // tpchQuery6
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.tpchQuery6(jobConf, benchmarkFile.getLineItemFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getLineItemDeserializer());
        }
        logDuration("tpchQuery6", start, loopCount, value);

        //
        // tpchQuery1
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.tpchQuery1(jobConf, benchmarkFile.getLineItemFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getLineItemDeserializer());
        }
        logDuration("tpchQuery1", start, loopCount, value);

        //
        // all
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.all(jobConf, benchmarkFile.getLineItemFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getLineItemDeserializer());
        }
        logDuration("all", start, loopCount, value);
    }

    private static void benchmarkOld(JobConf jobConf, BenchmarkFile benchmarkFile, int loopCount)
            throws Exception
    {
        System.out.println();
        System.out.println(benchmarkFile.getName());

        Object value = null;

        long start;

        //
        // string
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkReadComment(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
        }
        logDuration("string", start, loopCount, value);

        start = System.nanoTime();
        if (benchmarkFile.getOrderDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadCommentText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadCommentColumnarText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadCommentColumnarBinary(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadCommentDwrf(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof org.apache.hadoop.hive.ql.io.orc.OrcSerde) {
            value = "NOT_SUPPORTED";
        }
        else {
            throw new UnsupportedOperationException("Unsupported serde " + benchmarkFile.getOrderDeserializer().getClass().getName());
        }
        logDuration("p_string", start, loopCount, value);

        //
        // smallint
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkReadSmallint(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
        }
        logDuration("smallint", start, loopCount, value);

        start = System.nanoTime();
        if (benchmarkFile.getOrderDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadSmallintText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadSmallintColumnarText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadSmallintColumnarBinary(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadSmallintDwrf(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof org.apache.hadoop.hive.ql.io.orc.OrcSerde) {
            value = "NOT_SUPPORTED";
        }
        else {
            throw new UnsupportedOperationException("Unsupported serde " + benchmarkFile.getOrderDeserializer().getClass().getName());
        }
        logDuration("p_smallint", start, loopCount, value);

        //
        // int
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkReadInt(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
        }
        logDuration("int", start, loopCount, value);

        start = System.nanoTime();
        if (benchmarkFile.getOrderDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadIntText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadIntColumnarText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadIntColumnarBinary(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadIntDwrf(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof org.apache.hadoop.hive.ql.io.orc.OrcSerde) {
            value = "NOT_SUPPORTED";
        }
        else {
            throw new UnsupportedOperationException("Unsupported serde " + benchmarkFile.getOrderDeserializer().getClass().getName());
        }
        logDuration("p_int", start, loopCount, value);

        //
        // bigint
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkReadBigint(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
        }
        logDuration("bigint", start, loopCount, value);

        start = System.nanoTime();
        if (benchmarkFile.getOrderDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBigintText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBigintColumnarText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBigintColumnarBinary(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBigintDwrf(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof org.apache.hadoop.hive.ql.io.orc.OrcSerde) {
            value = "NOT_SUPPORTED";
        }
        else {
            throw new UnsupportedOperationException("Unsupported serde " + benchmarkFile.getOrderDeserializer().getClass().getName());
        }
        logDuration("p_bigint", start, loopCount, value);

        //
        // float
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkReadFloat(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
        }
        logDuration("float", start, loopCount, value);

        start = System.nanoTime();
        if (benchmarkFile.getOrderDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadFloatText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadFloatColumnarText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadFloatColumnarBinary(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadFloatDwrf(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof org.apache.hadoop.hive.ql.io.orc.OrcSerde) {
            value = "NOT_SUPPORTED";
        }
        else {
            throw new UnsupportedOperationException("Unsupported serde " + benchmarkFile.getOrderDeserializer().getClass().getName());
        }
        logDuration("p_float", start, loopCount, value);

        //
        // double
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkReadDouble(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
        }
        logDuration("double", start, loopCount, value);

        start = System.nanoTime();
        if (benchmarkFile.getOrderDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadDoubleText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadDoubleColumnarText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadDoubleColumnarBinary(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadDoubleDwrf(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof org.apache.hadoop.hive.ql.io.orc.OrcSerde) {
            value = "NOT_SUPPORTED";
        }
        else {
            throw new UnsupportedOperationException("Unsupported serde " + benchmarkFile.getOrderDeserializer().getClass().getName());
        }
        logDuration("p_double", start, loopCount, value);

        //
        // boolean
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkReadBoolean(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
        }
        logDuration("boolean", start, loopCount, value);

        start = System.nanoTime();
        if (benchmarkFile.getOrderDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBooleanText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBooleanColumnarText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBooleanColumnarBinary(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBooleanDwrf(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof org.apache.hadoop.hive.ql.io.orc.OrcSerde) {
            value = "NOT_SUPPORTED";
        }
        else {
            throw new UnsupportedOperationException("Unsupported serde " + benchmarkFile.getOrderDeserializer().getClass().getName());
        }
        logDuration("p_boolean", start, loopCount, value);

        //
        // binary
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkReadBinary(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
        }
        logDuration("binary", start, loopCount, value);

        start = System.nanoTime();
        if (benchmarkFile.getOrderDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBinaryText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBinaryColumnarText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBinaryColumnarBinary(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadBinaryDwrf(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof org.apache.hadoop.hive.ql.io.orc.OrcSerde) {
            value = "NOT_SUPPORTED";
        }
        else {
            throw new UnsupportedOperationException("Unsupported serde " + benchmarkFile.getOrderDeserializer().getClass().getName());
        }
        logDuration("p_binary", start, loopCount, value);

        //
        // three columns
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkRead3Columns(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
        }
        logDuration("three", start, loopCount, value);

        start = System.nanoTime();
        if (benchmarkFile.getOrderDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkRead3ColumnsText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkRead3ColumnsColumnarText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkRead3ColumnsColumnarBinary(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkRead3ColumnsDwrf(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof org.apache.hadoop.hive.ql.io.orc.OrcSerde) {
            value = "NOT_SUPPORTED";
        }
        else {
            throw new UnsupportedOperationException("Unsupported serde " + benchmarkFile.getOrderDeserializer().getClass().getName());
        }
        logDuration("p_three", start, loopCount, value);

        //
        // all columns
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkReadAllColumns(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
        }
        logDuration("all", start, loopCount, value);
        start = System.nanoTime();
        if (benchmarkFile.getOrderDeserializer() instanceof LazySimpleSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadAllColumnsText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof ColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadAllColumnsColumnarText(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof LazyBinaryColumnarSerDe) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadAllColumnsColumnarBinary(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof OrcSerde) {
            for (int loops = 0; loops < loopCount; loops++) {
                value = benchmarkReadAllColumnsDwrf(jobConf, benchmarkFile.getOrderFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getOrderDeserializer());
            }
        }
        else if (benchmarkFile.getOrderDeserializer() instanceof org.apache.hadoop.hive.ql.io.orc.OrcSerde) {
            value = "NOT_SUPPORTED";
        }
        else {
            throw new UnsupportedOperationException("Unsupported serde " + benchmarkFile.getOrderDeserializer().getClass().getName());
        }
        logDuration("p_all", start, loopCount, value);
    }

    private static void logDuration(String label, long start, int loopCount, Object value)
    {
        if (NOT_SUPPORTED.equals(value)) {
            return;
        }

        long end = System.nanoTime();
        long nanos = end - start;
        Duration duration = new Duration(1.0 * nanos / loopCount, NANOSECONDS).convertTo(SECONDS);
        System.out.printf("%16s %6s %s\n", label, duration, value);
    }

    private static <K, V extends Writable> List<Object> benchmarkReadAllColumns(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);
        PrimitiveObjectInspector stringFieldInspector = (PrimitiveObjectInspector) stringField.getFieldObjectInspector();

        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int smallintFieldIndex = allStructFieldRefs.indexOf(smallintField);
        PrimitiveObjectInspector smallintFieldInspector = (PrimitiveObjectInspector) smallintField.getFieldObjectInspector();

        StructField intField = rowInspector.getStructFieldRef("t_int");
        int intFieldIndex = allStructFieldRefs.indexOf(intField);
        PrimitiveObjectInspector intFieldInspector = (PrimitiveObjectInspector) intField.getFieldObjectInspector();

        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);
        PrimitiveObjectInspector bigintFieldInspector = (PrimitiveObjectInspector) bigintField.getFieldObjectInspector();

        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int floatFieldIndex = allStructFieldRefs.indexOf(floatField);
        PrimitiveObjectInspector floatFieldInspector = (PrimitiveObjectInspector) floatField.getFieldObjectInspector();

        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);
        PrimitiveObjectInspector doubleFieldInspector = (PrimitiveObjectInspector) doubleField.getFieldObjectInspector();

        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int booleanFieldIndex = allStructFieldRefs.indexOf(booleanField);
        PrimitiveObjectInspector booleanFieldInspector = (PrimitiveObjectInspector) booleanField.getFieldObjectInspector();

        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int binaryFieldIndex = allStructFieldRefs.indexOf(binaryField);
        PrimitiveObjectInspector binaryFieldInspector = (PrimitiveObjectInspector) binaryField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(
                stringFieldIndex,
                smallintFieldIndex,
                intFieldIndex,
                bigintFieldIndex,
                floatFieldIndex,
                doubleFieldIndex,
                booleanFieldIndex,
                binaryFieldIndex));

        long stringLengthSum = 0;
        long smallintSum = 0;
        long intSum = 0;
        long bigintSum = 0;
        double floatSum = 0;
        double doubleSum = 0;
        long booleanSum = 0;
        long binaryLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            smallintSum = 0;
            intSum = 0;
            bigintSum = 0;
            floatSum = 0;
            doubleSum = 0;
            booleanSum = 0;
            binaryLengthSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object stringData = rowInspector.getStructFieldData(rowData, stringField);
                if (stringData != null) {
                    Object stringPrimitive = stringFieldInspector.getPrimitiveJavaObject(stringData);
                    String stringValue = (String) stringPrimitive;
                    stringLengthSum += stringValue.length();
                }

                Object smallintData = rowInspector.getStructFieldData(rowData, smallintField);
                if (smallintData != null) {
                    Object smallintPrimitive = smallintFieldInspector.getPrimitiveJavaObject(smallintData);
                    short shortValue = ((Number) smallintPrimitive).shortValue();
                    smallintSum += shortValue;
                }

                Object intData = rowInspector.getStructFieldData(rowData, intField);
                if (intData != null) {
                    Object intPrimitive = intFieldInspector.getPrimitiveJavaObject(intData);
                    int intValue = ((Number) intPrimitive).intValue();
                    intSum += intValue;
                }

                Object bigintData = rowInspector.getStructFieldData(rowData, bigintField);
                if (bigintData != null) {
                    Object bigintPrimitive = bigintFieldInspector.getPrimitiveJavaObject(bigintData);
                    long bigintValue = ((Number) bigintPrimitive).longValue();
                    bigintSum += bigintValue;
                }

                Object floatData = rowInspector.getStructFieldData(rowData, floatField);
                if (floatData != null) {
                    Object floatPrimitive = floatFieldInspector.getPrimitiveJavaObject(floatData);
                    float floatValue = ((Number) floatPrimitive).floatValue();
                    floatSum += floatValue;
                }

                Object doubleData = rowInspector.getStructFieldData(rowData, doubleField);
                if (doubleData != null) {
                    Object doublePrimitive = doubleFieldInspector.getPrimitiveJavaObject(doubleData);
                    double doubleValue = ((Number) doublePrimitive).doubleValue();
                    doubleSum += doubleValue;
                }

                Object booleanData = rowInspector.getStructFieldData(rowData, booleanField);
                if (booleanData != null) {
                    Object booleanPrimitive = booleanFieldInspector.getPrimitiveJavaObject(booleanData);
                    boolean booleanValue = ((Boolean) booleanPrimitive);
                    booleanSum += booleanValue ? 1 : 2;
                }

                Object binaryData = rowInspector.getStructFieldData(rowData, binaryField);
                if (binaryData != null) {
                    Object binaryPrimitive = binaryFieldInspector.getPrimitiveJavaObject(binaryData);
                    byte[] binaryValue = (byte[]) binaryPrimitive;
                    binaryLengthSum += binaryValue.length;
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(stringLengthSum, smallintSum, intSum, bigintSum, floatSum, doubleSum, booleanSum, binaryLengthSum);
    }

    private static <K, V extends Writable> List<Object> benchmarkReadAllColumnsText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);

        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int smallintFieldIndex = allStructFieldRefs.indexOf(smallintField);

        StructField intField = rowInspector.getStructFieldRef("t_int");
        int intFieldIndex = allStructFieldRefs.indexOf(intField);

        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);

        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int floatFieldIndex = allStructFieldRefs.indexOf(floatField);

        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);

        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int booleanFieldIndex = allStructFieldRefs.indexOf(booleanField);

        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int binaryFieldIndex = allStructFieldRefs.indexOf(binaryField);

        int[] startPosition = new int[13];

        long stringSum = 0;
        long smallintSum = 0;
        long intSum = 0;
        long bigintSum = 0;
        double floatSum = 0;
        double doubleSum = 0;
        long booleanSum = 0;
        long binarySum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            smallintSum = 0;
            intSum = 0;
            bigintSum = 0;
            floatSum = 0;
            doubleSum = 0;
            booleanSum = 0;
            binarySum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BinaryComparable row = (BinaryComparable) value;

                byte[] bytes = row.getBytes();
                parseTextFields(bytes, 0, row.getLength(), startPosition);

                int stringStart = startPosition[stringFieldIndex];
                int stringLength = startPosition[stringFieldIndex + 1] - stringStart - 1;
                if (!isNull(bytes, stringStart, stringLength)) {
                    byte[] stringValue = Arrays.copyOfRange(bytes, stringStart, stringStart + stringLength);
                    stringSum += stringValue.length;
                }

                int smallintStart = startPosition[smallintFieldIndex];
                int smallintLength = startPosition[smallintFieldIndex + 1] - smallintStart - 1;
                if (!isNull(bytes, smallintStart, smallintLength)) {
                    long smallintValue = NumberParser.parseLong(bytes, smallintStart, smallintLength);
                    smallintSum += smallintValue;
                }

                int intStart = startPosition[intFieldIndex];
                int intLength = startPosition[intFieldIndex + 1] - intStart - 1;
                if (!isNull(bytes, intStart, intLength)) {
                    long intValue = NumberParser.parseLong(bytes, intStart, intLength);
                    intSum += intValue;
                }

                int bigintStart = startPosition[bigintFieldIndex];
                int bigintLength = startPosition[bigintFieldIndex + 1] - bigintStart - 1;
                if (!isNull(bytes, bigintStart, bigintLength)) {
                    long bigintValue = NumberParser.parseLong(bytes, bigintStart, bigintLength);
                    bigintSum += bigintValue;
                }

                int floatStart = startPosition[floatFieldIndex];
                int floatLength = startPosition[floatFieldIndex + 1] - floatStart - 1;
                if (!isNull(bytes, floatStart, floatLength)) {
                    float floatValue = parseFloat(bytes, floatStart, floatLength);
                    floatSum += floatValue;
                }

                int doubleStart = startPosition[doubleFieldIndex];
                int doubleLength = startPosition[doubleFieldIndex + 1] - doubleStart - 1;
                if (!isNull(bytes, doubleStart, doubleLength)) {
                    double doubleValue = NumberParser.parseDouble(bytes, doubleStart, doubleLength);
                    doubleSum += doubleValue;
                }

                int booleanStart = startPosition[booleanFieldIndex];
                int booleanLength = startPosition[booleanFieldIndex + 1] - booleanStart - 1;
                if (isTrue(bytes, booleanStart, booleanLength)) {
                    booleanSum += 1;
                }
                else if (isFalse(bytes, booleanStart, booleanLength)) {
                    booleanSum += 2;
                }
                else {
                    // null
                }

                int binaryStart = startPosition[binaryFieldIndex];
                int binaryLength = startPosition[binaryFieldIndex + 1] - binaryStart - 1;
                if (!isNull(bytes, binaryStart, binaryLength)) {
                    byte[] binaryValue = Arrays.copyOfRange(bytes, binaryStart, binaryStart + binaryLength);
                    binaryValue = Base64.decodeBase64(binaryValue);
                    binarySum += binaryValue.length;
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(stringSum, smallintSum, intSum, bigintSum, floatSum, doubleSum, booleanSum, binarySum);
    }

    private static <K, V extends Writable> List<Object> benchmarkReadAllColumnsColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);

        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int smallintFieldIndex = allStructFieldRefs.indexOf(smallintField);

        StructField intField = rowInspector.getStructFieldRef("t_int");
        int intFieldIndex = allStructFieldRefs.indexOf(intField);

        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);

        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int floatFieldIndex = allStructFieldRefs.indexOf(floatField);

        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);

        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int booleanFieldIndex = allStructFieldRefs.indexOf(booleanField);

        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int binaryFieldIndex = allStructFieldRefs.indexOf(binaryField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(
                stringFieldIndex,
                smallintFieldIndex,
                intFieldIndex,
                bigintFieldIndex,
                floatFieldIndex,
                doubleFieldIndex,
                booleanFieldIndex,
                binaryFieldIndex));

        long stringSum = 0;
        long smallintSum = 0;
        long intSum = 0;
        long bigintSum = 0;
        double floatSum = 0;
        double doubleSum = 0;
        long booleanSum = 0;
        long binarySum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            smallintSum = 0;
            intSum = 0;
            bigintSum = 0;
            floatSum = 0;
            doubleSum = 0;
            booleanSum = 0;
            binarySum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;

                BytesRefWritable stringBytesRefWritable = row.unCheckedGet(stringFieldIndex);
                byte[] stringBytes = stringBytesRefWritable.getData();
                int stringStart = stringBytesRefWritable.getStart();
                int stringLength = stringBytesRefWritable.getLength();
                if (!isNull(stringBytes, stringStart, stringLength)) {
                    byte[] stringValue = Arrays.copyOfRange(stringBytes, stringStart, stringStart + stringLength);
                    stringSum += stringValue.length;
                }

                BytesRefWritable smallintBytesRefWritable = row.unCheckedGet(smallintFieldIndex);
                byte[] smallintBytes = smallintBytesRefWritable.getData();
                int smallintStart = smallintBytesRefWritable.getStart();
                int smallintLength = smallintBytesRefWritable.getLength();
                if (!isNull(smallintBytes, smallintStart, smallintLength)) {
                    long smallintValue = NumberParser.parseLong(smallintBytes, smallintStart, smallintLength);
                    smallintSum += smallintValue;
                }

                BytesRefWritable intBytesRefWritable = row.unCheckedGet(intFieldIndex);
                byte[] intBytes = intBytesRefWritable.getData();
                int intStart = intBytesRefWritable.getStart();
                int intLength = intBytesRefWritable.getLength();
                if (!isNull(intBytes, intStart, intLength)) {
                    long intValue = NumberParser.parseLong(intBytes, intStart, intLength);
                    intSum += intValue;
                }

                BytesRefWritable bigintBytesRefWritable = row.unCheckedGet(bigintFieldIndex);
                byte[] bigintBytes = bigintBytesRefWritable.getData();
                int bigintStart = bigintBytesRefWritable.getStart();
                int bigintLength = bigintBytesRefWritable.getLength();
                if (!isNull(bigintBytes, bigintStart, bigintLength)) {
                    long bigintValue = NumberParser.parseLong(bigintBytes, bigintStart, bigintLength);
                    bigintSum += bigintValue;
                }

                BytesRefWritable floatBytesRefWritable = row.unCheckedGet(floatFieldIndex);
                byte[] floatBytes = floatBytesRefWritable.getData();
                int floatStart = floatBytesRefWritable.getStart();
                int floatLength = floatBytesRefWritable.getLength();
                if (!isNull(floatBytes, floatStart, floatLength)) {
                    float floatValue = parseFloat(floatBytes, floatStart, floatLength);
                    floatSum += floatValue;
                }

                BytesRefWritable doubleBytesRefWritable = row.unCheckedGet(doubleFieldIndex);
                byte[] doubleBytes = doubleBytesRefWritable.getData();
                int doubleStart = doubleBytesRefWritable.getStart();
                int doubleLength = doubleBytesRefWritable.getLength();
                if (!isNull(doubleBytes, doubleStart, doubleLength)) {
                    double doubleValue = NumberParser.parseDouble(doubleBytes, doubleStart, doubleLength);
                    doubleSum += doubleValue;
                }

                BytesRefWritable booleanBytesRefWritable = row.unCheckedGet(booleanFieldIndex);
                byte[] booleanBytes = booleanBytesRefWritable.getData();
                int booleanStart = booleanBytesRefWritable.getStart();
                int booleanLength = booleanBytesRefWritable.getLength();
                if (isTrue(booleanBytes, booleanStart, booleanLength)) {
                    booleanSum += 1;
                }
                else if (isFalse(booleanBytes, booleanStart, booleanLength)) {
                    booleanSum += 2;
                }
                else {
                    // null
                }

                BytesRefWritable binaryBytesRefWritable = row.unCheckedGet(binaryFieldIndex);
                byte[] binaryBytes = binaryBytesRefWritable.getData();
                int binaryStart = binaryBytesRefWritable.getStart();
                int binaryLength = binaryBytesRefWritable.getLength();
                if (!isNull(binaryBytes, binaryStart, binaryLength)) {
                    byte[] binaryValue = Arrays.copyOfRange(binaryBytes, binaryStart, binaryStart + binaryLength);
                    binaryValue = Base64.decodeBase64(binaryValue);
                    binarySum += binaryValue.length;
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(stringSum, smallintSum, intSum, bigintSum, floatSum, doubleSum, booleanSum, binarySum);
    }

    private static <K, V extends Writable> List<Object> benchmarkReadAllColumnsColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);

        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int smallintFieldIndex = allStructFieldRefs.indexOf(smallintField);

        StructField intField = rowInspector.getStructFieldRef("t_int");
        int intFieldIndex = allStructFieldRefs.indexOf(intField);

        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);

        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int floatFieldIndex = allStructFieldRefs.indexOf(floatField);

        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);

        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int booleanFieldIndex = allStructFieldRefs.indexOf(booleanField);

        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int binaryFieldIndex = allStructFieldRefs.indexOf(binaryField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(
                stringFieldIndex,
                smallintFieldIndex,
                intFieldIndex,
                bigintFieldIndex,
                floatFieldIndex,
                doubleFieldIndex,
                booleanFieldIndex,
                binaryFieldIndex));

        long stringSum = 0;
        long smallintSum = 0;
        long intSum = 0;
        long bigintSum = 0;
        double floatSum = 0;
        double doubleSum = 0;
        long booleanSum = 0;
        long binarySum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            smallintSum = 0;
            intSum = 0;
            bigintSum = 0;
            floatSum = 0;
            doubleSum = 0;
            booleanSum = 0;
            binarySum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;

                BytesRefWritable stringBytesRefWritable = row.unCheckedGet(stringFieldIndex);
                byte[] stringBytes = stringBytesRefWritable.getData();
                int stringStart = stringBytesRefWritable.getStart();
                int stringLength = stringBytesRefWritable.getLength();
                // todo how are string nulls encoded in binary
                if (!isNull(stringBytes, stringStart, stringLength)) {
                    byte[] stringValue = Arrays.copyOfRange(stringBytes, stringStart, stringStart + stringLength);
                    stringSum += stringValue.length;
                }

                BytesRefWritable smallintBytesRefWritable = row.unCheckedGet(smallintFieldIndex);
                byte[] smallintBytes = smallintBytesRefWritable.getData();
                int smallintStart = smallintBytesRefWritable.getStart();
                int smallintLength = smallintBytesRefWritable.getLength();
                if (smallintLength != 0) {
                    short smallintValue = unsafe.getShort(smallintBytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + smallintStart);
                    smallintValue = Short.reverseBytes(smallintValue);
                    smallintSum += smallintValue;
                }

                BytesRefWritable intBytesRefWritable = row.unCheckedGet(intFieldIndex);
                byte[] intBytes = intBytesRefWritable.getData();
                int intStart = intBytesRefWritable.getStart();
                int intLength = intBytesRefWritable.getLength();
                if (intLength != 0) {
                    int intValue = readVInt(intBytes, intStart, intLength);
                    intSum += intValue;
                }

                BytesRefWritable bigintBytesRefWritable = row.unCheckedGet(bigintFieldIndex);
                byte[] bigintBytes = bigintBytesRefWritable.getData();
                int bigintStart = bigintBytesRefWritable.getStart();
                int bigintLength = bigintBytesRefWritable.getLength();
                if (bigintLength != 0) {
                    long bigintValue = readVBigint(bigintBytes, bigintStart, bigintLength);
                    bigintSum += bigintValue;
                }

                BytesRefWritable floatBytesRefWritable = row.unCheckedGet(floatFieldIndex);
                byte[] floatBytes = floatBytesRefWritable.getData();
                int floatStart = floatBytesRefWritable.getStart();
                int floatLength = floatBytesRefWritable.getLength();
                if (floatLength != 0) {
                    int intBits = unsafe.getInt(floatBytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + floatStart);
                    float floatValue = Float.intBitsToFloat(Integer.reverseBytes(intBits));
                    floatSum += floatValue;
                }

                BytesRefWritable doubleBytesRefWritable = row.unCheckedGet(doubleFieldIndex);
                byte[] doubleBytes = doubleBytesRefWritable.getData();
                int doubleStart = doubleBytesRefWritable.getStart();
                int doubleLength = doubleBytesRefWritable.getLength();
                if (doubleLength != 0) {
                    long longBits = unsafe.getLong(doubleBytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + doubleStart);
                    double doubleValue = Double.longBitsToDouble(Long.reverseBytes(longBits));
                    doubleSum += doubleValue;
                }

                BytesRefWritable booleanBytesRefWritable = row.unCheckedGet(booleanFieldIndex);
                byte[] booleanBytes = booleanBytesRefWritable.getData();
                int booleanStart = booleanBytesRefWritable.getStart();
                int booleanLength = booleanBytesRefWritable.getLength();
                if (booleanLength != 0) {
                    byte val = booleanBytes[booleanStart];
                    booleanSum += val != 0 ? 1 : 2;
                }

                BytesRefWritable binaryBytesRefWritable = row.unCheckedGet(binaryFieldIndex);
                byte[] binaryBytes = binaryBytesRefWritable.getData();
                int binaryStart = binaryBytesRefWritable.getStart();
                int binaryLength = binaryBytesRefWritable.getLength();
                if (!isNull(binaryBytes, binaryStart, binaryLength)) {
                    byte[] binaryValue = Arrays.copyOfRange(binaryBytes, binaryStart, binaryStart + binaryLength);
                    binarySum += binaryValue.length;
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(stringSum, smallintSum, intSum, bigintSum, floatSum, doubleSum, booleanSum, binarySum);
    }

    private static <K, V extends Writable> List<Object> benchmarkReadAllColumnsDwrf(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);

        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int smallintFieldIndex = allStructFieldRefs.indexOf(smallintField);

        StructField intField = rowInspector.getStructFieldRef("t_int");
        int intFieldIndex = allStructFieldRefs.indexOf(intField);

        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);

        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int floatFieldIndex = allStructFieldRefs.indexOf(floatField);

        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);

        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int booleanFieldIndex = allStructFieldRefs.indexOf(booleanField);

        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int binaryFieldIndex = allStructFieldRefs.indexOf(binaryField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(
                stringFieldIndex,
                smallintFieldIndex,
                intFieldIndex,
                bigintFieldIndex,
                floatFieldIndex,
                doubleFieldIndex,
                booleanFieldIndex,
                binaryFieldIndex));

        long stringSum = 0;
        long smallintSum = 0;
        long intSum = 0;
        long bigintSum = 0;
        double floatSum = 0;
        double doubleSum = 0;
        long booleanSum = 0;
        long binarySum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            smallintSum = 0;
            intSum = 0;
            bigintSum = 0;
            floatSum = 0;
            doubleSum = 0;
            booleanSum = 0;
            binarySum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject stringLazyObject = row.getFieldValue(stringFieldIndex);
                Text text = (Text) stringLazyObject.materialize();
                if (text != null) {
                    byte[] stringValue = Arrays.copyOfRange(text.getBytes(), 0, text.getLength());
                    stringSum += stringValue.length;
                }

                OrcLazyObject smallintLazyObject = row.getFieldValue(smallintFieldIndex);
                ShortWritable smallintValue = (ShortWritable) smallintLazyObject.materialize();
                if (smallintValue != null) {
                    smallintSum += smallintValue.get();
                }

                OrcLazyObject intLazyObject = row.getFieldValue(intFieldIndex);
                IntWritable intValue = (IntWritable) intLazyObject.materialize();
                if (intValue != null) {
                    intSum += intValue.get();
                }

                OrcLazyObject bigintLazyObject = row.getFieldValue(bigintFieldIndex);
                LongWritable bigintValue = (LongWritable) bigintLazyObject.materialize();
                if (bigintValue != null) {
                    bigintSum += bigintValue.get();
                }

                OrcLazyObject floatLazyObject = row.getFieldValue(floatFieldIndex);
                FloatWritable floatValue = (FloatWritable) floatLazyObject.materialize();
                if (floatValue != null) {
                    floatSum += floatValue.get();
                }

                OrcLazyObject doubleLazyObject = row.getFieldValue(doubleFieldIndex);
                DoubleWritable doubleValue = (DoubleWritable) doubleLazyObject.materialize();
                if (doubleValue != null) {
                    doubleSum += doubleValue.get();
                }

                OrcLazyObject booleanLazyObject = row.getFieldValue(booleanFieldIndex);
                BooleanWritable booleanValue = (BooleanWritable) booleanLazyObject.materialize();
                if (booleanValue != null) {
                    booleanSum += booleanValue.get() ? 1 : 2;
                }

                OrcLazyObject binaryLazyObject = row.getFieldValue(binaryFieldIndex);
                BytesWritable bytesWritable = (BytesWritable) binaryLazyObject.materialize();
                if (bytesWritable != null) {
                    byte[] binaryValue = Arrays.copyOfRange(bytesWritable.getBytes(), 0, bytesWritable.getLength());
                    binarySum += binaryValue.length;
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(stringSum, smallintSum, intSum, bigintSum, floatSum, doubleSum, booleanSum, binarySum);
    }

    private static <K, V extends Writable> List<Object> benchmarkRead3Columns(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);
        PrimitiveObjectInspector stringFieldInspector = (PrimitiveObjectInspector) stringField.getFieldObjectInspector();

        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);
        PrimitiveObjectInspector doubleFieldInspector = (PrimitiveObjectInspector) doubleField.getFieldObjectInspector();

        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);
        PrimitiveObjectInspector bigintFieldInspector = (PrimitiveObjectInspector) bigintField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(stringFieldIndex, doubleFieldIndex, bigintFieldIndex));

        long stringSum = 0;
        double doubleSum = 0;
        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            doubleSum = 0;
            bigintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object stringData = rowInspector.getStructFieldData(rowData, stringField);
                if (stringData != null) {
                    Object stringPrimitive = stringFieldInspector.getPrimitiveJavaObject(stringData);
                    String stringValue = (String) stringPrimitive;
                    stringSum += stringValue.length();
                }

                Object doubleData = rowInspector.getStructFieldData(rowData, doubleField);
                if (doubleData != null) {
                    Object doublePrimitive = doubleFieldInspector.getPrimitiveJavaObject(doubleData);
                    double doubleValue = ((Number) doublePrimitive).doubleValue();
                    doubleSum += doubleValue;
                }

                Object bigintData = rowInspector.getStructFieldData(rowData, bigintField);
                if (bigintData != null) {
                    Object bigintPrimitive = bigintFieldInspector.getPrimitiveJavaObject(bigintData);
                    long bigintValue = ((Number) bigintPrimitive).longValue();
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(stringSum, doubleSum, bigintSum);
    }

    private static <K, V extends Writable> List<Object> benchmarkRead3ColumnsText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);

        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);

        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);

        int[] startPosition = new int[13];

        long stringSum = 0;
        double doubleSum = 0;
        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            doubleSum = 0;
            bigintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BinaryComparable row = (BinaryComparable) value;

                byte[] bytes = row.getBytes();
                parseTextFields(bytes, 0, row.getLength(), startPosition);

                int stringStart = startPosition[stringFieldIndex];
                int stringLength = startPosition[stringFieldIndex + 1] - stringStart - 1;
                if (!isNull(bytes, stringStart, stringLength)) {
                    byte[] stringValue = Arrays.copyOfRange(bytes, stringStart, stringStart + stringLength);
                    stringSum += stringValue.length;
                }

                int doubleStart = startPosition[doubleFieldIndex];
                int doubleLength = startPosition[doubleFieldIndex + 1] - doubleStart - 1;
                if (!isNull(bytes, doubleStart, doubleLength)) {
                    double doubleValue = NumberParser.parseDouble(bytes, doubleStart, doubleLength);
                    doubleSum += doubleValue;
                }

                int bigintStart = startPosition[bigintFieldIndex];
                int bigintLength = startPosition[bigintFieldIndex + 1] - bigintStart - 1;
                if (!isNull(bytes, bigintStart, bigintLength)) {
                    long bigintValue = NumberParser.parseLong(bytes, bigintStart, bigintLength);
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(stringSum, doubleSum, bigintSum);
    }

    private static <K, V extends Writable> List<Object> benchmarkRead3ColumnsColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);
        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);
        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(stringFieldIndex, doubleFieldIndex, bigintFieldIndex));

        long stringSum = 0;
        double doubleSum = 0;
        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            doubleSum = 0;
            bigintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;

                BytesRefWritable stringBytesRefWritable = row.unCheckedGet(stringFieldIndex);
                byte[] stringBytes = stringBytesRefWritable.getData();
                int stringStart = stringBytesRefWritable.getStart();
                int stringLength = stringBytesRefWritable.getLength();
                if (!isNull(stringBytes, stringStart, stringLength)) {
                    byte[] stringValue = Arrays.copyOfRange(stringBytes, stringStart, stringStart + stringLength);
                    stringSum += stringValue.length;
                }

                BytesRefWritable doubleBytesRefWritable = row.unCheckedGet(doubleFieldIndex);
                byte[] doubleBytes = doubleBytesRefWritable.getData();
                int doubleStart = doubleBytesRefWritable.getStart();
                int doubleLength = doubleBytesRefWritable.getLength();
                if (!isNull(doubleBytes, doubleStart, doubleLength)) {
                    double doubleValue = NumberParser.parseDouble(doubleBytes, doubleStart, doubleLength);
                    doubleSum += doubleValue;
                }

                BytesRefWritable bigintBytesRefWritable = row.unCheckedGet(bigintFieldIndex);
                byte[] bigintBytes = bigintBytesRefWritable.getData();
                int bigintStart = bigintBytesRefWritable.getStart();
                int bigintLength = bigintBytesRefWritable.getLength();
                if (!isNull(bigintBytes, bigintStart, bigintLength)) {
                    long bigintValue = NumberParser.parseLong(bigintBytes, bigintStart, bigintLength);
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(stringSum, doubleSum, bigintSum);
    }

    private static <K, V extends Writable> List<Object> benchmarkRead3ColumnsColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);
        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);
        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(stringFieldIndex, doubleFieldIndex, bigintFieldIndex));

        long stringSum = 0;
        double doubleSum = 0;
        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            doubleSum = 0;
            bigintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;

                BytesRefWritable stringBytesRefWritable = row.unCheckedGet(stringFieldIndex);
                byte[] stringBytes = stringBytesRefWritable.getData();
                int stringStart = stringBytesRefWritable.getStart();
                int stringLength = stringBytesRefWritable.getLength();
                // todo how are string nulls encoded in binary
                if (!isNull(stringBytes, stringStart, stringLength)) {
                    byte[] stringValue = Arrays.copyOfRange(stringBytes, stringStart, stringStart + stringLength);
                    stringSum += stringValue.length;
                }

                BytesRefWritable doubleBytesRefWritable = row.unCheckedGet(doubleFieldIndex);
                byte[] doubleBytes = doubleBytesRefWritable.getData();
                int doubleStart = doubleBytesRefWritable.getStart();
                int doubleLength = doubleBytesRefWritable.getLength();
                if (doubleLength != 0) {
                    long longBits = unsafe.getLong(doubleBytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + doubleStart);
                    double doubleValue = Double.longBitsToDouble(Long.reverseBytes(longBits));
                    doubleSum += doubleValue;
                }

                BytesRefWritable bigintBytesRefWritable = row.unCheckedGet(bigintFieldIndex);
                byte[] bigintBytes = bigintBytesRefWritable.getData();
                int bigintStart = bigintBytesRefWritable.getStart();
                int bigintLength = bigintBytesRefWritable.getLength();
                if (bigintLength != 0) {
                    long bigintValue = readVBigint(bigintBytes, bigintStart, bigintLength);
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(stringSum, doubleSum, bigintSum);
    }

    private static <K, V extends Writable> List<Object> benchmarkRead3ColumnsDwrf(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField stringField = rowInspector.getStructFieldRef("t_string");
        int stringFieldIndex = allStructFieldRefs.indexOf(stringField);
        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int doubleFieldIndex = allStructFieldRefs.indexOf(doubleField);
        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        int bigintFieldIndex = allStructFieldRefs.indexOf(bigintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(stringFieldIndex, doubleFieldIndex, bigintFieldIndex));

        long stringSum = 0;
        double doubleSum = 0;
        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            doubleSum = 0;
            bigintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject stringLazyObject = row.getFieldValue(stringFieldIndex);
                Text text = (Text) stringLazyObject.materialize();
                if (text != null) {
                    byte[] stringValue = Arrays.copyOfRange(text.getBytes(), 0, text.getLength());
                    stringSum += stringValue.length;
                }

                OrcLazyObject bigintLazyObject = row.getFieldValue(bigintFieldIndex);
                LongWritable bigintValue = (LongWritable) bigintLazyObject.materialize();
                if (bigintValue != null) {
                    bigintSum += bigintValue.get();
                }

                OrcLazyObject doubleLazyObject = row.getFieldValue(doubleFieldIndex);
                DoubleWritable doubleValue = (DoubleWritable) doubleLazyObject.materialize();
                if (doubleValue != null) {
                    doubleSum += doubleValue.get();
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(stringSum, doubleSum, bigintSum);
    }

    private static <K, V extends Writable> long customerKeyText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("custkey");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        int[] startPosition = new int[13];

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BinaryComparable row = (BinaryComparable) value;

                byte[] bytes = row.getBytes();
                parseTextFields(bytes, 0, row.getLength(), startPosition);

                int start = startPosition[fieldIndex];
                int length = startPosition[fieldIndex + 1] - start - 1;

                if (!isNull(bytes, start, length)) {
                    long bigintValue = NumberParser.parseLong(bytes, start, length);
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }

    private static <K, V extends Writable> long customerKeyColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("custkey");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (!isNull(bytes, start, length)) {
                    long bigintValue = NumberParser.parseLong(bytes, start, length);
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }

    private static <K, V extends Writable> long customerKeyColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("custkey");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (length != 0) {
                    long bigintValue = readVBigint(bytes, start, length);
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }

    private static <K, V extends Writable> long customerKeyDwrf(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("custkey");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;
                OrcLazyObject orcLazyObject = row.getFieldValue(fieldIndex);
                LongWritable bigintValue = (LongWritable) orcLazyObject.materialize();
                if (bigintValue != null) {
                    bigintSum += bigintValue.get();
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }

    private static <K, V extends Writable> long benchmarkReadComment(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("comment");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);
        PrimitiveObjectInspector stringFieldInspector = (PrimitiveObjectInspector) stringField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object stringData = rowInspector.getStructFieldData(rowData, stringField);
                if (stringData != null) {
                    Object stringPrimitive = stringFieldInspector.getPrimitiveJavaObject(stringData);
                    String stringValue = (String) stringPrimitive;
                    stringLengthSum += stringValue.length();
                }

            }
            recordReader.close();
        }
        return stringLengthSum;
    }

    private static <K, V extends Writable> long benchmarkReadCommentText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField stringField = rowInspector.getStructFieldRef("comment");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        int[] startPosition = new int[13];

        long stringSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BinaryComparable row = (BinaryComparable) value;

                byte[] bytes = row.getBytes();
                parseTextFields(bytes, 0, row.getLength(), startPosition);

                int start = startPosition[fieldIndex];
                int length = startPosition[fieldIndex + 1] - start - 1;

                if (!isNull(bytes, start, length)) {
                    byte[] stringValue = Arrays.copyOfRange(bytes, start, start + length);
                    stringSum += stringValue.length;
                }
            }
            recordReader.close();
        }
        return stringSum;
    }

    private static <K, V extends Writable> long benchmarkReadCommentColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField stringField = rowInspector.getStructFieldRef("comment");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (!isNull(bytes, start, length)) {
                    byte[] stringValue = Arrays.copyOfRange(bytes, start, start + length);
                    stringSum += stringValue.length;
                }
            }
            recordReader.close();
        }
        return stringSum;
    }

    private static <K, V extends Writable> long benchmarkReadCommentColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField stringField = rowInspector.getStructFieldRef("comment");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (!isNull(bytes, start, length)) {
                    byte[] stringValue = Arrays.copyOfRange(bytes, start, start + length);
                    stringSum += stringValue.length;
                }
            }
            recordReader.close();
        }
        return stringSum;
    }

    private static <K, V extends Writable> long benchmarkReadCommentDwrf(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField stringField = rowInspector.getStructFieldRef("comment");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;
                OrcLazyObject orcLazyObject = row.getFieldValue(fieldIndex);
                Text text = (Text) orcLazyObject.materialize();
                if (text != null) {
                    byte[] stringValue = Arrays.copyOfRange(text.getBytes(), 0, text.getLength());
                    stringSum += stringValue.length;
                }
            }
            recordReader.close();
        }
        return stringSum;
    }

    private static <K, V extends Writable> long benchmarkReadSmallint(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int fieldIndex = allStructFieldRefs.indexOf(smallintField);
        PrimitiveObjectInspector smallintFieldInspector = (PrimitiveObjectInspector) smallintField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long smallintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            smallintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object smallintData = rowInspector.getStructFieldData(rowData, smallintField);
                if (smallintData != null) {
                    Object smallintPrimitive = smallintFieldInspector.getPrimitiveJavaObject(smallintData);
                    short shortValue = ((Number) smallintPrimitive).shortValue();
                    smallintSum += shortValue;
                }
            }
            recordReader.close();
        }
        return smallintSum;
    }

    private static <K, V extends Writable> long benchmarkReadSmallintText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int fieldIndex = allStructFieldRefs.indexOf(smallintField);

        int[] startPosition = new int[13];

        long smallintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            smallintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BinaryComparable row = (BinaryComparable) value;

                byte[] bytes = row.getBytes();
                parseTextFields(bytes, 0, row.getLength(), startPosition);

                int start = startPosition[fieldIndex];
                int length = startPosition[fieldIndex + 1] - start - 1;

                if (!isNull(bytes, start, length)) {
                    long smallintValue = NumberParser.parseLong(bytes, start, length);
                    smallintSum += smallintValue;
                }
            }
            recordReader.close();
        }
        return smallintSum;
    }

    private static <K, V extends Writable> long benchmarkReadSmallintColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int fieldIndex = allStructFieldRefs.indexOf(smallintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long smallintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            smallintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (!isNull(bytes, start, length)) {
                    long smallintValue = NumberParser.parseLong(bytes, start, length);
                    smallintSum += smallintValue;
                }
            }
            recordReader.close();
        }
        return smallintSum;
    }

    private static <K, V extends Writable> long benchmarkReadSmallintColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int fieldIndex = allStructFieldRefs.indexOf(smallintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long smallintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            smallintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (length != 0) {
                    short smallintValue = unsafe.getShort(bytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + start);
                    smallintValue = Short.reverseBytes(smallintValue);
                    smallintSum += smallintValue;
                }
            }
            recordReader.close();
        }
        return smallintSum;
    }

    private static <K, V extends Writable> long benchmarkReadSmallintDwrf(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField smallintField = rowInspector.getStructFieldRef("t_smallint");
        int fieldIndex = allStructFieldRefs.indexOf(smallintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long smallintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            smallintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;
                OrcLazyObject orcLazyObject = row.getFieldValue(fieldIndex);
                ShortWritable smallintValue = (ShortWritable) orcLazyObject.materialize();
                if (smallintValue != null) {
                    smallintSum += smallintValue.get();
                }
            }
            recordReader.close();
        }
        return smallintSum;
    }

    private static <K, V extends Writable> long benchmarkReadInt(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField intField = rowInspector.getStructFieldRef("t_int");
        int fieldIndex = allStructFieldRefs.indexOf(intField);
        PrimitiveObjectInspector intFieldInspector = (PrimitiveObjectInspector) intField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long intSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            intSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object intData = rowInspector.getStructFieldData(rowData, intField);
                if (intData != null) {
                    Object intPrimitive = intFieldInspector.getPrimitiveJavaObject(intData);
                    int intValue = ((Number) intPrimitive).intValue();
                    intSum += intValue;
                }
            }
            recordReader.close();
        }
        return intSum;
    }

    private static <K, V extends Writable> long benchmarkReadIntText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField intField = rowInspector.getStructFieldRef("t_int");
        int fieldIndex = allStructFieldRefs.indexOf(intField);

        int[] startPosition = new int[13];

        long intSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            intSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BinaryComparable row = (BinaryComparable) value;

                byte[] bytes = row.getBytes();
                parseTextFields(bytes, 0, row.getLength(), startPosition);

                int start = startPosition[fieldIndex];
                int length = startPosition[fieldIndex + 1] - start - 1;

                if (!isNull(bytes, start, length)) {
                    long intValue = NumberParser.parseLong(bytes, start, length);
                    intSum += intValue;
                }
            }
            recordReader.close();
        }
        return intSum;
    }

    private static <K, V extends Writable> long benchmarkReadIntColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField intField = rowInspector.getStructFieldRef("t_int");
        int fieldIndex = allStructFieldRefs.indexOf(intField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long intSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            intSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (!isNull(bytes, start, length)) {
                    long intValue = NumberParser.parseLong(bytes, start, length);
                    intSum += intValue;
                }
            }
            recordReader.close();
        }
        return intSum;
    }

    private static <K, V extends Writable> long benchmarkReadIntColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField intField = rowInspector.getStructFieldRef("t_int");
        int fieldIndex = allStructFieldRefs.indexOf(intField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long intSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            intSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (length != 0) {
                    int intValue = readVInt(bytes, start, length);
                    intSum += intValue;
                }
            }
            recordReader.close();
        }
        return intSum;
    }

    public static int readVInt(byte[] bytes, int offset, int length)
    {
        if (length == 1) {
            return bytes[offset];
        }

        int i = 0;
        for (int idx = 0; idx < length - 1; idx++) {
            byte b = bytes[offset + 1 + idx];
            i = i << 8;
            i = i | (b & 0xFF);
        }
        return WritableUtils.isNegativeVInt(bytes[offset]) ? ~i : i;
    }

    private static <K, V extends Writable> long benchmarkReadIntDwrf(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField intField = rowInspector.getStructFieldRef("t_int");
        int fieldIndex = allStructFieldRefs.indexOf(intField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long intSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            intSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;
                OrcLazyObject orcLazyObject = row.getFieldValue(fieldIndex);
                IntWritable intValue = (IntWritable) orcLazyObject.materialize();
                if (intValue != null) {
                    intSum += intValue.get();
                }
            }
            recordReader.close();
        }
        return intSum;
    }

    private static <K, V extends Writable> long benchmarkReadBigint(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("custkey");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);
        PrimitiveObjectInspector bigintFieldInspector = (PrimitiveObjectInspector) bigintField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object bigintData = rowInspector.getStructFieldData(rowData, bigintField);
                if (bigintData != null) {
                    Object bigintPrimitive = bigintFieldInspector.getPrimitiveJavaObject(bigintData);
                    long bigintValue = ((Number) bigintPrimitive).longValue();
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }

    private static <K, V extends Writable> long benchmarkReadBigintText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("custkey");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        int[] startPosition = new int[13];

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BinaryComparable row = (BinaryComparable) value;

                byte[] bytes = row.getBytes();
                parseTextFields(bytes, 0, row.getLength(), startPosition);

                int start = startPosition[fieldIndex];
                int length = startPosition[fieldIndex + 1] - start - 1;

                if (!isNull(bytes, start, length)) {
                    long bigintValue = NumberParser.parseLong(bytes, start, length);
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }

    private static <K, V extends Writable> long benchmarkReadBigintColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("custkey");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (!isNull(bytes, start, length)) {
                    long bigintValue = NumberParser.parseLong(bytes, start, length);
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }

    private static <K, V extends Writable> long benchmarkReadBigintColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("custkey");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (length != 0) {
                    long bigintValue = readVBigint(bytes, start, length);
                    bigintSum += bigintValue;
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }
    public static long readVBigint(byte[] bytes, int offset, int length)
    {
        if (length == 1) {
            return bytes[offset];
        }

        long i = 0;
        for (int idx = 0; idx < length - 1; idx++) {
            byte b = bytes[offset + 1 + idx];
            i = i << 8;
            i = i | (b & 0xFF);
        }
        return WritableUtils.isNegativeVInt(bytes[offset]) ? ~i : i;
    }

    private static <K, V extends Writable> long benchmarkReadBigintDwrf(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("custkey");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;
                OrcLazyObject orcLazyObject = row.getFieldValue(fieldIndex);
                LongWritable bigintValue = (LongWritable) orcLazyObject.materialize();
                if (bigintValue != null) {
                    bigintSum += bigintValue.get();
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }

    private static <K, V extends Writable> double benchmarkReadFloat(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int fieldIndex = allStructFieldRefs.indexOf(floatField);
        PrimitiveObjectInspector floatFieldInspector = (PrimitiveObjectInspector) floatField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double floatSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            floatSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object floatData = rowInspector.getStructFieldData(rowData, floatField);
                if (floatData != null) {
                    Object floatPrimitive = floatFieldInspector.getPrimitiveJavaObject(floatData);
                    float floatValue = ((Number) floatPrimitive).floatValue();
                    floatSum += floatValue;
                }
            }
            recordReader.close();
        }
        return floatSum;
    }

    private static <K, V extends Writable> double benchmarkReadFloatText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int fieldIndex = allStructFieldRefs.indexOf(floatField);

        int[] startPosition = new int[13];

        double floatSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            floatSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BinaryComparable row = (BinaryComparable) value;

                byte[] bytes = row.getBytes();
                parseTextFields(bytes, 0, row.getLength(), startPosition);

                int start = startPosition[fieldIndex];
                int length = startPosition[fieldIndex + 1] - start - 1;

                if (!isNull(bytes, start, length)) {
                    float floatValue = parseFloat(bytes, start, length);
                    floatSum += floatValue;
                }
            }
            recordReader.close();
        }
        return floatSum;
    }

    private static <K, V extends Writable> double benchmarkReadFloatColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int fieldIndex = allStructFieldRefs.indexOf(floatField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double floatSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            floatSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (!isNull(bytes, start, length)) {
                    float floatValue = parseFloat(bytes, start, length);
                    floatSum += floatValue;
                }
            }
            recordReader.close();
        }
        return floatSum;
    }

    public static float parseFloat(byte[] bytes, int start, int length)
    {
        char[] chars = new char[length];
        for (int pos = 0; pos < length; pos++) {
            chars[pos] = (char) bytes[start + pos];
        }
        String string = new String(chars);
        return Float.parseFloat(string);
    }

    private static <K, V extends Writable> double benchmarkReadFloatColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int fieldIndex = allStructFieldRefs.indexOf(floatField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double floatSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            floatSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (length != 0) {
                    int intBits = unsafe.getInt(bytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + start);
                    float floatValue = Float.intBitsToFloat(Integer.reverseBytes(intBits));
                    floatSum += floatValue;
                }
            }
            recordReader.close();
        }
        return floatSum;
    }

    private static <K, V extends Writable> double benchmarkReadFloatDwrf(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField floatField = rowInspector.getStructFieldRef("t_float");
        int fieldIndex = allStructFieldRefs.indexOf(floatField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double floatSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            floatSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;
                OrcLazyObject orcLazyObject = row.getFieldValue(fieldIndex);
                FloatWritable floatValue = (FloatWritable) orcLazyObject.materialize();
                if (floatValue != null) {
                    floatSum += floatValue.get();
                }
            }
            recordReader.close();
        }
        return floatSum;
    }

    private static <K, V extends Writable> double benchmarkReadDouble(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int fieldIndex = allStructFieldRefs.indexOf(doubleField);
        PrimitiveObjectInspector doubleFieldInspector = (PrimitiveObjectInspector) doubleField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double doubleSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            doubleSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object doubleData = rowInspector.getStructFieldData(rowData, doubleField);
                if (doubleData != null) {
                    Object doublePrimitive = doubleFieldInspector.getPrimitiveJavaObject(doubleData);
                    double doubleValue = ((Number) doublePrimitive).doubleValue();
                    doubleSum += doubleValue;
                }
            }
            recordReader.close();
        }
        return doubleSum;
    }

    private static <K, V extends Writable> double benchmarkReadDoubleText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int fieldIndex = allStructFieldRefs.indexOf(doubleField);

        int[] startPosition = new int[13];

        double doubleSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            doubleSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BinaryComparable row = (BinaryComparable) value;

                byte[] bytes = row.getBytes();
                parseTextFields(bytes, 0, row.getLength(), startPosition);

                int start = startPosition[fieldIndex];
                int length = startPosition[fieldIndex + 1] - start - 1;

                if (!isNull(bytes, start, length)) {
                    double doubleValue = NumberParser.parseDouble(bytes, start, length);
                    doubleSum += doubleValue;
                }
            }
            recordReader.close();
        }
        return doubleSum;
    }

    private static <K, V extends Writable> double benchmarkReadDoubleColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int fieldIndex = allStructFieldRefs.indexOf(doubleField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double doubleSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            doubleSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (!isNull(bytes, start, length)) {
                    double doubleValue = NumberParser.parseDouble(bytes, start, length);
                    doubleSum += doubleValue;
                }
            }
            recordReader.close();
        }
        return doubleSum;
    }

    private static <K, V extends Writable> double benchmarkReadDoubleColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int fieldIndex = allStructFieldRefs.indexOf(doubleField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double doubleSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            doubleSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (length != 0) {
                    long longBits = unsafe.getLong(bytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + start);
                    double doubleValue = Double.longBitsToDouble(Long.reverseBytes(longBits));
                    doubleSum += doubleValue;
                }
            }
            recordReader.close();
        }
        return doubleSum;
    }

    private static <K, V extends Writable> double benchmarkReadDoubleDwrf(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        int fieldIndex = allStructFieldRefs.indexOf(doubleField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double doubleSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            doubleSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;
                OrcLazyObject orcLazyObject = row.getFieldValue(fieldIndex);
                DoubleWritable doubleValue = (DoubleWritable) orcLazyObject.materialize();
                if (doubleValue != null) {
                    doubleSum += doubleValue.get();
                }
            }
            recordReader.close();
        }
        return doubleSum;
    }

    private static <K, V extends Writable> long benchmarkReadBoolean(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int fieldIndex = allStructFieldRefs.indexOf(booleanField);
        PrimitiveObjectInspector booleanFieldInspector = (PrimitiveObjectInspector) booleanField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long booleanSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            booleanSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object booleanData = rowInspector.getStructFieldData(rowData, booleanField);
                if (booleanData != null) {
                    Object booleanPrimitive = booleanFieldInspector.getPrimitiveJavaObject(booleanData);
                    boolean booleanValue = ((Boolean) booleanPrimitive);
                    booleanSum += booleanValue ? 1 : 2;
                }
            }
            recordReader.close();
        }
        return booleanSum;
    }

    private static <K, V extends Writable> long benchmarkReadBooleanText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int fieldIndex = allStructFieldRefs.indexOf(booleanField);

        int[] startPosition = new int[13];

        long booleanSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            booleanSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BinaryComparable row = (BinaryComparable) value;

                byte[] bytes = row.getBytes();
                parseTextFields(bytes, 0, row.getLength(), startPosition);

                int start = startPosition[fieldIndex];
                int length = startPosition[fieldIndex + 1] - start - 1;

                if (isTrue(bytes, start, length)) {
                    booleanSum += 1;
                }
                else if (isFalse(bytes, start, length)) {
                    booleanSum += 2;
                }
                else {
                    // null
                }
            }
            recordReader.close();
        }
        return booleanSum;
    }

    private static <K, V extends Writable> long benchmarkReadBooleanColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int fieldIndex = allStructFieldRefs.indexOf(booleanField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long booleanSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            booleanSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (isTrue(bytes, start, length)) {
                    booleanSum += 1;
                }
                else if (isFalse(bytes, start, length)) {
                    booleanSum += 2;
                }
                else {
                    // null
                }
            }
            recordReader.close();
        }
        return booleanSum;
    }

    private static <K, V extends Writable> long benchmarkReadBooleanColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int fieldIndex = allStructFieldRefs.indexOf(booleanField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long booleanSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            booleanSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (length != 0) {
                    byte val = bytes[start];
                    booleanSum += val != 0 ? 1 : 2;
                }
            }
            recordReader.close();
        }
        return booleanSum;
    }

    private static <K, V extends Writable> long benchmarkReadBooleanDwrf(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField booleanField = rowInspector.getStructFieldRef("t_boolean");
        int fieldIndex = allStructFieldRefs.indexOf(booleanField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long booleanSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            booleanSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;
                OrcLazyObject orcLazyObject = row.getFieldValue(fieldIndex);
                BooleanWritable booleanValue = (BooleanWritable) orcLazyObject.materialize();
                if (booleanValue != null) {
                    booleanSum += booleanValue.get() ? 1 : 2;
                }
            }
            recordReader.close();
        }
        return booleanSum;
    }

    private static <K, V extends Writable> long benchmarkReadBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int fieldIndex = allStructFieldRefs.indexOf(binaryField);
        PrimitiveObjectInspector binaryFieldInspector = (PrimitiveObjectInspector) binaryField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long binaryLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            binaryLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object binaryData = rowInspector.getStructFieldData(rowData, binaryField);
                if (binaryData != null) {
                    Object binaryPrimitive = binaryFieldInspector.getPrimitiveJavaObject(binaryData);
                    byte[] binaryValue = (byte[]) binaryPrimitive;
                    binaryLengthSum += binaryValue.length;
                }
            }
            recordReader.close();
        }
        return binaryLengthSum;
    }

    private static <K, V extends Writable> long benchmarkReadBinaryText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int fieldIndex = allStructFieldRefs.indexOf(binaryField);

        int[] startPosition = new int[13];

        long binarySum = 0;
        for (int i = 0; i < LOOPS; i++) {
            binarySum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BinaryComparable row = (BinaryComparable) value;

                byte[] bytes = row.getBytes();
                parseTextFields(bytes, 0, row.getLength(), startPosition);

                int start = startPosition[fieldIndex];
                int length = startPosition[fieldIndex + 1] - start - 1;

                if (!isNull(bytes, start, length)) {
                    byte[] binaryValue = Arrays.copyOfRange(bytes, start, start + length);
                    binaryValue = Base64.decodeBase64(binaryValue);
                    binarySum += binaryValue.length;
                }
            }
            recordReader.close();
        }
        return binarySum;
    }

    private static <K, V extends Writable> long benchmarkReadBinaryColumnarText(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int fieldIndex = allStructFieldRefs.indexOf(binaryField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long binarySum = 0;
        for (int i = 0; i < LOOPS; i++) {
            binarySum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (!isNull(bytes, start, length)) {
                    byte[] binaryValue = Arrays.copyOfRange(bytes, start, start + length);
                    binaryValue = Base64.decodeBase64(binaryValue);
                    binarySum += binaryValue.length;
                }
            }
            recordReader.close();
        }
        return binarySum;
    }

    private static <K, V extends Writable> long benchmarkReadBinaryColumnarBinary(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int fieldIndex = allStructFieldRefs.indexOf(binaryField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long binarySum = 0;
        for (int i = 0; i < LOOPS; i++) {
            binarySum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                BytesRefArrayWritable row = (BytesRefArrayWritable) value;
                BytesRefWritable bytesRefWritable = row.unCheckedGet(fieldIndex);
                byte[] bytes = bytesRefWritable.getData();
                int start = bytesRefWritable.getStart();
                int length = bytesRefWritable.getLength();

                if (!isNull(bytes, start, length)) {
                    byte[] binaryValue = Arrays.copyOfRange(bytes, start, start + length);
                    binarySum += binaryValue.length;
                }
            }
            recordReader.close();
        }
        return binarySum;
    }

    private static <K, V extends Writable> long benchmarkReadBinaryDwrf(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField binaryField = rowInspector.getStructFieldRef("t_binary");
        int fieldIndex = allStructFieldRefs.indexOf(binaryField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long binarySum = 0;
        for (int i = 0; i < LOOPS; i++) {
            binarySum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;
                OrcLazyObject orcLazyObject = row.getFieldValue(fieldIndex);
                BytesWritable bytesWritable = (BytesWritable) orcLazyObject.materialize();
                if (bytesWritable != null) {
                    byte[] binaryValue = Arrays.copyOfRange(bytesWritable.getBytes(), 0, bytesWritable.getLength());
                    binarySum += binaryValue.length;
                }
            }
            recordReader.close();
        }
        return binarySum;
    }

    private static void parseTextFields(byte[] bytes, int start, int length, int[] startPosition)
    {
        byte separator = 1;
//        byte separator = oi.getSeparator();
//        boolean lastColumnTakesRest = oi.getLastColumnTakesRest();
//        boolean isEscaped = oi.isEscaped();
//        byte escapeChar = oi.getEscapeChar();

//        if (fields == null) {
//            List<? extends StructField> fieldRefs = ((StructObjectInspector) oi)
//                    .getAllStructFieldRefs();
//            fields = new LazyObject[fieldRefs.size()];
//            for (int i = 0; i < fields.length; i++) {
//                fields[i] = LazyFactory.createLazyObject(fieldRefs.get(i)
//                        .getFieldObjectInspector());
//            }
//            fieldInited = new boolean[fields.length];
//            // Extra element to make sure we have the same formula to compute the
//            // length of each element of the array.
//            startPosition = new int[fields.length + 1];
//        }

        final int structByteEnd = start + length;
        int fieldId = 0;
        int fieldByteBegin = start;
        int fieldByteEnd = start;

        // Go through all bytes in the byte[]
        while (fieldByteEnd <= structByteEnd) {
            if (fieldByteEnd == structByteEnd || bytes[fieldByteEnd] == separator) {
                // Reached the end of a field?
//                if (lastColumnTakesRest && fieldId == fields.length - 1) {
//                    fieldByteEnd = structByteEnd;
//                }
                startPosition[fieldId] = fieldByteBegin;
                fieldId++;

                if (fieldId == startPosition.length - 1 || fieldByteEnd == structByteEnd) {
                    // All fields have been parsed, or bytes have been parsed.
                    // We need to set the startPosition of fields.length to ensure we
                    // can use the same formula to calculate the length of each field.
                    // For missing fields, their starting positions will all be the same,
                    // which will make their lengths to be -1 and uncheckedGetField will
                    // return these fields as NULLs.
                    for (int i = fieldId; i < startPosition.length; i++) {
                        startPosition[i] = fieldByteEnd + 1;
                    }
                    break;
                }

                fieldByteBegin = fieldByteEnd + 1;
                fieldByteEnd++;
            }
            else {
//                if (isEscaped && bytes[fieldByteEnd] == escapeChar
//                        && fieldByteEnd + 1 < structByteEnd) {
//                    // ignore the char after escape_char
//                    fieldByteEnd += 2;
//                }
//                else {
                fieldByteEnd++;
//                }
            }
        }

//        // Extra bytes at the end?
//        if (!extraFieldWarned && fieldByteEnd < structByteEnd) {
//            extraFieldWarned = true;
//            LOG.warn("Extra bytes detected at the end of the row! Ignoring similar "
//                    + "problems.");
//        }
//
//        // Missing fields?
//        if (!missingFieldWarned && fieldId < fields.length) {
//            missingFieldWarned = true;
//            LOG.info("Missing fields! Expected " + fields.length + " fields but "
//                    + "only got " + fieldId + "! Ignoring similar problems.");
//        }
//
//        Arrays.fill(fieldInited, false);
//        parsed = true;
    }

    private static boolean isNull(byte[] bytes, int start, int length)
    {
        return length == 2 && bytes[start] == '\\' && bytes[start + 1] == 'N';
    }

    private static class BenchmarkFile
    {
        private final String name;
        private final InputFormat<?, ? extends Writable> inputFormat;
        private final Deserializer orderDeserializer;
        private final FileSplit orderFileSplit;
        private final Deserializer lineItemDeserializer;
        private final FileSplit lineItemFileSplit;
        private final List<BenchmarkLineItem> lineItemBenchmarks;

        public BenchmarkFile(
                String name,
                String fileExtension,
                InputFormat<?, ? extends Writable> inputFormat,
                HiveOutputFormat<?, ?> outputFormat,
                SerDe orderSerDe,
                SerDe lineitemSerDe,
                String compressionCodec,
                boolean verifyChecksum,
                List<? extends BenchmarkLineItem> lineItemBenchmarks)
                throws Exception
        {
            this.name = name;
            this.inputFormat = inputFormat;
            this.orderDeserializer = orderSerDe;
            this.lineItemDeserializer = lineitemSerDe;
            this.lineItemBenchmarks = ImmutableList.copyOf(lineItemBenchmarks);

            File orderFile = new File(DATA_DIR, "order." + fileExtension);
            orderSerDe.initialize(new Configuration(), createTableProperties(ORDER_COLUMNS));
            if (!orderFile.exists()) {
                writeOrders(orderFile, outputFormat, orderSerDe, compressionCodec);
            }
            Path orderPath = new Path(orderFile.toURI());
            orderPath.getFileSystem(new Configuration()).setVerifyChecksum(verifyChecksum);
            this.orderFileSplit = new FileSplit(orderPath, 0, orderFile.length(), new String[0]);

            File lineItemFile = new File(DATA_DIR, "line_item." + fileExtension);
            lineitemSerDe.initialize(new Configuration(), createTableProperties(LINE_ITEM_COLUMNS));
            if (!lineItemFile.exists()) {
                writeLineItems(lineItemFile, outputFormat, lineitemSerDe, compressionCodec);
            }
            Path lineitemPath = new Path(lineItemFile.toURI());
            lineitemPath.getFileSystem(new Configuration()).setVerifyChecksum(verifyChecksum);
            this.lineItemFileSplit = new FileSplit(lineitemPath, 0, lineItemFile.length(), new String[0]);
        }

        private String getName()
        {
            return name;
        }

        private InputFormat<?, ? extends Writable> getInputFormat()
        {
            return inputFormat;
        }

        private Deserializer getOrderDeserializer()
        {
            return orderDeserializer;
        }

        private FileSplit getOrderFileSplit()
        {
            return orderFileSplit;
        }

        public Deserializer getLineItemDeserializer()
        {
            return lineItemDeserializer;
        }

        public FileSplit getLineItemFileSplit()
        {
            return lineItemFileSplit;
        }

        public List<BenchmarkLineItem> getLineItemBenchmarks()
        {
            return lineItemBenchmarks;
        }
    }

    public static Properties createTableProperties(List<HiveColumn> columns)
    {
        Properties orderTableProperties = new Properties();
        orderTableProperties.setProperty(
                "columns",
                Joiner.on(',').join(Iterables.transform(columns, nameGetter())));
        orderTableProperties.setProperty(
                "columns.types",
                Joiner.on(':').join(Iterables.transform(columns, typeNameGetter())));
        return orderTableProperties;
    }

    public static void writeOrders(File outputFile, HiveOutputFormat<?, ?> outputFormat, SerDe serDe, String compressionCodec)
            throws Exception
    {
        FSRecordWriter recordWriter = createRecordReader(ORDER_COLUMNS, outputFile, outputFormat, compressionCodec);

        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector(ORDER_COLUMNS);
        Object row = objectInspector.create();

        List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());

        for (Order order : new OrderGenerator(1, 1, 1)) {
            objectInspector.setStructFieldData(row, fields.get(0), order.getOrderKey());
            objectInspector.setStructFieldData(row, fields.get(1), order.getCustomerKey());
            objectInspector.setStructFieldData(row, fields.get(2), String.valueOf(order.getOrderStatus()));
            objectInspector.setStructFieldData(row, fields.get(3), order.getTotalPrice());
            objectInspector.setStructFieldData(row, fields.get(4), order.getOrderDate());
            objectInspector.setStructFieldData(row, fields.get(5), order.getOrderPriority());
            objectInspector.setStructFieldData(row, fields.get(6), order.getClerk());
            objectInspector.setStructFieldData(row, fields.get(7), order.getShipPriority());
            objectInspector.setStructFieldData(row, fields.get(8), order.getComment());

            Writable record = serDe.serialize(row, objectInspector);
            recordWriter.write(record);
        }

        recordWriter.close(false);
    }

    public static void writeLineItems(File outputFile, HiveOutputFormat<?, ?> outputFormat, SerDe serDe, String compressionCodec)
            throws Exception
    {
        FSRecordWriter recordWriter = createRecordReader(LINE_ITEM_COLUMNS, outputFile, outputFormat, compressionCodec);

        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector(LINE_ITEM_COLUMNS);
        Object row = objectInspector.create();

        List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());

        for (LineItem lineItem : new LineItemGenerator(1, 1, 1)) {
            objectInspector.setStructFieldData(row, fields.get(0), lineItem.getOrderKey());
            objectInspector.setStructFieldData(row, fields.get(1), lineItem.getPartKey());
            objectInspector.setStructFieldData(row, fields.get(2), lineItem.getSupplierKey());
            objectInspector.setStructFieldData(row, fields.get(3), lineItem.getLineNumber());
            objectInspector.setStructFieldData(row, fields.get(4), lineItem.getQuantity());
            objectInspector.setStructFieldData(row, fields.get(5), lineItem.getExtendedPrice());
            objectInspector.setStructFieldData(row, fields.get(6), lineItem.getDiscount());
            objectInspector.setStructFieldData(row, fields.get(7), lineItem.getTax());
            objectInspector.setStructFieldData(row, fields.get(8), lineItem.getReturnFlag());
            objectInspector.setStructFieldData(row, fields.get(9), lineItem.getStatus());
            objectInspector.setStructFieldData(row, fields.get(10), lineItem.getShipDate());
            objectInspector.setStructFieldData(row, fields.get(11), lineItem.getCommitDate());
            objectInspector.setStructFieldData(row, fields.get(12), lineItem.getReceiptDate());
            objectInspector.setStructFieldData(row, fields.get(13), lineItem.getShipInstructions());
            objectInspector.setStructFieldData(row, fields.get(14), lineItem.getShipMode());
            objectInspector.setStructFieldData(row, fields.get(15), lineItem.getComment());

            Writable record = serDe.serialize(row, objectInspector);
            recordWriter.write(record);
        }

        recordWriter.close(false);
    }

    public static FSRecordWriter createRecordReader(List<HiveColumn> columns, File outputFile, HiveOutputFormat<?, ?> outputFormat, String compressionCodec)
            throws Exception
    {
        JobConf jobConf = new JobConf();
        if (compressionCodec != null) {
            CompressionCodec codec = new CompressionCodecFactory(new Configuration()).getCodecByName(compressionCodec);
            jobConf.set(COMPRESS_CODEC, codec.getClass().getName());
            jobConf.set(COMPRESS_TYPE, CompressionType.BLOCK.toString());
            jobConf.set("parquet.compression", compressionCodec);
            jobConf.set("parquet.enable.dictionary", "true");
        }

        FSRecordWriter recordWriter = outputFormat.getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compressionCodec != null,
                createTableProperties(columns),
                new Progressable()
                {
                    @Override
                    public void progress()
                    {
                    }
                }
        );

        return recordWriter;
    }

    public static SettableStructObjectInspector createSettableStructObjectInspector(List<HiveColumn> columns)
    {
        return getStandardStructObjectInspector(
                Lists.transform(columns, nameGetter()),
                Lists.transform(columns, objectInspectorGetter()));
    }

    public static final class HiveColumn
    {
        private final String name;
        private final ObjectInspector objectInspector;

        private HiveColumn(String name, ObjectInspector objectInspector)
        {
            this.name = checkNotNull(name, "name is null");
            this.objectInspector = checkNotNull(objectInspector, "objectInspector is null");
        }

        public String getName()
        {
            return name;
        }

        public String getTypeName()
        {
            return objectInspector.getTypeName();
        }

        public ObjectInspector getObjectInspector()
        {
            return objectInspector;
        }

        public static Function<HiveColumn, String> nameGetter()
        {
            return new Function<HiveColumn, String>()
            {
                @Override
                public String apply(HiveColumn hiveColumn)
                {
                    return hiveColumn.getName();
                }
            };
        }

        public static Function<HiveColumn, String> typeNameGetter()
        {
            return new Function<HiveColumn, String>()
            {
                @Override
                public String apply(HiveColumn hiveColumn)
                {
                    return hiveColumn.getTypeName();
                }
            };
        }

        public static Function<HiveColumn, ObjectInspector> objectInspectorGetter()
        {
            return new Function<HiveColumn, ObjectInspector>()
            {
                @Override
                public ObjectInspector apply(HiveColumn hiveColumn)
                {
                    return hiveColumn.getObjectInspector();
                }
            };
        }
    }

    private static final Unsafe unsafe;

    static {
        try {
            // fetch theUnsafe object
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe == null) {
                throw new RuntimeException("Unsafe access not available");
            }

            // make sure the VM thinks bytes are only one byte wide
            if (Unsafe.ARRAY_BYTE_INDEX_SCALE != 1) {
                throw new IllegalStateException("Byte array index scale must be 1, but is " + Unsafe.ARRAY_BYTE_INDEX_SCALE);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
