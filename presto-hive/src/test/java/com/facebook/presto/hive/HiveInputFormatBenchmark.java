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

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.net.SocksSocketFactory;

import javax.net.SocketFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

@SuppressWarnings("deprecation")
public final class HiveInputFormatBenchmark
{
    private HiveInputFormatBenchmark()
    {
    }

    public static void main(String[] args)
            throws Exception
    {
        List<BenchmarkFile> benchmarkFiles = ImmutableList.of(
                new BenchmarkFile(
                        "text",
                        "hdfs://prestotest001.prn2.facebook.com:8020/user/hive/warehouse/presto_test/ds=2012-12-29/file_format=textfile/dummy=6/000000_0",
                        "presto_test.txt",
                        new TextInputFormat(),
                        new LazySimpleSerDe()),

                new BenchmarkFile(
                        "sequence",
                        "hdfs://prestotest001.prn2.facebook.com:8020/user/hive/warehouse/presto_test/ds=2012-12-29/file_format=sequencefile/dummy=4/000000_0",
                        "presto_test.sequence",
                        new SequenceFileInputFormat<Object, Writable>(),
                        new LazySimpleSerDe()),

                new BenchmarkFile(
                        "rc text",
                        "hdfs://prestotest001.prn2.facebook.com:8020/user/hive/warehouse/presto_test/ds=2012-12-29/file_format=rcfile-text/dummy=0/000000_0",
                        "presto_test.rc",
                        new RCFileInputFormat<>(),
                        new ColumnarSerDe()),

                new BenchmarkFile(
                        "rc binary",
                        "hdfs://prestotest001.prn2.facebook.com:8020/user/hive/warehouse/presto_test/ds=2012-12-29/file_format=rcfile-binary/dummy=2/000000_0",
                        "presto_test.rc-binary",
                        new RCFileInputFormat<>(),
                        new LazyBinaryColumnarSerDe())
        );

        JobConf jobConf = new JobConf();
        System.out.println("============ WARM UP ============");
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            benchmark(jobConf, benchmarkFile, 3);
        }

        System.out.println();
        System.out.println();
        System.out.println("============ BENCHMARK ============");
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            benchmark(jobConf, benchmarkFile, 4);
        }
    }

    private static void benchmark(JobConf jobConf, BenchmarkFile benchmarkFile, int loopCount)
            throws Exception
    {
        System.out.println(benchmarkFile.getName());

        for (int loops = 0; loops < loopCount; loops++) {
            long start = System.nanoTime();

            runOnce(jobConf, benchmarkFile.getFileSplit(), benchmarkFile.getInputFormat(), benchmarkFile.getDeserializer());
            System.out.println(Duration.nanosSince(start).convertToMostSuccinctTimeUnit());
        }
    }

    private static <K, V extends Writable> void runOnce(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        StructField stringField = rowInspector.getStructFieldRef("t_string");
        PrimitiveObjectInspector stringFieldInspector = (PrimitiveObjectInspector) stringField.getFieldObjectInspector();
        StructField doubleField = rowInspector.getStructFieldRef("t_double");
        PrimitiveObjectInspector doubleFieldInspector = (PrimitiveObjectInspector) doubleField.getFieldObjectInspector();
        StructField bigintField = rowInspector.getStructFieldRef("t_bigint");
        PrimitiveObjectInspector bigintFieldInspector = (PrimitiveObjectInspector) bigintField.getFieldObjectInspector();

        for (int i = 0; i < 10000; i++) {
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            long stringLengthSum = 0;
            double doubleSum = 0;
            long longSum = 0;

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object stringData = rowInspector.getStructFieldData(rowData, stringField);
                if (stringData != null) {
                    Object stringPrimitive = stringFieldInspector.getPrimitiveJavaObject(stringData);
                    String stringValue = (String) stringPrimitive;
                    stringLengthSum += stringValue.length();
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
                    long longValue = ((Number) bigintPrimitive).longValue();
                    longSum += longValue;
                }
            }
            recordReader.close();

//                if (stringLengthSum != 1235 || Math.abs(doubleSum - 65669.99999) > 0.0001 || longSum != 60858) {
//                    System.out.println(stringLengthSum + ", " + doubleSum + ", " + longSum);
//                    throw new IllegalArgumentException("invalid sum");
//                }
        }
    }

    private static Deserializer initializeDeserializer(Configuration config, Deserializer deserializer)
            throws SerDeException
    {
        Properties tableProperties = new Properties();
        tableProperties.setProperty(
                "columns",
                "t_string,t_tinyint,t_smallint,t_int,t_bigint,t_float,t_double,t_map,t_boolean,t_timestamp,t_binary,t_array_string,t_complex");
        tableProperties.setProperty(
                "columns.types",
                "string:tinyint:smallint:int:bigint:float:double:map<string,string>:boolean:timestamp:binary:array<string>:map<int,array<struct<s_string:string,s_double:double>>>");

        deserializer.initialize(config, tableProperties);

        return deserializer;
    }

    private static FileSplit createFileSplit(String remotePath, String localFile)
            throws IOException
    {
        File file = new File(localFile);
        if (!file.exists()) {
            Configuration config = new Configuration();
            config.setClass("hadoop.rpc.socket.factory.class.default", SocksSocketFactory.class, SocketFactory.class);
            config.set("hadoop.socks.server", "localhost:1080");

            Path path = new Path(remotePath);
            ByteStreams.copy(newHdfsInputStreamSupplier(config, path), Files.newOutputStreamSupplier(file));
        }
        return new FileSplit(new Path(file.toURI()), 0, file.length(), new String[0]);
    }

    public static InputSupplier<FSDataInputStream> newHdfsInputStreamSupplier(final Configuration configuration, final Path path)
    {
        return new InputSupplier<FSDataInputStream>()
        {
            @Override
            public FSDataInputStream getInput()
                    throws IOException
            {
                return path.getFileSystem(configuration).open(path);
            }
        };
    }

    private static class BenchmarkFile
    {
        private final String name;
        private final InputFormat<?, ? extends Writable> inputFormat;
        private final Deserializer deserializer;
        private final FileSplit fileSplit;

        public BenchmarkFile(String name, String remotePath, String localFile, InputFormat<?, ? extends Writable> inputFormat, Deserializer deserializer)
                throws Exception
        {
            this.name = name;
            this.inputFormat = inputFormat;
            this.deserializer = initializeDeserializer(new Configuration(), deserializer);

            this.fileSplit = createFileSplit(remotePath, localFile);
        }

        private String getName()
        {
            return name;
        }

        private InputFormat<?, ? extends Writable> getInputFormat()
        {
            return inputFormat;
        }

        private Deserializer getDeserializer()
        {
            return deserializer;
        }

        private FileSplit getFileSplit()
        {
            return fileSplit;
        }
    }
}
