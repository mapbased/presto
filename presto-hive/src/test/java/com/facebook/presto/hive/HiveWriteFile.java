package com.facebook.presto.hive;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.joda.time.DateTime;

import java.io.File;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings("deprecation")
public class HiveWriteFile
{
    private static final Progressable NULL_PROGRESSABLE = new Progressable()
    {
        @Override
        public void progress()
        {
        }
    };

    public static void main(String[] args)
            throws Exception
    {
        Properties tableProperties = new Properties();
        tableProperties.setProperty(
                "columns",
                "t_string,t_tinyint,t_smallint,t_int,t_bigint,t_float,t_double,t_map,t_boolean,t_timestamp,t_binary,t_array_string,t_complex");
        tableProperties.setProperty(
                "columns.types",
                "string:tinyint:smallint:int:bigint:float:double:map<string,string>:boolean:timestamp:binary:array<string>:map<int,array<struct<s_string:string,s_double:double>>>");

        writeFile(tableProperties, new File("test.txt"), new HiveIgnoreKeyTextOutputFormat<>(), new MetadataTypedColumnsetSerDe());
        writeFile(tableProperties, new File("test.rc"), new RCFileOutputFormat(), new ColumnarSerDe());
    }

    public static void writeFile(Properties tableProperties, File outputFile, HiveOutputFormat<?, ?> outputFormat, SerDe serDe)
            throws Exception
    {
        RecordWriter recordWriter = outputFormat.getHiveRecordWriter(
                new JobConf(),
                new Path(outputFile.toURI()),
                Text.class,
                false,
                tableProperties,
                NULL_PROGRESSABLE
        );

        serDe.initialize(new Configuration(), tableProperties);

        MetadataTypedColumnsetSerDe structBuilderSerDe = new MetadataTypedColumnsetSerDe();
        structBuilderSerDe.initialize(new Configuration(), tableProperties);
        SettableStructObjectInspector settableStructObjectInspector = (SettableStructObjectInspector) structBuilderSerDe.getObjectInspector();

        writeData(recordWriter, serDe, "file", 7, settableStructObjectInspector);
    }

    private static void writeData(RecordWriter recordWriter, SerDe serDe, String fileType, int baseValue, SettableStructObjectInspector objectInspector)
            throws Exception
    {
        Object row = objectInspector.create();

        List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());

        for (int rowNumber = 0; rowNumber < 1_000_000; rowNumber++) {
            if (rowNumber % 19 == 0) {
                objectInspector.setStructFieldData(row, fields.get(0), null);
            }
            else {
                objectInspector.setStructFieldData(row, fields.get(0), fileType + " test");
            }

            objectInspector.setStructFieldData(row, fields.get(1), ((byte) (baseValue + 1 + rowNumber)));
            objectInspector.setStructFieldData(row, fields.get(2), (short) (baseValue + 2 + rowNumber));
            objectInspector.setStructFieldData(row, fields.get(3), baseValue + 3 + rowNumber);

            if (rowNumber % 13 == 0) {
                objectInspector.setStructFieldData(row, fields.get(4), null);
            }
            else {
                objectInspector.setStructFieldData(row, fields.get(4), (long) baseValue + 4 + rowNumber);
            }

            objectInspector.setStructFieldData(row, fields.get(5), (float) (baseValue + 5.1 + rowNumber));
            objectInspector.setStructFieldData(row, fields.get(6), baseValue + 6.2 + rowNumber);

            if (rowNumber % 3 == 2) {
                objectInspector.setStructFieldData(row, fields.get(8), null);
            }
            else {
                objectInspector.setStructFieldData(row, fields.get(8), rowNumber % 3 != 0);
            }

            if (rowNumber % 17 == 0) {
                objectInspector.setStructFieldData(row, fields.get(9), null);
            }
            else {
                long seconds = MILLISECONDS.toSeconds(new DateTime(2011, 5, 6, 7, 8, 9, 123).getMillis());
                objectInspector.setStructFieldData(row, fields.get(9), new Timestamp(seconds * 1000));
            }

            if (rowNumber % 23 == 0) {
                objectInspector.setStructFieldData(row, fields.get(10), null);
            }
            else {
                objectInspector.setStructFieldData(row, fields.get(10), (fileType + " test").getBytes(Charsets.UTF_8));
            }

            Writable record = serDe.serialize(row, objectInspector);
            recordWriter.write(record);

        }
        recordWriter.close(false);
    }

//    private static void bar(StandardStructObjectInspector objectInspector, Object row, List<StructField> fields)
//    {
//        objectInspector.setStructFieldData(row, fields.get(0), "foo");
//
//        objectInspector.setStructFieldData(row, fields.get(1), (byte) 1);
//        objectInspector.setStructFieldData(row, fields.get(2), (short) 2);
//        objectInspector.setStructFieldData(row, fields.get(3), 3);
//        objectInspector.setStructFieldData(row, fields.get(4), 4L);
//
//        objectInspector.setStructFieldData(row, fields.get(5), 5.5f);
//        objectInspector.setStructFieldData(row, fields.get(6), 6.6);
//
//        objectInspector.setStructFieldData(row, fields.get(7), null);
//
//        objectInspector.setStructFieldData(row, fields.get(8), true);
//
//        objectInspector.setStructFieldData(row, fields.get(9), new Timestamp(8));
//
//        objectInspector.setStructFieldData(row, fields.get(10), new byte[]{9});
//
//        objectInspector.setStructFieldData(row, fields.get(11), null);
//        objectInspector.setStructFieldData(row, fields.get(12), null);
//    }
//
//    public static void x(StandardStructObjectInspector objectInspector, Object row, List<StructField> fields, String fileType, int baseValue)
//    {
//        for (int rowNumber = 0; rowNumber < 100_000; rowNumber++) {
//
//
//            if (rowNumber % 19 == 0) {
//                objectInspector.setStructFieldData(row, fields.get(0), null);
//            }
//            else {
//                objectInspector.setStructFieldData(row, fields.get(0), fileType + " test");
//            }
//
//            objectInspector.setStructFieldData(row, fields.get(1), ((byte) (baseValue + 1 + rowNumber)));
//            objectInspector.setStructFieldData(row, fields.get(2), (short) (baseValue + 2 + rowNumber));
//            objectInspector.setStructFieldData(row, fields.get(3), baseValue + 3 + rowNumber);
//
//            if (rowNumber % 13 == 0) {
//                objectInspector.setStructFieldData(row, fields.get(4), null);
//            }
//            else {
//                objectInspector.setStructFieldData(row, fields.get(4), (long) baseValue + 4 + rowNumber);
//            }
//
//            objectInspector.setStructFieldData(row, fields.get(5), (float) baseValue + 5.1 + rowNumber);
//            objectInspector.setStructFieldData(row, fields.get(6), baseValue + 6.2 + rowNumber);
//
//            if (rowNumber % 3 == 2) {
//                objectInspector.setStructFieldData(row, fields.get(8), null);
//            }
//            else {
//                objectInspector.setStructFieldData(row, fields.get(8), rowNumber % 3 != 0);
//            }
//
//            if (rowNumber % 17 == 0) {
//                objectInspector.setStructFieldData(row, fields.get(9), null);
//            }
//            else {
//                long seconds = MILLISECONDS.toSeconds(new DateTime(2011, 5, 6, 7, 8, 9, 123).getMillis());
//                objectInspector.setStructFieldData(row, fields.get(9), new Timestamp(seconds * 1000));
//            }
//
//            if (rowNumber % 23 == 0) {
//                objectInspector.setStructFieldData(row, fields.get(10), null);
//            }
//            else {
//                objectInspector.setStructFieldData(row, fields.get(10), (fileType + " test").getBytes(Charsets.UTF_8));
//            }
//
//            Text serializedText = (Text) lazySimpleSerDe.serialize(row, objectInspector);
//            recordWriter.write(serializedText);
//
//        }
//    }


    //        // Deserialize
    //        List<String> fieldNames = ImmutableList.of("t_string",
    //                "t_tinyint",
    //                "t_smallint",
    //                "t_int",
    //                "t_bigint",
    //                "t_float",
    //                "t_double",
    //                "t_map",
    //                "t_boolean",
    //                "t_timestamp",
    //                "t_binary",
    //                "t_array_string",
    //                "t_complex");
    //
    //        List<ObjectInspector> fieldInspectors = ImmutableList.<ObjectInspector>of(
    //                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
    //                PrimitiveObjectInspectorFactory.javaByteObjectInspector,
    //                PrimitiveObjectInspectorFactory.javaShortObjectInspector,
    //                PrimitiveObjectInspectorFactory.javaIntObjectInspector,
    //                PrimitiveObjectInspectorFactory.javaLongObjectInspector,
    //                PrimitiveObjectInspectorFactory.javaFloatObjectInspector,
    //                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
    //                PrimitiveObjectInspectorFactory.javaShortObjectInspector, // todo
    //                PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
    //                PrimitiveObjectInspectorFactory.javaTimestampObjectInspector,
    //                PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector,
    //                PrimitiveObjectInspectorFactory.javaShortObjectInspector, // todo
    //                PrimitiveObjectInspectorFactory.javaShortObjectInspector // todo
    //        );

    //        SettableStructObjectInspector objectInspector = getStandardStructObjectInspector(fieldNames, fieldInspectors);

}
