package net.murz;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class JsonOutputFormat<K, V> extends TextOutputFormat<K, V> {

    @Override
    public RecordWriter<K, V> getRecordWriter(
            FileSystem ignored,
            JobConf job,
            String name,
            Progressable progress) throws IOException {
        Path file = FileOutputFormat.getTaskOutputPath(job, name);
        FileSystem fs = file.getFileSystem(job);
        FSDataOutputStream out = fs.create(file, progress);
        return new JsonRecordWriter(out);
    }

    private static class JsonRecordWriter<K, V> extends LineRecordWriter<K, V>{
        static boolean firstRecord = true;
        @Override
        public synchronized void close(Reporter reporter)
                throws IOException {
            out.writeChar('}');
            super.close(null);
        }

        @Override
        public synchronized void write(K key, V value)
                throws IOException {
            if (!firstRecord) {
              out.writeChars(",\r\n");
            } else {
              firstRecord = false;
            }
            out.writeChars("\"" + key.toString() + "\":"+
                    value.toString());
        }

        public JsonRecordWriter(DataOutputStream out) throws IOException{
            super(out);
            out.writeChar('{');
        }
    }
}
