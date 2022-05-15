package com.geekbang;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Collectors;

public class MapReduce {
    public static class MapReduceWrite extends ArrayWritable {

        public MapReduceWrite(int[] ints) {
            super(IntWritable.class);
            IntWritable[] intWritable = new IntWritable[ints.length];
            for (int i = 0; i < ints.length; i++) {
                intWritable[i] = new IntWritable(ints[i]);
            }
            set(intWritable);
        }

        @Override
        public IntWritable[] get() {
            Writable[] temp = super.get();
            IntWritable[] values = new IntWritable[temp.length];
            for (int i = 0; i < temp.length; i++) {
                values[i] = (IntWritable) temp[i];
            }
            return values;
        }

        @Override
        public String toString() {
            return Arrays.asList(this.get()).stream().map(IntWritable::toString)
                    .collect(Collectors.joining(" "));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, MapReduceWrite, Text, MapReduceWrite> {
        @Override
        public void reduce(Text key,
                           Iterator<MapReduceWrite> values,
                           OutputCollector<Text, MapReduceWrite> output,
                           Reporter reporter) throws IOException {
            int[] total = new int[3];
            while (values.hasNext()) {
                MapReduceWrite row = values.next();
                IntWritable[] flowInfo = row.get();
                for (int i = 0; i < flowInfo.length; i++) {
                    total[i] += flowInfo[i].get();
                }
            }
            output.collect(key, new MapReduceWrite(total));
        }
    }

    public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, ArrayWritable> {
        @Override
        public void map(LongWritable key,
                        Text value,
                        OutputCollector<Text, ArrayWritable> output,
                        Reporter reporter) throws IOException {
            if (StringUtils.isNotBlank(value.toString())) {
                String line = value.toString();
                String[] parts = line.split("	");
                int FIELDS_COUNT = 10;
                if (parts.length >= FIELDS_COUNT) {
                    int[] arr_int = new int[]{
                            Integer.parseInt(parts[8]),
                            Integer.parseInt(parts[9]),
                            Integer.parseInt(parts[9]) + Integer.parseInt(parts[8])};
                    output.collect(new Text(parts[1]), new MapReduceWrite(arr_int));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        JobConf job = new JobConf(conf, MapReduce.class);
        job.setJobName("Map Reduce Job");
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapReduceWrite.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setInputFormat(TextInputFormat.class);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        job.setNumMapTasks(4);
        job.setNumReduceTasks(2);

        JobClient.runJob(job);
    }
}
