package com.zhanghui;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MobileFlowDriver {

    public static void main(String[] args) throws Exception {

        // Config get
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // Jar class
        job.setJarByClass(MobileFlowDriver.class);

        // mapper+reducer class
        job.setMapperClass(MobileFlowMapper.class);
        job.setReducerClass(MobileFlowReducer.class);

        // mapper output key+value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MobileFlowBean.class);

        // output key+value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MobileFlowBean.class);

        // Reducenum args
        job.setNumReduceTasks(Integer.parseInt(args[2]));

        // Input path & output path args
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
