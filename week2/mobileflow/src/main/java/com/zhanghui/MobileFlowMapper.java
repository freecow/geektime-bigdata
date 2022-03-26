package com.zhanghui;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MobileFlowMapper extends Mapper<LongWritable, Text, Text, MobileFlowBean> {

    MobileFlowBean bean = new MobileFlowBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // input one line
        String line = value.toString();

        // split field
        String[] fields = line.split("\t");

        // bean
        String phoneNum = fields[1];
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);
        bean.set(upFlow, downFlow);
        k.set(phoneNum);

        // output
        context.write(k, bean);
    }
}
