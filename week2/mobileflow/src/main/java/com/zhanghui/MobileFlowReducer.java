package com.zhanghui;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MobileFlowReducer extends Reducer<Text, MobileFlowBean, Text, MobileFlowBean> {

    @Override
    protected void reduce(Text key, Iterable<MobileFlowBean> values, Context context)
            throws IOException, InterruptedException {
        // Calculate Total
        long sum_upFlow = 0;
        long sum_downFlow = 0;
        for (MobileFlowBean bean : values) {
            sum_upFlow += bean.getUpFlow();
            sum_downFlow += bean.getDownFlow();
        }

        // output
        context.write(key, new MobileFlowBean(sum_upFlow, sum_downFlow));
    }
}

