package hadoop.mapreduce.partition2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /**
         * 1.获取一行
         * 1 	13736230513	120.196.100.99	www.baidu.com	2481		 24681			200
         * 2 	13560436666	120.196.100.99		            1116		 954			200
         */
        String line = value.toString();

        /**
         * 2.切割
         * 1,13736230513,120.196.100.99,www.baidu.com,2481,24681,200
         */
        String[] split = line.split("\t");

        /**
         * 3.抓取所需数据
         * 手机号 13736230513
         * 上行流量和下行流量 2481,24681
         */
        String phone = split[1];
        String upFlow = split[split.length - 3];
        String downFlow = split[split.length - 2];

        //4.封装
        outK.set(phone);
        outV.setUpFlow(Long.valueOf(upFlow));
        outV.setDownFlow(Long.valueOf(downFlow));
        outV.setSumFlow();

        //5.写出(这里进入环形缓冲区)
        context.write(outK, outV);
    }
}
