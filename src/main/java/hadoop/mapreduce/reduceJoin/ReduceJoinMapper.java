package hadoop.mapreduce.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ReduceJoinMapper extends Mapper<LongWritable, Text,Text,ProductOrderBean> {

    private String fileName;
    private Text outK = new Text();
    private ProductOrderBean outV = new ProductOrderBean();


    /**
     * 1.初始化 输入端有两个文件 order product
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //初始化时获取对应文件名称，方便后续操作
        FileSplit split = (FileSplit) context.getInputSplit();//获取切片信息
        fileName = split.getPath().getName();//获取文件名称
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //根据文件名称进行不同逻辑处理
        if (fileName.contains("order")){
            String[] split = line.split("\t");
            //封装outK
            outK.set(split[1]);
            //封装outV
            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");
        }else {
            String[] split = line.split("\t");
            //封装outK
            outK.set(split[0]);
            //封装outV
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setPname(split[1]);
            outV.setFlag("pd");
        }

        context.write(outK,outV);

    }

}
