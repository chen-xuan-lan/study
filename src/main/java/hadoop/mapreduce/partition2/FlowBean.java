package hadoop.mapreduce.partition2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 流量类
 * 1、定义类实现Writable接口
 * 2、重写序列化和反序列化方法 注：序列化和反序列化的顺序需要完全一致
 * 3、重写空参构造(反序列化时，需要反射调用空参构造函数)
 * 4、重写toString方法，用于打印输出(可用"\t"分开，方便后续使用)
 */
public class FlowBean implements Writable {

    private long upFlow;//上行流量
    private long downFlow;//下行流量
    private long sumFlow;//总流量

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    /**
     * 方法重载
     */
    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    /**
     * 重写空构造
     */
    public FlowBean() {
    }

    /***
     * 序列化方法
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    /**
     * 反序列化方法
     *
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return upFlow +
                "\t" + downFlow +
                "\t" + sumFlow;
    }
}
