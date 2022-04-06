package hadoop.mapreduce.reduceJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 商品-订单类
 * 1、定义类实现Writable接口
 * 2、重写序列化和反序列化方法 注：序列化和反序列化的顺序需要完全一致
 * 3、重写空参构造(反序列化时，需要反射调用空参构造函数)
 * 4、重写toString方法，用于打印输出(可用"\t"分开，方便后续使用)
 */
public class ProductOrderBean implements Writable {
    private String id;//订单id
    private String pid;//商品id
    private int amount;//商品数量
    private String pname;//商品名称
    private String flag;//标记是什么表 order pd

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    /**
     * 空参构造
     */
    public ProductOrderBean() {
    }

    /**
     * 序列化
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        //String类型数据写出使用 writeUTF() 方法
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pname);
        out.writeUTF(flag);
    }

    /**
     * 反序列化
     *
     * @param input
     * @throws IOException
     */
    @Override
    public void readFields(DataInput input) throws IOException {

        //读取String类型数据，使用readUTF()方法
        this.id = input.readUTF();
        this.pid = input.readUTF();
        this.amount = input.readInt();
        this.pname = input.readUTF();
        this.flag = input.readUTF();
    }

    @Override
    public String toString() {
        return id + '\t' + pname + '\t' + amount;
    }
}
