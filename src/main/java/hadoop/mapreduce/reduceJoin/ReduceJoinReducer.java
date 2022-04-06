package hadoop.mapreduce.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class ReduceJoinReducer extends Reducer<Text,ProductOrderBean,ProductOrderBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<ProductOrderBean> values, Context context) throws IOException, InterruptedException {
        //初始化集合
        ArrayList<ProductOrderBean> productOrders = new ArrayList<>();
        ProductOrderBean productOrder = new ProductOrderBean();

        //循环遍历
        for (ProductOrderBean value : values) {
            if ("order".equals(value.getFlag())){
                ProductOrderBean productOrderBean = new ProductOrderBean();
                try {
                    BeanUtils.copyProperties(productOrderBean,value);
                    productOrders.add(productOrderBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }else {
                try {
                    BeanUtils.copyProperties(productOrder,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        //循环遍历orderBeans 赋值 pdname
        for (ProductOrderBean order : productOrders) {
            order.setPname(productOrder.getPname());
            context.write(order,NullWritable.get());
        }

    }

}
