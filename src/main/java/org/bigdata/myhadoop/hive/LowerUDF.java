package org.bigdata.myhadoop.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author liuhui
 * @version V1.0
 * @Package org.bigdata.myhadoop.hive
 * @date 2017/9/22 20:13
 * 1.New UDF classes need to inherit from this UDF class
 * 2.Implement one or more methods named {@code evaluate} which will be called by Hive
 * 3.{@code evaluate} should never be a void method. However it can return {@code null} if
 * needed.
 * 4.Return types as well as method arguments can be either Java primitives or the corresponding
 * {@link org.apache.hadoop.io.Writable Writable} class.
 *
 */
public class LowerUDF extends UDF {

    public Text evaluate(Text str){

        if (null == str.toString()){

            return null;
        }

        //lower
        return new Text(str.toString().toLowerCase());
    }

    public static void main(String[] args) {
        System.out.println(new LowerUDF().evaluate(new Text("HIVE")));
    }
}
