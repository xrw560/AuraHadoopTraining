package org.training.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @ClassName RemoveQuotesUDF
 * @Author nick
 * @Date 2019/2/26 11:05
 * @Description
 */
public class RemoveQuotesUDF extends UDF {
    public Text evaluate(Text str) {
        if (null == str) {
            return new Text();
        }
        if (null == str.toString()) {
            return new Text();
        }
        return new Text(str.toString().replace("\"", ""));
    }
}
