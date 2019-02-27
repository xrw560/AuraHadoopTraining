package org.training.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * @ClassName DataTransformUDF
 * @Author nick
 * @Date 2019/2/26 11:20
 * @Description
 */
public class DataTransformUDF extends UDF {
    private final SimpleDateFormat inputFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    private final SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    /**
     * 31/Aug/2015:00:04:37 +0800转换为20150831000437
     *
     * @param input
     * @return
     */
    public Text evaluate(Text input) {
        Text output = new Text();
        if (null == input) {
            return null;
        }
        String inputDate = input.toString().trim();
        if (null == inputDate) {
            return null;
        }
        try {
            Date parseDate = inputFormat.parse(inputDate);
            String outputDate = outputFormat.format(parseDate);
            output.set(outputDate);
        } catch (Exception e) {
            e.printStackTrace();
            return output;
        }
        return output;
    }

    public static void main(String[] args) {
        System.out.println(new DataTransformUDF().evaluate(new Text("31/Aug/2015:00:04:37 +0800")));
    }
}
