package org.training.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * hive>add jar /home/hadoop/udf.jar
 * create temporary function tolowercase as 'org.training.hadoop.hive.Lower';
 * select tolowercase(name),age from t_test
 */
public class Lower extends UDF {
    public Text evaluate(final Text s) {
        if (s == null) {
            return null;
        }
        return new Text(s.toString().toLowerCase());
    }

    public static void main(String[] args) {
        System.out.println(new Lower().evaluate(new Text("HIVe")));
    }

}