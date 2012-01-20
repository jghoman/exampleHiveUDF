package exampleHiveUDF;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;

public class AddExampleUDF extends UDF {
  public IntWritable evaluate(IntWritable a, IntWritable b) {
    return new IntWritable(a.get() + b.get());
  }
}
