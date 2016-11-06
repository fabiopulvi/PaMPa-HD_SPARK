package pampa; /**
 * Created by fabio on 01/11/16.
 */


import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.SystemClock;


public class stat_1 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("pampa.stat_1").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println("First Example of data analysis with Spark and MLlib");

        //Load an external dataset
        JavaRDD<String> dataset = sc.textFile("gapminder.csv");



        class GetLength implements Function<String, Integer> {
            public Integer call(String s) {return s.length();};
        }

        JavaRDD<Integer> lineLengths = dataset.map(new GetLength());

        dataset.foreach(new VoidFunction<String>(){
            public void call(String line) {
                System.out.println(line);
            }});
        //dataset.collect().foreach(println);
        System.out.println(lineLengths);

    }
}
