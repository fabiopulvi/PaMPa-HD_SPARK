package pampa; /**
 * Created by fabio on 01/11/16.
 */


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;


public class pampa_main {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("pampa.stat_1").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println("First Example of data analysis with Spark and MLlib");

        //Load an external dataset
        final JavaRDD<String> dataset = sc.textFile("dataset1.txt");


        class split implements Function<String,Tuple2<String,ArrayList<Integer>>> {
            public Tuple2<String, ArrayList<Integer>> call(String s) {
                String[] row = s.split(",");
                String item = row[0];
                String[] tidlist_str = row[1].split(" ");
                ArrayList<Integer> tidlist_int = new ArrayList<Integer>();
                for (String tid : tidlist_str) tidlist_int.add(Integer.parseInt(tid));
                Tuple2<String, ArrayList<Integer>> row_out = new Tuple2<String, ArrayList<Integer>>(item, tidlist_int);

                return row_out;
            }
        }
        JavaRDD<Tuple2<String,ArrayList<Integer>>> dataset_2 = dataset.map(new split());



        class GetLength implements Function<String, Integer> {
            public Integer call(String s) {return s.length();};
        }

        JavaRDD<Integer> lineLengths = dataset.map(new GetLength());

        dataset.foreach(new VoidFunction<String>(){
            public void call(String line) {
                System.out.println(line);
            }});
        dataset_2.foreach(new VoidFunction<Tuple2<String,ArrayList<Integer>>>(){
            public void call(Tuple2<String,ArrayList<Integer>> t) {
                System.out.println(t._1());

            }});
        //dataset.collect().foreach(println);
        System.out.println(lineLengths);

    }
}
