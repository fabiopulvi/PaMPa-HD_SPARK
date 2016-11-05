package pampa; /**
 * Created by fabio on 01/11/16.
 */


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.util.SystemClock;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;


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

        //The dataset is in the right format now
/* questa non andava bene perchè non potevo cambiare tipo di output.
        //let's map it to build the first level projected tables in the reducers
        class mapping implements FlatMapFunction<Tuple2<String,ArrayList<Integer>>,Tuple2<Integer, Tuple2<String,ArrayList<Integer>>>> {
            public Iterable<Tuple2<Integer, Tuple2<String,ArrayList<Integer>>>> call (Tuple2<String,ArrayList<Integer>> row_input) {
                List<Tuple2<Integer, Tuple2<String,ArrayList<Integer>>>> row_output = new ArrayList<Tuple2<Integer, Tuple2<String,ArrayList<Integer>>>>();
                for (int tid=0;tid<row_input._2().size();tid++){
                    String partial_tidlist="";
                    int key=row_input._2().get(tid);
                    ArrayList<Integer> tidlist_temp = new ArrayList<Integer>();
                    for (int i=tid+1;i<row_input._2().size();i++)
                        tidlist_temp.add(row_input._2().get(i));
                    Tuple2 <String, ArrayList<Integer>> value = new Tuple2<String, ArrayList<Integer>>(row_input._1(),tidlist_temp);
                    Tuple2<Integer,Tuple2<String, ArrayList<Integer>>> tupla = new Tuple2<Integer, Tuple2<String, ArrayList<Integer>>>(key,value);
                    row_output.add(tupla);
                }
                return row_output;

            }
        }
       */

        //let's map it to build the first level projected tables in the reducers
        class mapping2 implements PairFlatMapFunction<Tuple2<String,ArrayList<Integer>>,Integer, Tuple2<String,ArrayList<Integer>>> {
            public Iterable<Tuple2<Integer, Tuple2<String,ArrayList<Integer>>>> call (Tuple2<String,ArrayList<Integer>> row_input) {
                List<Tuple2<Integer, Tuple2<String,ArrayList<Integer>>>> row_output = new ArrayList<Tuple2<Integer, Tuple2<String,ArrayList<Integer>>>>();
                for (int tid=0;tid<row_input._2().size();tid++){
                    String partial_tidlist="";
                    int key=row_input._2().get(tid);
                    ArrayList<Integer> tidlist_temp = new ArrayList<Integer>();
                    for (int i=tid+1;i<row_input._2().size();i++)
                        tidlist_temp.add(row_input._2().get(i));
                    Tuple2 <String, ArrayList<Integer>> value = new Tuple2<String, ArrayList<Integer>>(row_input._1(),tidlist_temp);
                    Tuple2<Integer,Tuple2<String, ArrayList<Integer>>> tupla = new Tuple2<Integer, Tuple2<String, ArrayList<Integer>>>(key,value);
                    row_output.add(tupla);
                }
                return row_output;

            }
        }

        JavaRDD<Tuple2<Integer, Tuple2<String,ArrayList<Integer>>>> mapped_values = dataset_2.flatMap(new mapping());
        JavaPairRDD<Integer, Tuple2<String,ArrayList<Integer>>> mapped_values2 = dataset_2.flatMapToPair(new mapping2());

        List<Tuple2<String,ArrayList<Integer>>> initialSet=  new ArrayList<Tuple2<String, ArrayList<Integer>>>();

        class addtolist implements Function2<List<Tuple2<String,ArrayList<Integer>>>, Tuple2<String,ArrayList<Integer>>, List<Tuple2<String,ArrayList<Integer>>>> {
            public List<Tuple2<String,ArrayList<Integer>>> call(List<Tuple2<String,ArrayList<Integer>>> list, Tuple2<String,ArrayList<Integer>> s) {
                list.add(s);
                return list;
            };
        }

        class mergelist implements Function2 <List<Tuple2<String,ArrayList<Integer>>>, List<Tuple2<String,ArrayList<Integer>>>, List<Tuple2<String,ArrayList<Integer>>>> {
            public  List<Tuple2<String,ArrayList<Integer>>> call(List<Tuple2<String,ArrayList<Integer>>> list, List<Tuple2<String,ArrayList<Integer>>> list2) {
                list.addAll(list2);
                return list;
            }
        }




        JavaPairRDD<Integer, List<Tuple2<String,ArrayList<Integer>>>> reduced_values = mapped_values2.aggregateByKey(initialSet,new addtolist(), new mergelist());










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
                System.out.println(t._1()+"--"+t._2());

            }});
        mapped_values.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, ArrayList<Integer>>>>() {
            public void call(Tuple2<Integer, Tuple2<String, ArrayList<Integer>>> integerTuple2Tuple2) throws Exception {
                System.out.println(integerTuple2Tuple2._1()+" "+integerTuple2Tuple2._2()._1()+":"+integerTuple2Tuple2._2()._2());

            }
        });

        reduced_values.foreach(new VoidFunction<Tuple2<Integer, List<Tuple2<String, ArrayList<Integer>>>>>() {
            public void call(Tuple2<Integer, List<Tuple2<String, ArrayList<Integer>>>> integerListTuple2) throws Exception {
                System.out.println("key: "+integerListTuple2._1());
                for (Tuple2<String, ArrayList<Integer>> tupla: integerListTuple2._2()) {
                    System.out.println(tupla._1()+":"+tupla._2());
                }
            }
        });
        //dataset.collect().foreach(println);
        System.out.println(lineLengths);

    }
}
