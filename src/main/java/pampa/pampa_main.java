package pampa; /**
 * Created by fabio on 01/11/16.
 */


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.util.InnerClosureFinder;
import org.apache.spark.util.SystemClock;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import pampa.row;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

public class pampa_main {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("pampa.stat_1").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);



        //Load an external dataset
        final JavaRDD<String> dataset = sc.textFile("prova6_trasposta.txt");


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

          class addtolist implements Function2<List<Tuple2<String,ArrayList<Integer>>>, Tuple2<String,ArrayList<Integer>>, List<Tuple2<String,ArrayList<Integer>>>> {
            public List<Tuple2<String,ArrayList<Integer>>> call(List<Tuple2<String,ArrayList<Integer>>> list, Tuple2<String,ArrayList<Integer>> s) {
                list.add(s);
                return list;
            };
        }
*/

        //let's map it to build the first level projected tables in the reducers
        class mapping3 implements PairFlatMapFunction<Tuple2<String,ArrayList<Integer>>,Integer, row> {
            public Iterable<Tuple2<Integer, row>>call (Tuple2<String,ArrayList<Integer>> row_input) {
                List<Tuple2<Integer, row>> row_output = new ArrayList<Tuple2<Integer, row>>();
                for (int tid=0;tid<row_input._2().size();tid++){
                    String partial_tidlist="";
                    int key=row_input._2().get(tid);
                    ArrayList<Integer> tidlist_temp = new ArrayList<Integer>();
                    for (int i=tid+1;i<row_input._2().size();i++)
                        tidlist_temp.add(row_input._2().get(i));
                    row value_row= new row(row_input._1(),tidlist_temp);
                    Tuple2 <String, ArrayList<Integer>> value = new Tuple2<String, ArrayList<Integer>>(row_input._1(),tidlist_temp);
                    Tuple2<Integer,row> tupla = new Tuple2<Integer, row>(key,value_row);
                    row_output.add(tupla);
                }
                return row_output;

            }
        }

        //JavaRDD<Tuple2<Integer, Tuple2<String,ArrayList<Integer>>>> mapped_values = dataset_2.flatMap(new mapping());
        JavaPairRDD<Integer, row> mapped_values2 = dataset_2.flatMapToPair(new mapping3());

        List<row> initialSet=  new ArrayList<row>();



        class addtolist3 implements Function2<List<row>, row, List<row>> {
            public List<row> call(List<row> list, row s) {
                list.add(s);
                return list;
            };
        }

        class mergelist3 implements Function2 <List<row>, List<row>, List<row>> {
            public  List<row> call(List<row> list, List<row> list2) {
                list.addAll(list2);
                return list;
            }
        }



        //each reducer receives this
        JavaPairRDD<Integer, List<row>> reduced_values = mapped_values2.aggregateByKey(initialSet,new addtolist3(), new mergelist3());
/*
        reduced_values.foreach(
                new VoidFunction<Tuple2<Integer, List<row>>>() {
                    public void call(Tuple2<Integer, List<row>> integerListTuple2) throws Exception {
                        System.out.println("key: "+integerListTuple2._1());
                        for (row tupla: integerListTuple2._2()) {
                            System.out.println(tupla.getItem()+":"+tupla.getTid());
                        }
                    }
                });
*/
        //let's map into a proper structure
        class intoTable implements Function<Tuple2<Integer, List<row>>, table> {
            public final table call(Tuple2<Integer, List<row>> integerListTuple2) throws Exception {
                table tab = new table(integerListTuple2._1(),(ArrayList<row>) integerListTuple2._2());
                //System.out.println("just set a table with projection"+tab.getProjection());
                //System.out.println("line 143");
                return tab;
            }
        }

        JavaRDD<table> first_level_tables = reduced_values.map(new intoTable());
        //System.out.println("tables are:"+first_level_tables.count());
        //first_level_tables.collect();
        //System.out.println("tables are:"+first_level_tables.count());

/*
        first_level_tables.foreach(new VoidFunction<table>() {
            public void call(table tab) throws Exception {
                System.out.println("row 154");
                System.out.println("projection: "+tab.getProjection());
                for (row r : tab.getRows())
                    System.out.println(r.getItem()+":"+r.getTid());
                //System.out.println(tab.isEmpty());
            }
        });
*/






    //explore the tree recursively
        class explore implements FlatMapFunction<table,table> {
            public Iterable<table> call (table t) {
                //System.out.println("here!!!!!!!!!"+t.getProjection());

                List<table> list_explored = new ArrayList<table>();
                list_explored.add(t);
                list_explored.addAll(rec_explore(t));

                //for (table tt: list_explored) System.out.println(tt.getMax());
                return list_explored;

            }
        }

        JavaRDD<table> all_tables = first_level_tables.flatMap(new explore());

        all_tables.foreach(new VoidFunction<table>() {
            public void call(table tab) throws Exception {

                System.out.println("203 projection: "+tab.getProjection());
                for (row r : tab.getRows())
                    System.out.println(r.getItem()+":"+r.getTid());
                //System.out.println(tab.isEmpty());
            }
        });
        //System.out.println("tables are:"+first_level_tables.count());







/*


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



        reduced_values.foreach(
                new VoidFunction<Tuple2<Integer, List<row>>>() {
            public void call(Tuple2<Integer, List<row>> integerListTuple2) throws Exception {
                System.out.println("key: "+integerListTuple2._1());
                for (row tupla: integerListTuple2._2()) {
                    System.out.println(tupla.getItem()+":"+tupla.getTid());
                }
            }
        });
        //dataset.collect().foreach(println);
        System.out.println(lineLengths);
 */
    }

    public static List<table> rec_explore (table t) {
        List<table> explored = new ArrayList<table>();
        ArrayList<Integer> current_projection = t.getProjection();
        int max=t.getMax();
        //System.out.println("just received: "+current_projection);
        for (int i=current_projection.get(current_projection.size()-1)+1;i<=max;i++) {
            //prepare the new table for the new iteration
            //if (current_projection.size()>1) System.out.println("i is "+i+" and table contains: "+t.getRows().size());

            //new projection
            ArrayList<Integer> new_projection = (ArrayList<Integer>) current_projection.clone();
            new_projection.add(i);

            //new arraylist of rows
            boolean flag_row=FALSE;
            ArrayList<row> new_rows = new ArrayList<row>();
            for (row r : t.getRows()) {
               // if (current_projection.size()>1)         System.out.println("row 243: "+r.getTid());


                for (int a=0; a<r.getTid().size();a++) {

                    //if (r.getTid().size()==0) continue;
       //             System.out.println("comparing:"+i+" with "+r.getTid().get(a));
                    if (r.getTid().get(a)==i) {
          //              if (current_projection.size()>1)              System.out.println("matching");
                        flag_row=TRUE;
                        ArrayList<Integer> r_to_add_int = (ArrayList<Integer>) r.getTid().clone();
                        r_to_add_int= new ArrayList<Integer>(r_to_add_int.subList(a+1,(r.getTid().size())));
                        row r_to_add_int_row = new row(r.getItem(),r_to_add_int);
                        new_rows.add(r_to_add_int_row);
                       //break;
                    }
                }
            }
            if (flag_row) {
                boolean flag_row_new = FALSE;
                //if (current_projection.size()>1) System.out.println("projection 262: "+new_projection);
                for (row r: new_rows) {
                    if (r.getTid().size()>0) flag_row_new=TRUE;

                    }

                //add the table
                table new_tab = new table (new_projection,new_rows);


                explored.add(new_tab);
                if (flag_row_new) {
                    explored.addAll(rec_explore(new_tab));

                }
                else {return explored;}

            }
            //to add the check if it is not empty







        }
        for (table tab : explored) System.out.println("all tables: "+tab.getProjection());
        return explored;
    }

}
