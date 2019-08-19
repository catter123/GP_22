package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author catter
 * @date 2019/7/29 19:21
 */
public class wordcount {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("javaWordCount").setMaster("local[2]");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> file = context.textFile("hdfs://hadoop02:8020/aaa");

        JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String word) throws Exception {
                return Arrays.asList(word.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> truple = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> sumed = truple.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        JavaPairRDD<Integer, String> swap = sumed.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        });

        JavaPairRDD<Integer, String> sorted = swap.sortByKey(false);
        JavaPairRDD<String, Integer> endd = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        });
        endd.saveAsTextFile("C:\\Users\\catter\\Desktop\\bbb.txt");
        context.stop();


    }
}
