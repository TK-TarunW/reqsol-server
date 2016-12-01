package sol.req.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class MaxTemperatureSpark {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperatureSpark <input path> <output path>");
            System.exit(-1);
        }
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext("local", "MaxTemperatureSpark", conf);
        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaRDD<String[]> records = lines.map(line -> line.split("\t"));

        JavaRDD<String[]> filtered = records.filter(record -> (record[1] != "9999" && record[2].matches("[01459]")) );

        JavaPairRDD<Integer, Integer> tuple  = filtered.mapToPair(filter-> new Tuple2(Integer.parseInt(filter[0]), Integer.parseInt(filter[1])) );

        JavaPairRDD<Integer, Integer> maxTemps = tuple.reduceByKey( (i1,i2) -> Math.max(i1, i2));
        maxTemps.saveAsTextFile(args[1]);

    }
}