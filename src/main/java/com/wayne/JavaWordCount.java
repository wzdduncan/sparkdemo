package com.wayne;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.elasticsearch.spark.rdd.api.java.JavaEsSpark.esRDD;

public class JavaWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    private Logger logger = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) throws Exception {
        new JavaWordCount().readEs();
        new JavaWordCount().writeEs();
    }


    public void writeEs() {
        String elasticIndex = "spark/docs";
        //https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html#spark-native
        SparkConf sparkConf = new SparkConf().setAppName("writeEs").setMaster("local[*]").set("es.index.auto.create", "true")
                .set("es.nodes", "192.168.33.10").set("es.port", "30901").set("es.nodes.wan.only", "true");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());//adapter
        Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2);
        Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");
        JavaRDD<Map<String, ?>> javaRDD = jsc.parallelize(ImmutableList.of(numbers, airports));
        JavaEsSpark.saveToEs(javaRDD, elasticIndex);
    }

    public void readEs() {
        SparkConf sparkConf = new SparkConf().setAppName("writeEs").setMaster("local[*]").set("es.index.auto.create", "true")
                .set("es.nodes", "192.168.33.10").set("es.port", "30901").set("es.nodes.wan.only", "true");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());//adapter
        System.out.println("执行chaxun");
        logger.info("执行查询");
        JavaRDD<Map<String, Object>> searchRdd = esRDD(jsc, "index_ik_test/fulltext").values();
        for (Map<String, Object> item : searchRdd.collect()) {
            item.forEach((key, value)->{
                System.out.println("search key:" + key + ", search value:" + value);
                logger.info("search key:{}, search value:{}",key,value);
            });
        }
        sparkSession.stop();
    }

    public void JavaWordCount(){
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();
//        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
        JavaRDD<String> lines = spark.read().textFile("/sparkdata").javaRDD();
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        spark.stop();
    }
}
