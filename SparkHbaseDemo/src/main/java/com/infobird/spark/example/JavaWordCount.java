package com.infobird.spark.example;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class JavaWordCount {

	private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void main(String[] args) {
		
		if (args.length < 1) {
			System.out.println("Usage : JavaWordCount <file>" );
			System.exit(1);
		}
		
		SparkConf sparkConf = new SparkConf().setAppName("SparkWordCount");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> rdd = sparkContext.textFile(args[0], 1);
		
		JavaRDD<String> words = rdd.flatMap(new FlatMapFunction<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String t) throws Exception {
				return Arrays.asList(SPACE.split(t));
			}
		});
		
		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {

				return new Tuple2<String, Integer>(t, 1);
			}
		});
		
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<String, Integer> tuple2 : output) {
			System.out.println(tuple2._1() + ":" + tuple2._2());
		}
		
		sparkContext.stop();
		sparkContext.close();
	}
}
