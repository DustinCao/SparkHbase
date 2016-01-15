package com.infobird.spark.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.amazonaws.services.ec2.model.Filter;

public class TestSpark {

	public static void testSpark() {
	
		SparkConf sparkConf = new SparkConf().setAppName("TestSpark");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> rdd = sparkContext.textFile("/user/test/data");
		
		System.out.println("the file's lines:" + rdd.count());
		
		System.out.println("The first line:" + rdd.first());
		
		
		//rdd.filter(new Fu);
	}
}
