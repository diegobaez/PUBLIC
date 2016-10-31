package dbx.compute.spark.jobs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class WordCount {

	public static void main(String[] args) {
	    String logFile = args[0]; // Should be some file on your system
	    SparkConf conf = new SparkConf().setAppName("Simple Application");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    //JavaRDD<String> logData = sc.textFile(logFile).cache();
	    JavaRDD<String> logData = sc.textFile(logFile.trim()).cache();

	    long numAs = logData.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("a"); }
	    }).count();

	    long numBs = logData.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("b"); }
	    }).count();

	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	  }
}
