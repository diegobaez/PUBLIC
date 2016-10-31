package dbx.compute.spark.jobs;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;

public class PiCalculator {

	public static void main(String[] args) {

		int NUM_SAMPLES;
		try{
			if(args[0] != null)
				NUM_SAMPLES=Integer.parseInt(args[0].trim());
			else
				NUM_SAMPLES=1000;
			}
		catch(Exception e){
			NUM_SAMPLES=1000;
		}
		
		if(NUM_SAMPLES>1000) NUM_SAMPLES=1000;
		
	    SparkConf conf = new SparkConf().setAppName("Pi Application");
	    JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> l = new ArrayList<Integer>(NUM_SAMPLES);
		for (int i = 0; i < NUM_SAMPLES; i++) {
		  l.add(i);
		}

		long count = sc.parallelize(l).filter(new Function<Integer, Boolean>() {
		  public Boolean call(Integer i) {
		    double x = Math.random();
		    double y = Math.random();
		    return x*x + y*y < 1;
		  }
		}).count();
		System.out.println("Pi["+NUM_SAMPLES+"] is roughly " + 4.0 * count / NUM_SAMPLES);
	}

}
