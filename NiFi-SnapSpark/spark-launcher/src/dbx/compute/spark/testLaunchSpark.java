package dbx.compute.spark;

import java.util.ArrayList;

public class testLaunchSpark {

	static public void main(String[] args) {

		// -sh /Users/dbaez/LOCAL-DEV/spark/spark-2.0.0-bin-hadoop2.7
		// -nm testLauncher
		// -lj
		// /Users/dbaez/GDRIVE/DEV-2016/SPARK-LAUNCHER/target/SPARK-LAUNCHER-0.0.1-SNAPSHOT.jar
		// -cl dbx.compute.spark.EquityRiskCalculation
		// -mt local
		// -dm 2g
		// -ec 2
		// -em 4G
		// -app_args "hey|malparido"

		String SparkHome = "/Users/dbaez/LOCAL-DEV/spark/spark-2.0.0-bin-hadoop2.7";
		String AppName = "Test Spark Launcher";
		String AppResource = "/Users/dbaez/DEV/WORK/nifi-SnapSpark/spark-job/target/spark-job-0.0.1.jar";
		String Master = "local";
		String driver_memory = "2g";
		String executor_cores = "4";
		String executor_memory = "4G";

		LaunchSparkJob launcher = new LaunchSparkJob();

		// Test EquityRiskCalculation
		String MainClass = "dbx.compute.spark.jobs.EquityRiskCalculation";
		String data = "1|2|3|4|5|6|7";
		System.out.println("========= " + MainClass + "[" + data + "]");
		ArrayList<String> retData = new ArrayList<String>();
		ArrayList<String> errData = new ArrayList<String>();

		int ret = launcher.launch(retData, errData, SparkHome, AppName, AppResource, MainClass, Master, driver_memory,
				executor_cores, executor_memory, data);
		System.out.println("RET: " + ret);
		if (ret == 0) {
			for (int i = 0; i < retData.size(); i++) {
				System.out.println(retData.get(i));
			}
		} else {
			for (int i = 0; i < errData.size(); i++) {
				System.out.println(retData.get(i));
			}
		}

		System.out.println("---------------------------------------");

		// Test JavaHdfsLR
		MainClass = "dbx.compute.spark.jobs.JavaHdfsLR";
		data = "/Users/dbaez/LOCAL-DEV/spark/spark-2.0.0-bin-hadoop2.7/data/mllib/lr_data.txt|5";
		System.out.println("========= " + MainClass + "[" + data + "]");
		retData = new ArrayList<String>();
		errData = new ArrayList<String>();

		ret = launcher.launch(retData, errData, SparkHome, AppName, AppResource, MainClass, Master, driver_memory,
				executor_cores, executor_memory, data);
		System.out.println("RET: " + ret);
		if (ret == 0) {
			for (int i = 0; i < retData.size(); i++) {
				System.out.println(retData.get(i));
			}
		} else {
			for (int i = 0; i < errData.size(); i++) {
				System.out.println(retData.get(i));
			}
		}
		System.out.println("---------------------------------------");

		// Test JavaHdfsLR
		MainClass = "dbx.compute.spark.jobs.OutputStreamReaderRunnable";
		data = "/Users/dbaez/LOCAL-DEV/spark/spark-2.0.0-bin-hadoop2.7/data/mllib/lr_data.txt|5";
		System.out.println("========= " + MainClass + "[" + data + "]");
		retData = new ArrayList<String>();
		errData = new ArrayList<String>();

		ret = launcher.launch(retData, errData, SparkHome, AppName, AppResource, MainClass, Master, driver_memory,
				executor_cores, executor_memory, data);
		System.out.println("RET: " + ret);
		if (ret == 0) {
			for (int i = 0; i < retData.size(); i++) {
				System.out.println(retData.get(i));
			}
		} else {
			for (int i = 0; i < errData.size(); i++) {
				System.out.println(errData.get(i));
			}
		}
		System.out.println("---------------------------------------");

	}

}
