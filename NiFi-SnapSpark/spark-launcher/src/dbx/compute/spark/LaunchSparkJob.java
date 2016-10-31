package dbx.compute.spark;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.spark.launcher.SparkLauncher;

public class LaunchSparkJob {
	private SparkLauncher spark;

	// Arguments
	String SparkHome; // -spark_home
	String AppName; // -name
	String AppResource; // -launch_jar
	String Master; // -master
	String DRIVER_MEMORY; // -driver_mem
	String EXECUTOR_CORES; // -exec_cores
	String EXECUTOR_MEMORY; // -exec_mem

	String Conf; // -conf
	String Config; // -config
	String DeployMode; // -deploy_mode
	String JavaHome; // -java_home
	String MainClass; // -class
	String PropertiesFile; // -properties_file

	String AppArgs; // -app_args
	String AppFile; // -app_file
	String AppJar; // -app_jar
	String AppPyFile; // -app_py_files
	String AppSparkArg; // -spark_arg

	boolean verbose = false; // -v

	public int launch(Collection retData, Collection errData, String _SparkHome, String _AppName, String _AppResource,
			String _MainClass, String _Master, String _DRIVER_MEMORY, String _EXECUTOR_CORES, String _EXECUTOR_MEMORY, String data)
			{

		try {
		SparkHome = _SparkHome;
		AppName = _AppName;
		AppResource = _AppResource;
		MainClass = _MainClass;
		Master = _Master;
		DRIVER_MEMORY = _DRIVER_MEMORY;
		EXECUTOR_CORES = _EXECUTOR_CORES;
		EXECUTOR_MEMORY = _EXECUTOR_MEMORY;

		if (SparkHome == null || AppName == null || AppResource == null || MainClass == null || Master == null
				|| DRIVER_MEMORY == null || EXECUTOR_CORES == null || EXECUTOR_MEMORY == null) {

			return -1;
		}

		// Set vars
		this.spark = new SparkLauncher();
		this.spark.setSparkHome(SparkHome); // "/Users/dbaez/LOCAL-DEV/spark/spark-2.0.0-bin-hadoop2.7");
		this.spark.setAppName(AppName);
		this.spark.setAppResource(AppResource); // "/Users/dbaez/DEV/SPARK/testSparkLaunch.jar");
		this.spark.setMainClass(MainClass); // "dbx.compute.spark.testSubmit");
		this.spark.setMaster(Master); // "local");
		this.spark.setConf(SparkLauncher.DRIVER_MEMORY, DRIVER_MEMORY); // SparkLauncher.DRIVER_MEMORY,
																		// "2g");
		this.spark.setConf(SparkLauncher.EXECUTOR_CORES, EXECUTOR_CORES);
		this.spark.setConf(SparkLauncher.EXECUTOR_MEMORY, EXECUTOR_MEMORY);

		// Data for App
		if (data != null)
			this.spark.addAppArgs(data);
		
		//---
		// Optional
		if (DeployMode != null)
			this.spark.setDeployMode(DeployMode);

		// *** if(Conf != null)

		// *** this.spark.setConfig(name, value); // do multiple...

		if (this.JavaHome != null)
			this.spark.setJavaHome(JavaHome);

		if(this.PropertiesFile != null)
			this.spark.setPropertiesFile(PropertiesFile);

//		if(this.AppArgs != null)
//			this.spark.addAppArgs(AppArgs);

		if(this.AppFile != null)
			this.spark.addFile(AppFile);

		if(this.AppJar != null)
			this.spark.addJar(AppJar);

		if(this.AppPyFile != null)
			this.spark.addPyFile(AppPyFile);

		if(this.AppSparkArg != null)
			this.spark.addSparkArg(AppSparkArg);

		this.spark.setVerbose(this.verbose);
		//---
		
		Process child = this.spark.launch();

		InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(child.getInputStream(),
				"spark-launcher", retData);
		Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
		inputThread.start();
		
		//----
		InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(child.getErrorStream(), "error", errData);
		Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
		errorThread.start();
		//----
		
		System.out.println("Waiting for finish...");
		int exitCode = child.waitFor();
		System.out.println("Finished! Exit code:" + exitCode);

		return exitCode;
		} catch (Exception e) {
			e.printStackTrace(System.err);
			return -1;
		}

	}

	public String[] launch(String[] args, Collection retData) {

		try {

			Options options = new Options();

			options.addOption("sh", "spark_home", true, "Sets Spark Home");
			options.addOption("nm", "name", true, "Set the application name");
			options.addOption("lj", "launch_jar", true, "Set the main application resource. Jar to launched");
			options.addOption("cl", "class", true, "Sets the application class name for Java/Scala applications");
			options.addOption("mt", "master", true, "Set the Spark master for the application.");
			options.addOption("dm", "driver_mem", true, "Set the driver memory");
			options.addOption("ec", "exec_cores", true, "Set the number of executor CPU cores");
			options.addOption("em", "exec_mem", true, "Set the the executor memory");

			options.addOption("mo", "deploy_mode", true, "Set the deploy mode for the application");
			options.addOption("cf", "conf", true, "Set a single configuration value for the application");
			options.addOption("cg", "config", true, "Set a configuration value for the launcher library");
			options.addOption("jh", "java_home", true, "Sets Java Home");
			options.addOption("pf", "properties_file", true,
					"Set a custom properties file with Spark configuration for the application");
			options.addOption("ag", "app_args", true, "Adds command line arguments for the application");
			options.addOption("af", "app_file", true, "Adds a file to be submitted with the application");
			options.addOption("aj", "app_jar", true, "Adds a jar file to be submitted with the application");
			options.addOption("ap", "app_py_files", true,
					"Adds a python file / zip / egg to be submitted with the application");
			options.addOption("sa", "spark_arg", true, "Adds an argument with a value to the Spark invocation");

			options.addOption("v", "verbose", false, "Verbose Output");

			CommandLineParser parser = new DefaultParser();
			CommandLine cmd = parser.parse(options, args);

			// Help
			if (cmd.hasOption("v")) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("sb server", options);
				return null;
			} else {
			}

			// Required
			SparkHome = cmd.getOptionValue("sh"); // -spark_home
			AppName = cmd.getOptionValue("nm"); // -name
			AppResource = cmd.getOptionValue("lj"); // -launch_jar
			MainClass = cmd.getOptionValue("cl"); // -class
			Master = cmd.getOptionValue("mt"); // -master
			DRIVER_MEMORY = cmd.getOptionValue("dm"); // -driver_mem
			EXECUTOR_CORES = cmd.getOptionValue("ec"); // -exec_cores
			EXECUTOR_MEMORY = cmd.getOptionValue("em"); // -exec_mem

			if (SparkHome == null || AppName == null || AppResource == null || MainClass == null || Master == null
					|| DRIVER_MEMORY == null || EXECUTOR_CORES == null || EXECUTOR_MEMORY == null) {

				return null;
			}
			
			this.spark = new SparkLauncher();
			this.spark.setSparkHome(SparkHome); // "/Users/dbaez/LOCAL-DEV/spark/spark-2.0.0-bin-hadoop2.7");
			this.spark.setAppName(AppName);
			this.spark.setAppResource(AppResource); // "/Users/dbaez/DEV/SPARK/testSparkLaunch.jar");
			this.spark.setMainClass(MainClass); // "dbx.compute.spark.testSubmit");
			this.spark.setMaster(Master); // "local");
			this.spark.setConf(SparkLauncher.DRIVER_MEMORY, DRIVER_MEMORY); // SparkLauncher.DRIVER_MEMORY,
																			// "2g");
			this.spark.setConf(SparkLauncher.EXECUTOR_CORES, EXECUTOR_CORES);
			this.spark.setConf(SparkLauncher.EXECUTOR_MEMORY, EXECUTOR_MEMORY);

			System.out.println(
					"LAUNCHING:"
					+"[SparkHome = "+SparkHome+"]"
					+"[AppName = "+AppName+"]"
					+"[AppResource = "+AppResource+"]"
					+"[MainClass = "+MainClass+"]"
					+"[Master = "+Master+"]"
					+"[DRIVER_MEMORY = "+DRIVER_MEMORY+"]"
					+"[EXECUTOR_CORES = "+EXECUTOR_CORES+"]"
					+"[EXECUTOR_MEMORY = "+EXECUTOR_MEMORY+"]"
					);

			// Optional
			DeployMode = cmd.getOptionValue("mo"); // -deploy_mode
			if (DeployMode != null)
				this.spark.setDeployMode(DeployMode);

			Conf = cmd.getOptionValue("cf"); // -conf ******
			// *** if(Conf != null)

			Config = cmd.getOptionValue("cg"); // -config ***
			// *** this.spark.setConfig(name, value); // do multiple...

			JavaHome = cmd.getOptionValue("jh"); // -java_home
			if (JavaHome != null)
				this.spark.setJavaHome(JavaHome);

			PropertiesFile = cmd.getOptionValue("pf"); // -properties_file
			if (PropertiesFile != null)
				this.spark.setPropertiesFile(PropertiesFile);

			AppArgs = cmd.getOptionValue("ag"); // -app_args
			if (AppArgs != null)
				this.spark.addAppArgs(AppArgs);

			AppFile = cmd.getOptionValue("af"); // -app_file
			if (AppFile != null)
				this.spark.addFile(AppFile);

			AppJar = cmd.getOptionValue("aj"); // -app_jar
			if (AppJar != null)
				this.spark.addJar(AppJar);

			AppPyFile = cmd.getOptionValue("ap"); // -app_py_files
			if (AppPyFile != null)
				this.spark.addPyFile(AppPyFile);

			AppSparkArg = cmd.getOptionValue("sa"); // -spark_arg
			if (AppSparkArg != null)
				this.spark.addSparkArg(AppSparkArg);

			if (cmd.hasOption("v"))
				this.spark.setVerbose(true);
			
			
			// launch!
			Process child = this.spark.launch();
			InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(child.getInputStream(),
					"spark-launcher", retData);
			Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
			inputThread.start();

			// Wait until it is done
			System.out.println("Waiting for finish...");
			int exitCode = child.waitFor();
			System.out.println("Finished! Exit code:" + exitCode);

			return null;
		} catch (Exception e) {
			e.printStackTrace(System.err);
			return null;
		}
	}

	public SparkLauncher getSpark() {
		return spark;
	}

	public void setSpark(SparkLauncher spark) {
		this.spark = spark;
	}

	public String getSparkHome() {
		return SparkHome;
	}

	public void setSparkHome(String sparkHome) {
		SparkHome = sparkHome;
	}

	public String getAppName() {
		return AppName;
	}

	public void setAppName(String appName) {
		AppName = appName;
	}

	public String getAppResource() {
		return AppResource;
	}

	public void setAppResource(String appResource) {
		AppResource = appResource;
	}

	public String getMaster() {
		return Master;
	}

	public void setMaster(String master) {
		Master = master;
	}

	public String getDRIVER_MEMORY() {
		return DRIVER_MEMORY;
	}

	public void setDRIVER_MEMORY(String dRIVER_MEMORY) {
		DRIVER_MEMORY = dRIVER_MEMORY;
	}

	public String getEXECUTOR_CORES() {
		return EXECUTOR_CORES;
	}

	public void setEXECUTOR_CORES(String eXECUTOR_CORES) {
		EXECUTOR_CORES = eXECUTOR_CORES;
	}

	public String getEXECUTOR_MEMORY() {
		return EXECUTOR_MEMORY;
	}

	public void setEXECUTOR_MEMORY(String eXECUTOR_MEMORY) {
		EXECUTOR_MEMORY = eXECUTOR_MEMORY;
	}

	public String getConf() {
		return Conf;
	}

	public void setConf(String conf) {
		Conf = conf;
	}

	public String getConfig() {
		return Config;
	}

	public void setConfig(String config) {
		Config = config;
	}

	public String getDeployMode() {
		return DeployMode;
	}

	public void setDeployMode(String deployMode) {
		DeployMode = deployMode;
	}

	public String getJavaHome() {
		return JavaHome;
	}

	public void setJavaHome(String javaHome) {
		JavaHome = javaHome;
	}

	public String getMainClass() {
		return MainClass;
	}

	public void setMainClass(String mainClass) {
		MainClass = mainClass;
	}

	public String getPropertiesFile() {
		return PropertiesFile;
	}

	public void setPropertiesFile(String propertiesFile) {
		PropertiesFile = propertiesFile;
	}

	public String getAppArgs() {
		return AppArgs;
	}

	public void setAppArgs(String appArgs) {
		AppArgs = appArgs;
	}

	public String getAppFile() {
		return AppFile;
	}

	public void setAppFile(String appFile) {
		AppFile = appFile;
	}

	public String getAppJar() {
		return AppJar;
	}

	public void setAppJar(String appJar) {
		AppJar = appJar;
	}

	public String getAppPyFile() {
		return AppPyFile;
	}

	public void setAppPyFile(String appPyFile) {
		AppPyFile = appPyFile;
	}

	public String getAppSparkArg() {
		return AppSparkArg;
	}

	public void setAppSparkArg(String appSparkArg) {
		AppSparkArg = appSparkArg;
	}

	public boolean isVerbose() {
		return verbose;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

}
