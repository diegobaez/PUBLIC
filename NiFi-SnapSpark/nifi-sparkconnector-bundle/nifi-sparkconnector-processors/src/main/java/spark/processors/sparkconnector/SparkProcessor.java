/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package spark.processors.sparkconnector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import dbx.compute.spark.LaunchSparkJob;

@Tags({ "SPARK", "SPARK LAUNCH" })
@CapabilityDescription("Used to launch a Spark Job, and return the results")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })

public class SparkProcessor extends AbstractProcessor {

	static int flag = 0; // testing purposes
	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;

	// ====== Properties ======
	// SparkHome = cmd.getOptionValue("sh"); // -spark_home
	public static final PropertyDescriptor SPARK_HOME = new PropertyDescriptor.Builder().name("Spark Home")
			.description("SPARK_HOME environment variable").required(true).defaultValue("")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	// AppName = cmd.getOptionValue("nm"); // -name
	public static final PropertyDescriptor APP_NAME = new PropertyDescriptor.Builder().name("Application Name")
			.description("Set the application name").required(true).defaultValue("")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	// AppResource = cmd.getOptionValue("lj"); // -launch_jar
	public static final PropertyDescriptor JAR_PROCESSOR = new PropertyDescriptor.Builder()
			.name("JAR File to Submit to Spark")
			.description("JAR FIle containing the processing to be submitted to Spark").required(true).defaultValue("")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	// MainClass = cmd.getOptionValue("cl"); // -class
	public static final PropertyDescriptor ENTRY_POINT = new PropertyDescriptor.Builder().name("Class Entry Point")
			.description("Main Class in the JAR to submit to Spark").required(true).defaultValue("")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	// Master = cmd.getOptionValue("mt"); // -master
	public static final PropertyDescriptor MASTER = new PropertyDescriptor.Builder().name("Master")
			.description("Set the Spark master for the application").required(true).defaultValue("local")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	// DRIVER_MEMORY = cmd.getOptionValue("dm"); // -driver_mem
	public static final PropertyDescriptor DRIVER_MEMORY = new PropertyDescriptor.Builder().name("Driver Memory")
			.description("Driver Memory").required(true).defaultValue("")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	// EXECUTOR_CORES = cmd.getOptionValue("ec"); // -exec_cores
	public static final PropertyDescriptor EXECUTOR_CORES = new PropertyDescriptor.Builder().name("Executor CPU cores")
			.description("Set the number of executor CPU cores").required(true).defaultValue("")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	// EXECUTOR_MEMORY = cmd.getOptionValue("em"); // -exec_mem
	public static final PropertyDescriptor EXECUTOR_MEMORY = new PropertyDescriptor.Builder().name("Executor memory")
			.description("Set the the executor memory").required(true).defaultValue("")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	// ====== Relationships ======
	public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder().name("SUCCESS")
			.description("Results in Flowfile").build();

	public static final Relationship FAIL_RELATIONSHIP = new Relationship.Builder().name("FAILURE")
			.description("Check return attribute for error code").build();

	// ====== Init ======
	@Override
	protected void init(final ProcessorInitializationContext context) {

		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(SPARK_HOME);
		descriptors.add(APP_NAME);
		descriptors.add(JAR_PROCESSOR);
		descriptors.add(ENTRY_POINT);
		descriptors.add(MASTER);
		descriptors.add(DRIVER_MEMORY);
		descriptors.add(EXECUTOR_CORES);
		descriptors.add(EXECUTOR_MEMORY);

		// descriptors.add(DATA); // *** USE FLOWFILE

		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(FAIL_RELATIONSHIP);
		relationships.add(SUCCESS_RELATIONSHIP);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {

	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}
		getLogger().info("Got a flow file");

		// InputStream flowdata = session.read(flowFile);
		try {
			// String data = IOUtils.toString(flowdata).trim();
			byte[] data = readContent(session, flowFile);

			getLogger().info("data_file[" + data + "]");
			// log Data

			String SparkHome = context.getProperty(SPARK_HOME).getValue();
			String AppName = context.getProperty(APP_NAME).getValue();
			String AppResource = context.getProperty(JAR_PROCESSOR).getValue();
			String MainClass = context.getProperty(ENTRY_POINT).getValue();
			String Master = context.getProperty(MASTER).getValue();
			String driver_memory = context.getProperty(DRIVER_MEMORY).getValue();
			String executor_cores = context.getProperty(EXECUTOR_CORES).getValue();
			String executor_memory = context.getProperty(EXECUTOR_MEMORY).getValue();

			if (SparkHome == null || SparkHome.isEmpty()) {
				getLogger().error("Empty or Null " + SPARK_HOME.getDisplayName() + " Property");
				flowFile = session.putAttribute(flowFile, "return",
						"Empty or Null " + SPARK_HOME.getDisplayName() + " Property");
				session.transfer(flowFile, FAIL_RELATIONSHIP);
				return;
			}
			if (AppName == null || AppName.isEmpty()) {
				getLogger().error("Empty or Null " + APP_NAME.getDisplayName() + " Property");
				flowFile = session.putAttribute(flowFile, "return",
						"Empty or Null " + APP_NAME.getDisplayName() + " Property");
				session.transfer(flowFile, FAIL_RELATIONSHIP);
				return;
			}
			if (AppResource == null || AppResource.isEmpty()) {
				getLogger().error("Empty or Null " + JAR_PROCESSOR.getDisplayName() + " Property");
				flowFile = session.putAttribute(flowFile, "return",
						"Empty or Null " + JAR_PROCESSOR.getDisplayName() + " Property");
				session.transfer(flowFile, FAIL_RELATIONSHIP);
				return;
			}
			if (MainClass == null || MainClass.isEmpty()) {
				getLogger().error("Empty or Null " + ENTRY_POINT.getDisplayName() + " Property");
				flowFile = session.putAttribute(flowFile, "return",
						"Empty or Null " + ENTRY_POINT.getDisplayName() + " Property");
				session.transfer(flowFile, FAIL_RELATIONSHIP);
				return;
			}
			if (Master == null || Master.isEmpty()) {
				getLogger().error("Empty or Null " + MASTER.getDisplayName() + " Property");
				flowFile = session.putAttribute(flowFile, "return",
						"Empty or Null " + MASTER.getDisplayName() + " Property");
				session.transfer(flowFile, FAIL_RELATIONSHIP);
				return;
			}
			if (driver_memory == null || driver_memory.isEmpty()) {
				getLogger().error("Empty or Null " + DRIVER_MEMORY.getDisplayName() + " Property");
				flowFile = session.putAttribute(flowFile, "return",
						"Empty or Null " + DRIVER_MEMORY.getDisplayName() + " Property");
				session.transfer(flowFile, FAIL_RELATIONSHIP);
				return;
			}
			if (executor_cores == null || executor_cores.isEmpty()) {
				getLogger().error("Empty or Null " + EXECUTOR_CORES.getDisplayName() + " Property");
				flowFile = session.putAttribute(flowFile, "return",
						"Empty or Null " + EXECUTOR_CORES.getDisplayName() + " Property");
				session.transfer(flowFile, FAIL_RELATIONSHIP);
				return;
			}
			if (executor_memory == null || executor_memory.isEmpty()) {
				getLogger().error("Empty or Null " + EXECUTOR_MEMORY.getDisplayName() + " Property");
				flowFile = session.putAttribute(flowFile, "return",
						"Empty or Null " + EXECUTOR_MEMORY.getDisplayName() + " Property");
				session.transfer(flowFile, FAIL_RELATIONSHIP);
				return;
			}

			getLogger().info("Launching Spark Job..." + "[SparkHome=" + SparkHome + "]" + "[AppName=" + AppName + "]"
					+ "[AppResource=" + AppResource + "]" + "[MainClass=" + MainClass + "]" + "[Master=" + Master + "]"
					+ "[driver_memory=" + driver_memory + "]" + "[executor_cores=" + executor_cores + "]"
					+ "[executor_memory=" + executor_memory + "]" + "[data=" + new String(data) + "]");

			LaunchSparkJob launcher = new LaunchSparkJob();
			ArrayList<String> retData = new ArrayList<String>();
			ArrayList<String> errData = new ArrayList<String>();

			int ret = launcher.launch(retData, errData, SparkHome, AppName, AppResource, MainClass, Master,
					driver_memory, executor_cores, executor_memory, new String(data));

			flowFile = session.putAttribute(flowFile, "return", "Return: " + ret);

			if (ret == 0) {
				getLogger().info("SPARK Returned PROCESSING Results = [");
				// Print Results to SAME FLOWFILE - Overwrite
				flowFile = session.write(flowFile, new OutputStreamCallback() {

					public void process(OutputStream out) throws IOException {
						for (int i = 0; i < retData.size() - 1; i++) {
							out.write(retData.get(i).getBytes());
							out.write("\n".getBytes());
							getLogger().info(retData.get(i));
						}
						getLogger().info("]");
					}
				});
				getLogger().info("SUCCESS...");
				session.transfer(flowFile, SUCCESS_RELATIONSHIP);
			} else {
				StringBuffer errmsg = new StringBuffer();

				getLogger().error("Spark Launch Failed: code[" +ret+"]:[");
				// Print error to SAME FLOWFILE - Overwrite
				flowFile = session.write(flowFile, new OutputStreamCallback() {

					public void process(OutputStream out) throws IOException {
						for (int i = 0; i < errData.size() - 1; i++) {
							out.write(errData.get(i).getBytes());
							out.write("\n".getBytes());
							getLogger().error(errData.get(i));
							errmsg.append(errData.get(i)+"\n");
						}
						getLogger().error("]");
					}
				});

				getLogger().info("FAILURE..." + "Spark Launch Failed: code[" +ret+"]: "+errmsg);
				session.transfer(flowFile, FAIL_RELATIONSHIP);
			}

		} catch (

		Exception ioe) {
			getLogger().error(ioe.getLocalizedMessage());
			getLogger().error(ioe.getStackTrace().toString());
			getLogger().info("FAILURE...");
			session.transfer(flowFile, FAIL_RELATIONSHIP);
		}
	}

	/**
	 * Helper method to read the FlowFile content stream into a byte array.
	 *
	 * @param session
	 *            - the current process session.
	 * @param flowFile
	 *            - the FlowFile to read the content from.
	 *
	 * @return byte array representation of the FlowFile content.
	 */
	protected byte[] readContent(final ProcessSession session, final FlowFile flowFile) {
		final ByteArrayOutputStream baos = new ByteArrayOutputStream((int) flowFile.getSize() + 1);
		session.read(flowFile, new InputStreamCallback() {
			@Override
			public void process(final InputStream in) throws IOException {
				StreamUtils.copy(in, baos);
			}
		});

		return baos.toByteArray();
	}

}
