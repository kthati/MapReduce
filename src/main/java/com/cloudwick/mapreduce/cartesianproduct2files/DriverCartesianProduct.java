package com.cloudwick.mapreduce.cartesianproduct2files;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DriverCartesianProduct {
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		long start = System.currentTimeMillis();
		JobConf conf = new JobConf("Cartesian Product");
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		 if (otherArgs.length != 3) {
		 System.err.println("Usage: CartesianProduct <comment data> <out>");
		 System.exit(1);
		 }

//		Path cartesianProductResult = new Path(
//				"/Users/KINNU/Documents/Java/java/MapReduce/CrossProduct1File/TESTCASE_RESULTS/output_ins_sce/");
//		FileSystem fs = FileSystem.get(conf);
//		fs.delete(cartesianProductResult, true);

		// Configure the join type
		conf.setJarByClass(DriverCartesianProduct.class);

		conf.setMapperClass(CartesianMapper.class);
		//conf.setReducerClass(CartesianReducer.class);

		//conf.setNumReduceTasks(0);

		conf.setInputFormat(CartesianInputFormat.class);
		 CartesianInputFormat.setLeftInputInfo(conf, TextInputFormat.class,
		 otherArgs[0]);
		 CartesianInputFormat.setRightInputInfo(conf, TextInputFormat.class,
		 otherArgs[1]);

//		CartesianInputFormat
//				.setLeftInputInfo(
//						conf,
//						TextInputFormat.class,
//						"/Users/KINNU/Documents/Java/java/MapReduce/CrossProduct1File/TESTCASE_RESULTS/instrument_data/");
//		CartesianInputFormat
//				.setRightInputInfo(
//						conf,
//						TextInputFormat.class,
//						"/Users/KINNU/Documents/Java/java/MapReduce/CrossProduct1File/TESTCASE_RESULTS/scenario_data/");

			FileSystem fs = FileSystem.get(conf);
			fs.delete(new Path(otherArgs[2]), true);

		 TextOutputFormat.setOutputPath(conf, new Path(otherArgs[2]));
//		TextOutputFormat.setOutputPath(conf, cartesianProductResult);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		RunningJob job = JobClient.runJob(conf);
		while (!job.isComplete()) {
			Thread.sleep(1000);
		}

		long finish = System.currentTimeMillis();

		System.out.println("Time in ms: " + (finish - start));

		System.exit(job.isSuccessful() ? 0 : 2);
	}

}
