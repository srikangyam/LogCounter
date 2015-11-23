package com.skangyam.hadoop.mapreduce.LogCounter;

	import org.apache.commons.logging.Log;
	import org.apache.commons.logging.LogFactory;
	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.conf.Configured;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	import org.apache.hadoop.util.Tool;
	import org.apache.hadoop.util.ToolRunner;

	public class LogCounterDriver extends Configured implements Tool {
		private final Log LOG = LogFactory.getLog(LogCounterDriver.class);

		public int run(String[] args) throws Exception {
			if (args.length != 2) {
				System.out.printf("Usage: %s [generic options] "
						+ "<Log File Input Path> <Output Path>\n", getClass()
						.getSimpleName());
				return -1;
			}

			// Configuration
			Configuration conf = new Configuration();

			// Job definition
			Job job = Job.getInstance(conf);
			job.setJarByClass(LogCounterDriver.class);
			job.setJobName("DEGUG LOG COUNTER");

			// Mapper Details
			job.setMapperClass(CounterMap.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			// Reducer Details
			job.setNumReduceTasks(0);

			// Configuration of Output paths on HDFS
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			// Start the job
			job.waitForCompletion(true);

			// Final Counter Readings
			LOG.info("INFO COUNT: " + job.getCounters().findCounter(DEBUG_COUNTER.INFO).getValue());
			LOG.info("WARNING COUNT: "+ job.getCounters().findCounter(DEBUG_COUNTER.WARNING).getValue());
			LOG.info("ERROR COUNT: " + job.getCounters().findCounter(DEBUG_COUNTER.ERROR).getValue());

			return 0;
		}

		public static void main(String... args) throws Exception {
			int exitCode = ToolRunner.run(new LogCounterDriver(), args);
			System.exit(exitCode);
		}
	}
