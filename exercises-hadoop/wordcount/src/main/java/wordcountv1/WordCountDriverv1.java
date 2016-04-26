package wordcountv1;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriverv1 {
	public static void main(String args[]) {
		if (args.length != 2) {
			System.err
					.println("Usage: WordCountDriverv1 InputDirectory OutputDirectory");
			System.exit(2);
		}

		File inputDir = new File(args[0]);
		if (!inputDir.exists() || !inputDir.isDirectory()) {
			System.err.println("Invalid Input Directory");
		}
		File outputDir = new File(args[1]);
		if (outputDir.exists()) {
			outputDir.delete();
		}

		try {
			Job job = new Job();
			job.setJarByClass(WordCountDriverv1.class);
			job.setMapperClass(WordCountMapperv1.class);
			job.setReducerClass(WordCountReducerv1.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
