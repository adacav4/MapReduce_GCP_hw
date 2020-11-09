import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser; 

public class Driver { 

	public static void main(String[] args) throws Exception { 
		
		Configuration conf = new Configuration(); 
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); 

		if (otherArgs.length < 4) { 
			System.err.println("Error: please provide 4 paths"); 
			System.exit(2); 
		} 

		Job job = Job.getInstance(conf, "least 5"); 
		
		// Set the Main class
		job.setJarByClass(Driver.class); 

		// Set Mapper and Reducer classes
		job.setMapperClass(least_5_Mapper.class); 
		job.setReducerClass(least_5_Reducer.class); 

		// Set the output class types for Mapper
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(LongWritable.class); 

		// Set the output class types for Reducer
		job.setOutputKeyClass(LongWritable.class); 
		job.setOutputValueClass(Text.class); 	
		
		// Set number of reducers to 1 (feed in all mappers for each input file into one reducer)
        job.setNumReduceTasks(1);
        
		// Get the input file paths, the output file path
		Path in1 = new Path(args[0]);
		Path in2 = new Path(args[1]);
		Path in3 = new Path(args[2]);
		Path out = new Path(args[3]);
		
		MultipleInputs.addInputPath(job, in1, TextInputFormat.class, least_5_Mapper.class);
		MultipleInputs.addInputPath(job, in2, TextInputFormat.class, least_5_Mapper.class);
		MultipleInputs.addInputPath(job, in3, TextInputFormat.class, least_5_Mapper.class);
		FileOutputFormat.setOutputPath(job, out);

		System.exit(job.waitForCompletion(true) ? 0 : 1); 
	} 
} 
