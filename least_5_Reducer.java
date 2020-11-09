import java.io.IOException; 
import java.util.Map; 
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer; 

public class least_5_Reducer extends Reducer<Text, LongWritable, LongWritable, Text> { 

	private TreeMap<Long, String> tmap2; 

	@Override
	public void setup(Context context) throws IOException, InterruptedException { 
		tmap2 = new TreeMap<Long, String>(); 
	} 

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException { 
		
		// Get the key, value pair
		String name = key.toString(); 
		long count = 0; 

		// Get the count for the specific key
		for (LongWritable val : values) { 
			count = val.get(); 
		} 

		// Add the key, value pair to the TreeMap
		tmap2.put(count, name); 
		
		// When the size of the map is N+1, remove the highest element to ensure we are getting the least N entries
		if (tmap2.size() > 5) { 
			tmap2.remove(tmap2.lastKey()); 
		} 
	} 

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException { 
		
		// Writes the key, value pairs for output
		for (Map.Entry<Long, String> entry : tmap2.entrySet()) { 
			long count = entry.getKey(); 
			String name = entry.getValue(); 
			
			context.write(new LongWritable(count), new Text(name)); 
		} 
	} 
} 
