import java.io.*; 
import java.util.*; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.mapreduce.Mapper; 

public class least_5_Mapper extends Mapper<Object, Text, Text, LongWritable> { 

	private TreeMap<Long, String> tmap;	

	@Override
	public void setup(Context context) throws IOException, InterruptedException { 
		tmap = new TreeMap<Long, String>();
	} 

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
		
		// Split up the data from the file
		String[] tokens = value.toString().split("\t"); 

		// Get the movie name and the number of views it has for key, value pair
		String movie_name = tokens[0]; 
		long no_of_views = Long.parseLong(tokens[1]); 

		// Add the key, value pair to the TreeMap
		tmap.put(no_of_views, movie_name); 

		// When the size of the map is N+1, remove the highest element to ensure we are getting the least N entries
		if (tmap.size() > 5) { 
			tmap.remove(tmap.lastKey()); 
		}
	} 

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException { 
		
		// Writes the key, value pairs for the reducer 
		for (Map.Entry<Long, String> entry : tmap.entrySet()) { 
			long count = entry.getKey(); 
			String name = entry.getValue(); 

			context.write(new Text(name), new LongWritable(count)); 
		} 
	} 
} 
