package stripes;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripesCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {
	
	private MapWritable result = new MapWritable();
	
	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
		for(MapWritable value : values){
			for(Map.Entry<Writable,Writable> entry : value.entrySet()){
				result.put(entry.getKey(), entry.getValue());
			}
		}
		context.write(key, result);
		result.clear();
	}
}
