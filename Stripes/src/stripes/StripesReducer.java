package stripes;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripesReducer extends Reducer<Text, MapWritable, Text, DoubleWritable> {

	private MapWritable freqMap = new MapWritable();
	private double count = 0;
	
	protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
		
		count = 0;
		
		for(MapWritable value : values){
			for(Map.Entry<Writable,Writable> entry : value.entrySet()){
				freqMap.put(entry.getKey(), entry.getValue());
				count += Double.parseDouble(entry.getValue().toString());
			}
		}
		
		for(Map.Entry<Writable, Writable> entry : freqMap.entrySet()){
			double occurence = Double.parseDouble(entry.getValue().toString());
			context.write(new Text("["+key.toString()+","+entry.getKey().toString()+"]"), new DoubleWritable(occurence/count));
		}
		freqMap.clear();
	}
	
}
