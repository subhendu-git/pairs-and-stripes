package pairs;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PairsReducer extends Reducer<WordPair, DoubleWritable, Text, DoubleWritable> {
	
	private DoubleWritable totalCount = new DoubleWritable();
	private DoubleWritable relativeFrequency = new DoubleWritable();
	private Text currentWord = new Text("NA");
	private Text wildCard = new Text("*");
	
	protected void reduce(WordPair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
		if(key.getNeighbour().equals(wildCard)){
			if(key.getWord().equals(currentWord)){
				totalCount.set(totalCount.get() + getTotalCount(values));
			}
			else{
				currentWord.set(key.getWord());
				totalCount.set(0);
				totalCount.set(getTotalCount(values));
			}
		}
		else{
			int count = getTotalCount(values);
			relativeFrequency.set((double)count/totalCount.get());
			context.write(key.toText(), relativeFrequency);
		}
	}
	
	private int getTotalCount(Iterable<DoubleWritable> values){
		int count = 0;
		for(DoubleWritable value : values){
			count += value.get();
		}
		return count;
	}
}
