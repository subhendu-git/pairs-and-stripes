package stripes;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StripesMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
	
	private MapWritable freqMap = new MapWritable();
	private Text word = new Text();
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		int neighbours = 2;
		String[] tokens = value.toString().split("\\s+");
		int numOfTokens = tokens.length;
		if(numOfTokens>1){
			for(int i=0; i<numOfTokens;i++){
				tokens[i] = tokens[i].replaceAll("\\W+", "");
				word.set(tokens[i]);
				freqMap.clear();
				
				int start = (i - neighbours)<0 ? 0 : (i - neighbours);
				int end = (i + neighbours) >= numOfTokens ? (numOfTokens - 1) : (i + neighbours);
				for(int j=start; j<=end; j++){
					if(i!=j){
						Text neighbour = new Text(tokens[j].replaceAll("\\W+", ""));
						if(freqMap.containsKey(neighbour)){
							DoubleWritable count = (DoubleWritable)freqMap.get(neighbour);
							count.set(count.get()+1);
						}
						else{
							freqMap.put(neighbour, new DoubleWritable(1));
						}
					}
				}
				context.write(word, freqMap);
			}
		}
	}
}
