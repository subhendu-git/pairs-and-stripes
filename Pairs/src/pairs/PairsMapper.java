package pairs;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PairsMapper extends Mapper<LongWritable, Text, WordPair, DoubleWritable> {
	private WordPair wordPair = new WordPair();
	private DoubleWritable one = new DoubleWritable(1);
	private DoubleWritable totalCount = new DoubleWritable();
	
		
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		int neighbours = 2;
		
		String[] tokens = value.toString().split("\\s+");
		int numOfTokens = tokens.length;
		if(numOfTokens>1){
			for(int i = 0; i<tokens.length; i++){
				tokens[i] = tokens[i].replaceAll("\\W+", "");
				
				if(!tokens[i].equals("")){
					wordPair.setWord(tokens[i]);
					
					int start = (i - neighbours) < 0 ? 0 : (i - neighbours);
					int end = (i + neighbours) >= numOfTokens ? (numOfTokens - 1) : (i + neighbours);
					for(int j = start; j<=end;j++){
						if(j!=i){
							wordPair.setNeighbour(tokens[j].replaceAll("\\W+", ""));
							context.write(wordPair, one);
						}
					}
					wordPair.setNeighbour("*");
					totalCount.set(end - start);
					context.write(wordPair, totalCount);
					
				}
			}
		}
	}
}
