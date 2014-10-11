package pairs;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PairsPartitioner extends Partitioner<WordPair, DoubleWritable> {

	@Override
	public int getPartition(WordPair arg0, DoubleWritable arg1, int numPartitions) {
		return Math.abs(arg0.getWord().hashCode()) % numPartitions;
	}

}
