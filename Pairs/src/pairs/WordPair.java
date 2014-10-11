package pairs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordPair implements WritableComparable<WordPair> {
	
	private Text word;
	private Text neighbour;
	
	public WordPair(){
		word = new Text();
		neighbour = new Text();
	}
	
	public WordPair(Text word, Text neighbour){
		this.word = word;
		this.neighbour = neighbour;
	}
	
	public void setWord(String str){
		word.set(str);
	}
	
	public void setNeighbour(String str){
		neighbour.set(str);
	}
	
	public Text getWord(){
		return word;
	}
	
	public Text getNeighbour(){
		return neighbour;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		if(word == null){
			word = new Text();
		}
		if(neighbour == null){
			neighbour = new Text();
		}
		
		word.readFields(arg0);
		neighbour.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		word.write(arg0);
		neighbour.write(arg0);
	}

	@Override
	public int compareTo(WordPair arg0) {
		int val = this.getWord().compareTo(arg0.getWord());
		if(val != 0){
			return val;
		}
		if(this.getNeighbour().toString().equals("*")){
			return -1;
		}
		else if(arg0.getNeighbour().toString().equals("*")){
			return -1;
		}
		return this.getNeighbour().compareTo(arg0.getNeighbour());
	}
	
	
	public int hashCode(){
		return Math.abs(word.hashCode()*31 + neighbour.hashCode());
	}
	
	public boolean equals(WordPair arg0){
		return this.word.equals(arg0.getWord()) && this.neighbour.equals(arg0.getNeighbour());
	}
	
	public Text toText(){
		Text t = new Text("["+word.toString()+","+neighbour.toString()+"]");
		return t;
	}

}
