import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q1 {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
            String temp = value.toString();
            String[] inputs = temp.split("	");
			if(inputs.length == 1){

			}else{
				for(String s:inputs[1].split(",")){
					int id1=Integer.parseInt(inputs[0]);
					int id2=Integer.parseInt(s);
					String output = "";
					if(id1<id2){
						output=id1+","+id2;
					}else{
						output=id2+","+id1;
					}
					context.write(new Text(output), new Text(inputs[1]));;
				}
			}
		}
	}
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			ArrayList<ArrayList<String>> wrap=new ArrayList<>();
			for(Text t:values){
				String[] friends=t.toString().split(",");
				ArrayList<String> temp = new ArrayList(Arrays.asList(friends));
				wrap.add(temp);
			}
			ArrayList<String> com = wrap.get(0);
			com.retainAll(wrap.get(1));
			String[] k=key.toString().split(",");
			for(String temp:k){
				com.remove(temp);
			}
			StringBuilder sb=new StringBuilder();
			for(String str:com){
				sb.append(str + ",");
			}
			if(sb.length != 0){
				context.write(key, new Text(sb.toString()));
			}
		}
	}

}
