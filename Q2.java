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

public class Q2 {
	public static class Map
	extends Mapper<Text, Text, Text, Text>{
		public void map(Text key, Text value, Context context
				) throws IOException, InterruptedException {
			String temp = value.toString();
			String inputs = temp.split("	");
			Configuration config = context.getConfiguration();
			String str1 = config.get("u1");
			String str2 =config.get("u2");
			if(inputs.length==1){

			}else{
				if(inputs[0].equals(str1)||inputs[0].equals(str2)){
					for(String s:inputs[1].split(",")){
						int id1=Integer.parseInt(inputs[0]);
						int id2=Integer.parseInt(s);
						String output="";
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
	}
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		private Text rst = new Text();

		public void reduce(Text key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			ArrayList<ArrayList<String>> wrap=new ArrayList<>();
			for(Text t :values){
				String[] friends=t.toString().split(",");
				ArrayList<String> temp = new ArrayList<>(Arrays.asList(friends);
				wrap.add(temp);
			}
			ArrayList<String> com=new ArrayList<>();
			if(wrap.size()==2){
				com = wrap.get(0);
				com.retainAll(wrap.get(1));
			}
			String[] k=key.toString().split(",");
			for(String temp:k){
				com.remove(temp);
			}
			if(com.size()>0){
				StringBuilder sb=new StringBuilder();
				for(String s:com){
					sb.append(s + ",");
				}
				rst.set(sb.toString());
				context.write(key, rst);
			}
		}
	}
	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		config.set("u1", args[2]);
		config.set("u2", args[3]);
		String[] attrs = new GenericOptionsParser(config, args).getRemainingArgs();
		if (attrs.length != 4) {
			System.err.println("Usage: Q2 <in> <out> <u1> <u2>");
			System.exit(2);
		}
		Job job = new Job(config, "Q2");
		job.setJarByClass(Q2.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(attrs[0]));
		FileOutputFormat.setOutputPath(job, new Path(attrs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
