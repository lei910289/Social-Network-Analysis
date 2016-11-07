import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class Q3 {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private Text key = new Text();
		private Text val = new Text();
		HashMap<String, String> map = new HashMap<String, String>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int val1 = Integer.parseInt(context.getConfiguration().get("user1"));
			int val2 = Integer.parseInt(context.getConfiguration().get("user2"));

			// swap
			if(val1 > val2){
				int temp = val1;
				val1 = val2;
				val2 = temp;
			}
			
			String[] data = value.toString().split("	");
			if(data.length == 1) return;
			String dataKey = data[0];
			String dataVal = data[1];
			
			
			StringBuilder sb = new StringBuilder();

			String[] vArrs = dataVal.split(",");
			String space = "";
			for(String s : vArrs){
				sb.append(space + map.get(s));
				space = ",";
			}

			String vPrime = sb.toString();
			val.set(vPrime);

			for(String s : vArrs){
				int first = Integer.valueOf(dataKey);
				int second = Integer.valueOf(s);
				if(first > second){
					int temp = first;
					first = second;
					second = temp;
				}

				if(val1 == first && val2 == second){
					StringBuilder sb2 = new StringBuilder();
					sb2.append(first).append(",").append(second);
					key.set(sb2.toString());
					
					context.write(key,val);
				}
			
				
			}
		}
		
		public void setup(Context context) throws IOException, InterruptedException{
			super.setup(context);
			Configuration config = context.getConfiguration();
			String userdata = config.get("userdata");
			Path path = new Path(userdata);
			FileSystem fileSystem = path.getFileSystem(config);
			FileStatus[] fileStatus = fileSystem.listStatus(path);
			for(FileStatus status : fileStatus){
				Path p = status.getPath();
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(p)));
				String line = bufferedReader.readLine();
				while(line != null){
					String[] arr = line.split(",");
					map.put(arr[0], arr[1]  + ":" + arr[arr.length - 1]);
					line = bufferedReader.readLine();
				}
			}
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private Text output = new Text();
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

			
			ArrayList<ArrayList<String>> common = new ArrayList<ArrayList<String>>();
			
			for (Text val : value) {
				
				String[] vArrs = val.toString().split(",");
				ArrayList<String> temp = new ArrayList(Arrays.asList(vArrs));
				common.add(temp);
				
			}
			
			ArrayList<String> first = common.get(0);
			ArrayList<String> second = common.get(1);
			
			first.retainAll(second);
			
			StringBuilder sb = new StringBuilder();
			String space = "";
			for (int i = 0; i < first.size(); i++) {
				sb.append(first.get(i));
				if (i != first.size() - 1) {
					sb.append(",");
				}
			}
			if(sb.toString().isEmpty()) return;
			
			output = new Text(sb.toString());
			context.write(key, output);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		String[] attrs = new GenericOptionsParser(config, args).getRemainingArgs();
		String user1 = args[2];
		String user2 = args[3];
		String userdata = args[4];

		config.set("user1", args[2]);
		config.set("user2", args[3]);
		config.set("userdata", args[4]);
		
		if (attrs.length != 5) {
			System.err.println("Usage: InMemoryJoin <in> <out>");
			System.exit(2);
		}
		Job job = new Job(config, "InMemoryJoin");
		job.setJarByClass(Q3.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(attrs[0]));
		FileOutputFormat.setOutputPath(job, new Path(attrs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}