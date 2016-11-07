package q4;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Q4 {
    
    static public class Map extends Mapper<LongWritable, Text, Text, Text> {
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String temp = value.toString();
            String[] mydata = temp.split("	");
            String user = mydata[0].trim();
            
            if(mydata.length == 2){
                String[] friends = mydata[1].split(",");
                for(int i = 0; i < friends.length; i++){
                    // get the friends list
                    // write the pair of id and its friends list as array
                    context.write(new Text(user), new Text(friends[i]));
                }
            }
        }
    }
    
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private HashMap<String, String> addr;
        private HashMap<String, String> birth;
        private HashMap<String, Double> age = new HashMap<String, Double>();
        
        public void setup(Context context) throws IOException, InterruptedException{
            
            addr = new HashMap<String, String>();
            birth = new HashMap<String, String>();
            Path path=new Path(context.getConfiguration().get("userdata"));
            
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
            // read from buffer reader
            String line=br.readLine();
            while (line != null){
                String[] details = line.split(",");
                // map the id to address
                addr.put(details[0], details[3]+","+details[4]+","+details[5]);
                
                String birthDate = details[9];
                DateFormat format = new SimpleDateFormat("MM/dd/yyyy");
                Date birthDay;
                
                try {
                    birthDay = format.parse(birthDate);
                    Calendar dob = Calendar.getInstance();
                    dob.setTime(birthDay);
                    Calendar today = Calendar.getInstance();
                    int age = today.get(Calendar.YEAR) - dob.get(Calendar.YEAR);
                    // map the id to age
                    birth.put(details[0], age + "");
                    
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                line=br.readLine();
            }
            br.close();
        }
        
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            
            List<String> friendAge = new ArrayList<String>();
            String user = key.toString();
            for (Text val : values) {
                // for every friend
                friendAge.add(birth.get(val.toString()));
            }
            
            double maxAge = 0.0;
            if(friendAge.size()!=0){
                for (String str : friendAge) {
                    maxAge = Math.max(maxAge, Double.parseDouble(str));
                }
            }
            age.put(user+","+addr.get(user), maxAge);
        }
        
        
        protected void cleanup(Context context) throws IOException, InterruptedException {
            
            List<java.util.Map.Entry<String, Double>> resList = new ArrayList<>(age.entrySet());
            Collections.sort(resList,new Comparator<java.util.Map.Entry<String, Double>>()
                             {
                                 public int compare(java.util.Map.Entry<String, Double> e1, java.util.Map.Entry<String, Double> e2) {
                                     return e2.getValue().compareTo(e1.getValue());
                                 }
                             });
            
            int count = 0;
            for(java.util.Map.Entry<String, Double> entry: resList){
                if(count<10){
                    String outkey = entry.getKey();
                    String outAge = entry.getValue()+"";
                    context.write(new Text(outkey), new Text(outAge));
                    count++;
                }
                else break;
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        String[] attrs = new GenericOptionsParser(config,args).getRemainingArgs();
        
        if (attrs.length != 3) {
            System.err.println("Usage: RecommandFriends <in> <out>");
            System.exit(2);
        }
        
        String userData = attrs[2];
        config.set("userdata", userData);
        
        Job job = new Job (config, "Question4");
        job.setJarByClass(Q4.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(attrs[0]));
        FileOutputFormat.setOutputPath(job, new Path(attrs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
