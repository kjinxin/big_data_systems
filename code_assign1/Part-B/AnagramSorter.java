import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.WritableComparable;
//import org.apache.hadoop.io.WritableComparator;

public class AnagramSorter{
	//Map class
	public static class Map extends MapReduceBase implements 
	Mapper<LongWritable, Text, Text, Text>
	{
		//Map function
		public void map(LongWritable key, Text value,
		OutputCollector<Text, Text>output, Reporter reporter) throws IOException
		{
			String word=value.toString();
			char[] wordCharArray=word.toCharArray();
			Arrays.sort(wordCharArray);
			String wordSorted=new String(wordCharArray);
			output.collect(new Text(wordSorted), new Text(word));
		}
	}

	//Reduce class
	public static class Reduce extends MapReduceBase implements
	Reducer<Text, Text, IntWritable, Text>
	{
		//Reduce by Anagrams
		public void reduce(Text key, Iterator<Text> values,
		OutputCollector<IntWritable, Text>output, Reporter reporter) throws IOException
		{
			int sum=0;
			String collection=new String();
			while(values.hasNext())
			{
				Text val=values.next();
				collection=collection+val.toString()+' ';
				sum=sum+1;
			}
			output.collect(new IntWritable(sum), new Text(collection));
		}
	}
	/*
	public static class KeyComparator extends WritableComparator
	{
		public KeyComparator()
		{
			super(IntWritable.class, true);
		}
		@Override
		public int compare(WritableComparable w1, WritableComparable w2)
		{
			IntWritable k1 = (IntWritable) w1;
          		IntWritable k2 = (IntWritable) w2;
          		return  -1*k1.compareTo(k2);
		}
	}
	*/
	public static class Map2 extends MapReduceBase implements
        Mapper<LongWritable, Text, LongWritable, Text>
        {
                //Map function
                public void map(LongWritable key, Text value,
                OutputCollector<LongWritable, Text>output, Reporter reporter) throws IOException
                {
			String S=value.toString();
			int tab=S.indexOf('\t');
			int actual_key=Integer.parseInt(S.substring(0,tab));
			String actual_string=S.substring(tab+1);
                        output.collect(new LongWritable(actual_key), new Text(actual_string));
                }
        }

	//Reduce class
        public static class Reduce2 extends MapReduceBase implements
        Reducer<LongWritable, Text, LongWritable, Text>
        {
                //Reduce by Anagrams
                public void reduce(LongWritable key, Iterator<Text> values,
                OutputCollector<LongWritable, Text>output, Reporter reporter) throws IOException
                {
                        while(values.hasNext())
			{
                        	output.collect(null, values.next());
			}
                }
        }

	//main function
	public static void main(String[] args) throws Exception
	{
		JobConf conf = new JobConf(AnagramSorter.class);
		conf.setJobName("Anagram");
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path("/tempoutput"));
		JobClient.runJob(conf);

		JobConf conf2 = new JobConf(AnagramSorter.class);
		conf2.setJobName("Sort");
                conf2.setOutputKeyClass(LongWritable.class);
                conf2.setOutputValueClass(Text.class);
                conf2.setMapOutputKeyClass(LongWritable.class);
                conf2.setMapOutputValueClass(Text.class);
                conf2.setMapperClass(Map2.class);
                conf2.setOutputKeyComparatorClass(LongWritable.DecreasingComparator.class);
                //conf.setCombinerClass(Reduce.class);
                conf2.setReducerClass(Reduce2.class);
                conf2.setInputFormat(TextInputFormat.class);
                conf2.setOutputFormat(TextOutputFormat.class);
                FileInputFormat.setInputPaths(conf2, new Path("/tempoutput"));
                FileOutputFormat.setOutputPath(conf2, new Path(args[1]));
                JobClient.runJob(conf2);
	}
}
	
