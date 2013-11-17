package co.uk.dataissexy.xd.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TwitterHashtagMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String cleanedLine = line.replaceAll(
				"[\\\\\"!@?%^&*:;'|\\/.,<>]\\(\\)+=", " ");
		StringTokenizer tokenizer = new StringTokenizer(cleanedLine);

		while (tokenizer.hasMoreTokens()) {
			String thisToken = tokenizer.nextToken();
			if (thisToken.startsWith("#")) {
				if(thisToken.substring(1).contains("#")){
					String[] extras = thisToken.substring(1).split("#");
					for(int i = 0 ; i < extras.length; i++){
						context.write(new Text("#" + thisToken), new IntWritable(1));
					}
				} else {
					context.write(new Text(thisToken), new IntWritable(1));
				}
			}
		}
	}
}
