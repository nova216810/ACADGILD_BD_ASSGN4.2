
	
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public  class PS3Reducer extends Reducer<Text,Text,Text,IntWritable> 
{
	  private IntWritable units_sold = new IntWritable();
  	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	  {
	      int sum = 0;
	      for(Text val : values) 
		    {
	        sum ++;
	      }
	      units_sold.set(sum);
		    context.write(key, units_sold);
	   }
}
