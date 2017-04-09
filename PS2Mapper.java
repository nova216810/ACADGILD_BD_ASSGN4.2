
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public  class PS2Mapper extends Mapper<LongWritable, Text, Text, Text>
{
	private Text company = new Text();
	private Text product = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
	      String[] token = value.toString().split("\\|");
    		int flag=0;
				if (token[0].equals("NA") || token[1].equals("NA"))
			  {
				  flag = 1;
				}
			
			   if (flag == 0)
			   {
				    company .set(token[0]);
				    product.set(token[1]);
				    context.write(company,product);
				}
	 } 
}
	    
	
	  
