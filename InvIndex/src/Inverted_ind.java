import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Inverted_ind {

	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration();
		Job job = new Job(conf,"Invertedindex");
		//Defining the output key and value class for the mapper
		job.setJarByClass(Inverted_ind.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		//Defining the output value class for the mapper
		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/home/skrj/inverted_index.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/home/skrj/dataop_invertd"));
		
		 job.waitForCompletion(true);
	}

public static class Map extends Mapper<LongWritable, Text, Text, Text> {

    private Text wordText = new Text();
    private final static Text document = new Text();

    protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        String[] line = value.toString().split("=");

        String documentName = line[0];
        document.set(documentName);
        String textStr = line[1];
        String[] wordArray = textStr.split(" ");
        for(int i = 0; i <  wordArray.length; i++) {
            wordText.set(wordArray[i]);
            context.write(wordText,document);
        }
    }
}

public static class Reduce extends
Reducer<Text, Text, Text, Text> {
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws java.io.IOException, InterruptedException {
        StringBuffer buffer = new StringBuffer();
        for (Text value : values) {
            if(buffer.length() != 0) {
                buffer.append(" ");
            }
            buffer.append(value.toString());
        }
        Text documentList = new Text();
        documentList.set(buffer.toString());
        context.write(key, documentList);
    }
}
}

