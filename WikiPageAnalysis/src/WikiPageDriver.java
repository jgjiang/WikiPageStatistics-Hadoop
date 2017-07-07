import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikiPageDriver extends Configured implements Tool{

	private static FileSystem fs; 
	private static Configuration conf;
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
		conf = new Configuration();
		
		// implement the runner tool
		int exitCode = ToolRunner.run(conf, new WikiPageDriver(), args);  
		System.exit(exitCode);
        
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		
		// Configuration conf = this.getConf();
		
		/*if(args.length != 2){  
        *  System.err.println("Usage:WikiPageCount <input> <output>");  
        *  System.exit(-1); }  
        *
        */
		
		// configure 1st job
		Job pageCount = Job.getInstance();
        pageCount.setJarByClass(WikiPageDriver.class);
        pageCount.setJobName("PageCount");
        
        pageCount.setMapperClass(PageCountMapper.class);
        pageCount.setReducerClass(PageCountReducer.class);
        pageCount.setOutputKeyClass(Text.class);
        pageCount.setOutputValueClass(LongWritable.class);
        
        FileInputFormat.addInputPath(pageCount, new Path(args[0]));
        FileOutputFormat.setOutputPath(pageCount, new Path(args[1]));
       
        pageCount.waitForCompletion(true);
        
        
        // configure 2nd job
        Job pageSort = Job.getInstance();
        pageSort.setJarByClass(WikiPageDriver.class);
        pageSort.setJobName("PageSort");
        
        pageSort.setMapperClass(PageSortMapper.class);
        pageSort.setReducerClass(PageSortReducer.class);
        pageSort.setSortComparatorClass(SortComparator.class);
        
        pageSort.setMapOutputKeyClass(LongWritable.class);  
        pageSort.setMapOutputValueClass(Text.class);
        
        pageSort.setOutputKeyClass(Text.class);
        pageSort.setOutputValueClass(LongWritable.class);
      
        FileInputFormat.addInputPath(pageSort, new Path(args[1]));
        FileOutputFormat.setOutputPath(pageSort, new Path(args[2]));
       
        pageSort.waitForCompletion(true);
        
        // configure 3rd job
        Job pageAvg = Job.getInstance();
        pageAvg.setJarByClass(WikiPageDriver.class);
        pageAvg.setJobName("PageAverage");
        
        pageAvg.setMapperClass(PageAverageMapper.class);
        pageAvg.setReducerClass(PageAverageReducer.class);
        
        pageAvg.setMapOutputKeyClass(Text.class);  
        pageAvg.setMapOutputValueClass(LongWritable.class);
        
        pageAvg.setOutputKeyClass(Text.class);
        pageAvg.setOutputValueClass(LongWritable.class);
        
        
        FileInputFormat.addInputPath(pageAvg, new Path(args[2]));
        FileOutputFormat.setOutputPath(pageAvg, new Path(args[3]));
        
        pageAvg.waitForCompletion(true);
        
        
        // configure 4th job
        Job pageAvgSort = Job.getInstance();
        pageAvgSort.setJarByClass(WikiPageDriver.class);
        pageAvgSort.setJobName("PageAvgSort");
        
        pageAvgSort.setMapperClass(PageAvgSortMapper.class);
        pageAvgSort.setReducerClass(PageAvgSortReducer.class);
        pageAvgSort.setSortComparatorClass(AvgSortComparator.class);
        
        pageAvgSort.setMapOutputKeyClass(DoubleWritable.class);  
        pageAvgSort.setMapOutputValueClass(Text.class);
        
        pageAvgSort.setOutputKeyClass(Text.class);
        pageAvgSort.setOutputValueClass(DoubleWritable.class);
      
        FileInputFormat.addInputPath(pageAvgSort, new Path(args[3]));
        FileOutputFormat.setOutputPath(pageAvgSort, new Path(args[4]));
       
        pageAvgSort.waitForCompletion(true);
        
        return 1; 
	}
	
	public static class PageCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			// split the line based on whitespace matching
			String[] line = value.toString().trim().split("\\s+");
			
			
			if(line.length == 4) { // only consider valid data tuples (length == 4) and eliminate invalid ones
				String projectName = line[0];
				String pageCount = line[2];
				Text pName = new Text(projectName);
				Long pCount = Long.parseLong(pageCount);
				
				//output writing
				context.write(pName, new LongWritable(pCount));
			} 
			
		}

	}
	
	public static class PageCountReducer extends Reducer <Text, LongWritable, Text, LongWritable> {

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			long sum = 0;
			
			// for each project, calculate the total number of pageview 
			for (LongWritable value : values) {
		            sum += value.get();
		    }
		        
		    //output writing
		    context.write(key, new LongWritable(sum));
		}
		
	}
	
	public static class PageSortMapper extends Mapper<LongWritable,Text,LongWritable,Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// super.map(key, value, context);
			
			// convert pageCountReducerOutput into String value and split it based on whitespace matching
			String[] pageCountReducerOutput  = value.toString().trim().split("\\s+");
			
			String projectName = pageCountReducerOutput[0];
			String pageCount = pageCountReducerOutput[1];
			
			// data types change
			Text pName = new Text(projectName);
			Long pCount = Long.parseLong(pageCount);
			
			// reverse key,value of pageCountReducerOutput
			// output writing
			context.write(new LongWritable(pCount), pName);	
		}
		
	}
	
	public static class PageSortReducer extends Reducer<LongWritable,Text,Text,LongWritable>{

		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Reducer<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// super.reduce(arg0, arg1, arg2);
			
			for(Text value:values){
				//output writing
				context.write(value, key);
			}
		}  
           
	}
	
	public static class SortComparator extends WritableComparator {

		protected SortComparator() {
			super(LongWritable.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			// return super.compare(a, b);
			
			LongWritable mapOutputKey1 = (LongWritable) a;
			LongWritable mapOutputKey2 = (LongWritable) b;
			
			// sort the results based on mapOutputKey 
		    int compareValue = mapOutputKey1.compareTo(mapOutputKey2);
		    
		    // sort by descending order (negative number for descending order)
		    return -1*compareValue;
		}
		
	}
	
	public static class PageAverageMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		private String langName;
		private String pageCount;
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// super.map(key, value, context);
			
			
			String[] sortedResultLine = value.toString().trim().split("\\s+");
			
			if (sortedResultLine.length == 2){
				String projectName = sortedResultLine[0];
				if(projectName.contains(".")){
					String langNames []= projectName.trim().split("\\.");
					langName = langNames[0];
				} else {
					langName = projectName;
				}
				
				pageCount = sortedResultLine[1];
			}
			
			Text langNameText = new Text(langName);
			Long pageCountLong = Long.parseLong(pageCount);
			
			context.write(langNameText, new LongWritable(pageCountLong));
		}
			
	}
	
	
	
	public static class PageAverageReducer extends Reducer <Text, LongWritable, Text, DoubleWritable>{

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// super.reduce(arg0, arg1, arg2);
			long sum = 0;
			double avgNum = 0;
			
			for(LongWritable value : values){
				sum += value.get();
			}
		
			// obtain FileSystem object
			fs = FileSystem.get(conf);
			
			// obtain wikipage directory path in hdfs
			Path pt = new Path("hdfs://127.0.1.1:9000/user/hadoop/wikipage");
			
			// obtain contents from wikipage file directory
			ContentSummary cs = fs.getContentSummary(pt);
			
			// get the number of files (we have 6 files in wikipage)
			long fileCount = cs.getFileCount();
			
			avgNum = sum/fileCount;
			context.write(key, new DoubleWritable(avgNum));
		}
		
	}
	
	
	public static class PageAvgSortMapper extends Mapper<LongWritable,Text,DoubleWritable,Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, DoubleWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// super.map(key, value, context);
			
			// convert pageAvgReducerOutput into String value and split it based on whitespace matching
			String[] pageAvgReducerOutput  = value.toString().trim().split("\\s+");
			
			String langName = pageAvgReducerOutput[0];
			String pageCount = pageAvgReducerOutput[1];
			
			// data types change
			Text lName = new Text(langName);
			Double pCount = Double.parseDouble(pageCount);
			
			// reverse key,value of pageAvgReducerOutput
			// output writing
			context.write(new DoubleWritable(pCount), lName);	
		}
	}
	
	public static class PageAvgSortReducer extends Reducer<DoubleWritable,Text,Text,DoubleWritable>{

		@Override
		protected void reduce(DoubleWritable key, Iterable<Text> values,
				Reducer<DoubleWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// super.reduce(arg0, arg1, arg2);
			
			for(Text value:values){
				//output writing
				context.write(value, key);
			}
		}  
           
	}
	
	public static class AvgSortComparator extends WritableComparator {

	
		protected AvgSortComparator() {
			super(DoubleWritable.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			// return super.compare(a, b);
			
			DoubleWritable mapOutputKey1 = (DoubleWritable) a;
			DoubleWritable mapOutputKey2 = (DoubleWritable) b;
			
			// sort the results based on mapOutputKey 
		    int compareValue = mapOutputKey1.compareTo(mapOutputKey2);
		    
		    // sort by descending order (negative number for descending order)
		    return -1*compareValue;
		}
		
	}

}
