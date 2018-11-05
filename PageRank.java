// Nirvi Parikh, nparikh4@uncc.edu, 801022018

package pagerank;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends Configured implements Tool {

	private static final String N = "N";
	private static int isConverged=0;

	public static void main( String[] args) throws  Exception {
		int res  = ToolRunner.run( new PageRank(), args);
		System .exit(res);
	}

	public int run( String[] args) throws  Exception {

		FileSystem fs = FileSystem.get(getConf());
		long noOfDocs;

		// To calculate the number of documents
		Job jobGetCount  = Job.getInstance(getConf(), "pagerank");
		jobGetCount.setJarByClass( this.getClass());
		FileInputFormat.addInputPaths(jobGetCount,  args[0]);
		FileOutputFormat.setOutputPath(jobGetCount,  new Path(args[1]+ "getCount"));
		jobGetCount.setMapperClass( MapGetCount.class);
		jobGetCount.setReducerClass( ReduceGetCount.class);
		jobGetCount.setMapOutputValueClass(IntWritable.class);
		jobGetCount.setOutputKeyClass( Text.class);
		jobGetCount.setOutputValueClass(IntWritable.class);
		jobGetCount.waitForCompletion(true);
		fs.delete(new Path(args[1]+"getCount"), true);

		//To obtain the value of N, computed from first job
		noOfDocs = jobGetCount.getCounters().findCounter("N", "N").getValue();

		// To assign an initial page rank
		Job jobSetup  = Job.getInstance(getConf(), "pagerank");
		jobSetup.setJarByClass( this.getClass());
		FileInputFormat.addInputPaths(jobSetup,  args[0]);
		FileOutputFormat.setOutputPath(jobSetup,  new Path(args[1]+"0"));
		jobSetup.getConfiguration().setStrings(N, noOfDocs + "");
		jobSetup.setMapperClass( MapSetup.class);
		jobSetup.setReducerClass( ReduceSetup.class);
		jobSetup.setOutputKeyClass( Text.class);
		jobSetup.setOutputValueClass( Text.class);
		jobSetup.waitForCompletion(true);

		 int iter=1;
		 Job jobPR  = Job.getInstance(getConf(), "pagerank");

		//To calculate PageRank of each page
		//We will be using the outlinks

		//Iterating the same procedure till it converges
		while(iter<=10){
			if(iter>1){
				isConverged=1;
				jobPR  = Job.getInstance(getConf(), "pagerank");
			}

			jobPR.setJarByClass( this.getClass());
			FileInputFormat.addInputPaths(jobPR,  args[1]+(iter-1));
			FileOutputFormat.setOutputPath(jobPR, new Path(args[1]+iter));
			jobPR.setMapperClass( MapPageRank.class);
			jobPR.setReducerClass( ReducePageRank.class);
			jobPR.setOutputKeyClass( Text.class);
			jobPR.setOutputValueClass( Text.class);

			jobPR.waitForCompletion(true);

			fs.delete(new Path(args[1]+(iter-1)), true);

			if(iter>1){
				isConverged=(int) jobPR.getCounters().findCounter("convergence", "convergence").getValue();
				//Deciding whether the Page Ranks have been converged or not
				//Obtaining its value from counters
			}
			iter++;
		}
		//For sorting the urls based on Page Ranks
		Job jobSort  = Job.getInstance(getConf(), "pagerank");
		jobSort.setJarByClass( this.getClass());
		FileInputFormat.addInputPaths(jobSort,  args[1]+(iter-1));
		FileOutputFormat.setOutputPath(jobSort,  new Path(args[1]));
		jobSort.setMapperClass( MapSort.class);
		jobSort.setReducerClass( ReduceSort.class);
		jobSort.setMapOutputKeyClass(Text.class);
		jobSort.setMapOutputValueClass(Text.class);
		jobSort.setOutputKeyClass( Text.class);
		jobSort.setOutputValueClass( Text.class);
		jobSort.waitForCompletion(true);
		fs.delete(new Path(args[1]+(iter-1)), true);
		isConverged = (int) jobGetCount.getCounters().findCounter("convergence", "convergence").getValue();

		return 1;
	}

	//Mapper class
	public static class MapGetCount extends
	Mapper<LongWritable, Text, Text, IntWritable> {
		private static final Pattern titlePattern = Pattern
				.compile("<title>(.*?)</title>");

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String line = lineText.toString();
			if (line != null && !line.isEmpty()) {
				Text docTitle = new Text();

				Matcher titleMatcher = titlePattern.matcher(line);
				 //Comparing each line to check if it contains the title

				if (titleMatcher.find()) {
					docTitle = new Text(titleMatcher.group(1).trim());
					context.write(new Text(docTitle), new IntWritable(1));
					//If the title is present in the line then it is sent to the reducer
				}
			}
		}
	}

	//Reducer class
	public static class ReduceGetCount extends
	Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> counts,
				Context context) throws IOException,
				InterruptedException {

			context.getCounter("N", "N").increment(1); //increments the N counter for calculating the N for each title or key
		}
	}

	//Mapper class
	//Calculateing the initial Page Rank
	public static class MapSetup extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		private static final Pattern titlePattern = Pattern.compile("<title>(.*?)</title>"); //pattern to check the title
		private static final Pattern textPattern = Pattern.compile("<text(.*?)</text>");
		private static final Pattern linkPattern = Pattern .compile("\\[\\[(.*?)\\]\\]"); //pattern to check the outlinks

		double noOfDocs;

		public void setup(Context context) throws IOException, InterruptedException{
			noOfDocs = context.getConfiguration().getDouble(N, 1); //gets the n value from the configuration
		}


		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String line  = lineText.toString();
			if(line != null && !line.isEmpty() ){

				Text docTitle  = new Text();
				Text docLinks  = new Text();
				String links = null;
				String textLine = null;
				Matcher titleMatcher = titlePattern.matcher(line);
				Matcher textMatcher = textPattern.matcher(line);
				if(titleMatcher.find()){
					docTitle  = new Text(titleMatcher.group(1).trim());
		    	  }
				Matcher linksMatcher = null;
				while(textMatcher.find()){
					textLine  = textMatcher.group(1).trim();
					linksMatcher = linkPattern.matcher(textLine);
					double initialPageRank = (double)1/(noOfDocs);
					//Calculating the initial page rank
					StringBuilder stringBuilder = new StringBuilder("!!##"+initialPageRank+"!!##");
					int flag=0;

					while(linksMatcher != null && linksMatcher.find()){
						links=linksMatcher.group().replace("[[", "").replace("]]", "");
						if(flag==1){
							stringBuilder.append("!@#");
							//Adding the delimiter for differentiating between the two outlinks
						}
						stringBuilder.append(links.trim());
						flag=1;
					}
				docLinks = new Text(stringBuilder.toString().trim());
				context.write(docTitle,docLinks);
				}
			}
		}
	}
//Reducer Class
//To write the initial setup to the file for the further jobs
	public static class ReduceSetup extends Reducer<Text ,  Text ,  Text ,  Text > {
		public void reduce( Text word,  Text counts,  Context context)
				throws IOException,  InterruptedException {
			context.write(word,counts);
		}
	}
	//Main Mapper class
	public static class MapPageRank extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String line  = lineText.toString().trim();
			String[] lineSplit=line.split("!!##");
			//Spliting into initial rank and outlinks

			float currentRank =Float.parseFloat(lineSplit[1]); //Initial page rank
			String docTitle = lineSplit[0].trim(); //Title of the page


			if(lineSplit.length<3){
				context.write(new Text(docTitle),new Text(lineSplit[1]+"@@!@@!@@"+" "));
			}
			else if(lineSplit.length==3){

				String[] linksList=lineSplit[2].split("!@#");
				float temp=currentRank/linksList.length;
				 //calculating the rank or the number of outlinks value

				for(int u=0;u<linksList.length;u++){
					context.write(new Text(linksList[u].trim()+""), new Text(temp+"!!##val##!!"));
					//Writing the url and rank/outlinks value corresponding to each outlink
				}
				if(docTitle!=null){
					context.write(new Text(docTitle),new Text(lineSplit[1]+"@@!@@!@@"+lineSplit[2].trim()+""));
					 //Writing the url and its outlinks for reducer
				}
			}
		}
	}

	//Main Reducer
	public static class ReducePageRank extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			super.setup(context);
			//Initializing the counter
			context.getCounter("convergence", "convergence").setValue(1);
		}

		@Override
		public void reduce( Text word,  Iterable<Text > counts,  Context context)
				throws IOException,  InterruptedException {

			String docTitle=word.toString().trim();
			float newRank=0.0f;
			float val=0.0f;
			float prevRank=0.0f;
			String links=null;


				for(Text value:counts){

					String keyval = value.toString().trim();

					if(keyval.contains("!!##val##!!")){
						keyval=keyval.replace("!!##val##!!","");
						val=val+Float.parseFloat(keyval.toString());
						//Calculating the sum of Page Ranks divided by their outlinks for the current page
					}

					else{
						String[] tempout= value.toString().trim().split("@@!@@!@@");
						if(tempout[0].length()>0){
							prevRank=Float.parseFloat(tempout[0].trim());
							if(tempout.length==2){
								links = tempout[1].trim();
							}
							else{
								links = " ";
							}
						}
					}
				}

				if(links!=null){
					if(links.length()>0){
						newRank=(float) ((val*0.85)+0.15);
						//Calculating the page rank as per the damping factor i.e., 0.85
						float diff=prevRank-newRank;
						//Calculating the difference for the new Page Rank
						diff = Math.abs(diff);
						if(diff>0.001){
							//Iterating
							context.getCounter("maxDiff", "maxDiff").setValue(0);
						}
						context.write(new Text(docTitle), new Text("!!##"+newRank+"!!##"+links) );
				}
			}
		}
	}

	//Mapper class to sort the final Page Ranks obtained
	public static class MapSort extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String line  = lineText.toString().trim();
			String[] vals=line.split("!!##");
			double rank= Double.parseDouble(vals[1]);
			context.write(new Text("Ranking"), new Text(vals[0].trim()+"!!##"+rank));
			//Accessing tfidf and file names by this string.
			//Combining the file name with Page Rank.
			//Passing this to reducer. Note: their is a delimeter between file name and pag rank.
		}
	}
	//Reducer class for sorting the final page ranks
	public static class ReduceSort extends
	Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> counts,
				Context context) throws IOException, InterruptedException {

			HashMap<String,Double> ranks = new HashMap<String, Double>();

			for (Text count : counts){

				String[] urlRank=count.toString().split("!!##");
				//Spliting the value into the file name and its combined pagerank
				String url = urlRank[0].trim();
				double pagerank = Double.parseDouble(urlRank[1]);
				ranks.put(url, pagerank);
				// Puting the file name and tfidf in hash map in form of key.
		  	}

			List<Map.Entry<String, Double> > list =
		         new LinkedList<Map.Entry<String, Double> >(ranks.entrySet());
		        // Sorting the list
		        Collections.sort(list, new Comparator<Map.Entry<String, Double> >(){
		            public int compare(Map.Entry<String, Double> o1,
		                               Map.Entry<String, Double> o2)
		            {
		                return (o2.getValue()).compareTo(o1.getValue());
		            }
		        });
		        // Inserting the sorted data into hash map
		        HashMap<String, Double> temp = new LinkedHashMap<String, Double>();
		        for (Map.Entry<String, Double> aa : list) {
		            temp.put(aa.getKey(), aa.getValue());
		        }
			for (java.util.Map.Entry<String, Double> pair : temp.entrySet()) {
				//Iterating the hash map in decreasing order
		        context.write(new Text(pair.getKey().toString()), new Text(pair.getValue()+""));
			}
		}
	}
}
