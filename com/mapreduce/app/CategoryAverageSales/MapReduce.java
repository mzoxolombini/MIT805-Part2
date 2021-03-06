package com.mapreduce.app.CategoryAverageSales;

import java.lang.String;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.StringTokenizer;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import com.mapreduce.*;
import com.mapreduce.app.CategoryAverageSales.CompositeWritable;


public class MapReduce {
		
	
		public static class CategoryMapper extends Mapper<Object, Text, Text, CompositeWritable>
		{
			private Text sales = new Text();
			
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException
			{
				String[] split = value.toString().split(",");
				String Event_Type = split[1];
				String Category = split[4];
				if (Event_Type.equals("purchase")) {
				try
				{
					float Price = Float.parseFloat(split[6]);
					sales.set(Category);
					context.write(sales, new CompositeWritable(Price));
				}
				catch(NumberFormatException e){}
				
				}
			}
		}

	
		public static class CategoryReducer extends Reducer<Text,CompositeWritable,Text,FloatWritable>
		{
			private FloatWritable result = new FloatWritable();
			public void reduce(Text key, Iterable<CompositeWritable> values, Context context) throws IOException, InterruptedException
			{
				//System.out.println("--------------- MIT805 - EXECUTING REDUCER: " + key.toString());
				int numberOfCategories = 0;
				float total = 0;
				for(CompositeWritable val : values)
				{
					numberOfCategories = numberOfCategories +1;
					total += val.price;
				}																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																												
				result.set(total/ numberOfCategories);
				context.write(key, result);
			}
		}


}
