package com.mapreduce.app.PotentialSalesPerBrand;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.mapreduce.app.PotentialSalesPerBrand.*;
import com.mapreduce.app.PotentialSalesPerBrand.MapReduce.CartMapper;
import com.mapreduce.app.PotentialSalesPerBrand.MapReduce.CartReducer;




/**
 * Hello world!
 *
 */
public class main 
{

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		//create jpb
		Job job = Job.getInstance(conf, "Items_on_Carts_Job");
		job.setJarByClass(MapReduce.class);
		job.setMapperClass(CartMapper.class);
		job.setReducerClass(CartReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CompositeWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
