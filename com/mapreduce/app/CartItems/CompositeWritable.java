package com.mapreduce.app.CartItems;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public  class CompositeWritable implements Writable
{

	float price = 0;
	float total = 0;
	int count = 0;

	public CompositeWritable()
	{
	}

	public CompositeWritable(float price)
	{
		this.price = price;
	}

	public CompositeWritable(int count, float price, float total)
	{
		this.count = count;
		this.price = price;
		this.total = total;
	}

	public CompositeWritable(CompositeWritable copy)
	{
		this.price = copy.price;
		this.count = copy.count;
		this.total = copy.total;
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.price = in.readFloat();
		this.count = in.readInt();
		this.total = in.readFloat();
		
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		
		out.writeFloat(this.price);
		out.writeInt(count);
		out.writeFloat(this.total);
	}
}