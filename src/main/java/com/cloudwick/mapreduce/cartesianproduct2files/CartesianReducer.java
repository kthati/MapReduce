package com.cloudwick.mapreduce.cartesianproduct2files;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CartesianReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub

		List<String[]> list = new ArrayList<String[]>();

		while (values.hasNext()) {
			String[] split = values.next().toString().split(",");
			list.add(split);

		}
		String[][] array = list.toArray(new String[0][0]);

		StringBuffer buffer = new StringBuffer();

		int counter = 11;
		for (int i = 1; i < counter; i++) {
			double sum = 0.0;
			for (int j = 0; j < array.length; j++) {
				sum += Double.parseDouble(array[j][i]);
			}
			double avg = sum / array.length;
			buffer.append(avg);
			if (i != counter - 1) {
				buffer.append(",");
			}
		}
		output.collect(key, new Text(buffer.toString()));
	}

}
