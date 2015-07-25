package org.cristina;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.VectorWritable;
import org.cristina.PreprocesadoMapper;

public class PreprocesadoDriver extends Configured implements Tool{
	
	public int run(String[] args) throws Exception {
		
		// Ficheros de entrada y salida por linea de comandos
		String input = args[0];
		String output = args[1];
		
		/*String input = "/home/cloudera/CI1000.txt";
		String output = "/home/cloudera/Desktop/SequenceFileSalida";*/
		Path oPath = new Path(output);
		
		// Configuramos las variables necesarias para el job
		Configuration conf = getConf();
		FileSystem.get(oPath.toUri(), conf).delete(oPath, true);
		Job job = new Job(conf,"PreprocesadoDriver");
		job.setJarByClass(PreprocesadoMapper.class);
		
			
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
	    // Indicamos el mapper y tipos de salida de key/value
	
		job.setMapperClass(PreprocesadoMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		
		boolean success = job.waitForCompletion(true);
		
		System.exit(success ? 0 : 1);
		return 0;
		
		
	}
	public static void main(String args[]) throws Exception {
		ToolRunner.run(new PreprocesadoDriver(), args);
	}
}