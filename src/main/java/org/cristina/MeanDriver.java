package org.cristina;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.cristina.MeanDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

/* Clase MeanDriver mediante la cual se obtine la media aritmetica de unas series
 * temporales. Se leen los ficheros de entrada, se genera el fichero de salida y 
 * se llama a los métodos de configuración que implementan el mapper 
 * el combiner y el reducer. 
 */

public class MeanDriver extends Configured implements Tool{
	
	public int run(String[] args) throws Exception {
		
		
		
		/*Obtenemos la ruta de los ficheros de entrada y salida introducida mediante 
		 * parametros al invocar el programa.
		 */
		
		String input = args[0];
		String output = args[1];
	
		//String input = "/home/cloudera/CI1000.txt";
		//String output = "/home/cloudera/Desktop/CarteraInversion";
		
		Path oPath = new Path(output);
	
		/* Indicamos las carpetas HDFS de entrada y de salida del job
		 * Importante: la carpeta de resultados (salida) no debe existir
		 * cuando lanzamos el job
		 */
		
		Configuration conf = getConf();
		FileSystem.get(oPath.toUri(), conf).delete(oPath, true);
		Job job = new Job(conf,"MeanDriver");
		job.setJarByClass(MeanDriver.class);
		
			
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);



		/* Se definen la clase Map, la clase Combine y la clase Reduce del job */
		 
		job.setMapperClass(Mean.MeanMapper.class);
		job.setCombinerClass(Mean.MeanCombiner.class);
		job.setReducerClass(Mean.MeanReducer.class);
		
	 	//Tipos de salida	
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MeanWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MeanWritable.class);
				
		boolean success = job.waitForCompletion(true);
		
		System.exit(success ? 0 : 1);
		return 0;
		
	
	
	}
	public static void main(String args[]) throws Exception {
		ToolRunner.run(new MeanDriver(), args);
	}
}