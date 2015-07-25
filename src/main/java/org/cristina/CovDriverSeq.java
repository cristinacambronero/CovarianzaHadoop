package org.cristina;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

/* Clase CovDriverSeq mediante la cual llama a los metodos encargados del calculo de la covarianza con Mahout
 */
public class CovDriverSeq extends Configured implements Tool{
	
	public int run(String[] args) throws Exception {
		
				
		/*Obtenemos la ruta de los ficheros de entrada y salida introducida mediante 
		 * parametros al invocar el programa.
		 */
		
		String input = args[0]+"/part-r-*";
		String output = args[1];
		String meanDir = args[2];
		
		/*String input = "/home/cloudera/Desktop/SequenceFileSalida/part-r-*";
		String output = "/home/cloudera/Desktop/SalidaCov";
		String meanDir = "/home/cloudera/Desktop/CarteraInversion";*/
		

		Path oPath = new Path(output);
	
		/* Indicamos las carpetas HDFS de entrada y de salida del job
		 * Importante: la carpeta de resultados (salida) no debe existir
		 * cuando lanzamos el job
		 */
		
		Configuration conf = getConf();
		conf.set("MeanDir", meanDir);
		FileSystem.get(oPath.toUri(), conf).delete(oPath, true);
		Job job = new Job(conf,"CovDriverSeq");
		job.setJarByClass(CovDriverSeq.class);
				
			
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		/* Se definen la clase Map, la clase Combine y la clase Reduce del job */

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(CovarianzaSeq.CovMapper.class);
		job.setCombinerClass(CovarianzaSeq.CovCombiner.class);
		job.setReducerClass(CovarianzaSeq.CovReducer.class);
		
		//Tipos de salida key/value
	 		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
				
		boolean success = job.waitForCompletion(true);
		
		System.exit(success ? 0 : 1);
		return 0;
		
	
	
	}
	public static void main(String args[]) throws Exception {
		ToolRunner.run(new CovDriverSeq(), args);
	}
}
