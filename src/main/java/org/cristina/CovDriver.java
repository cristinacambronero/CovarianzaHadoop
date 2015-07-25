package org.cristina;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

/* Clase CovDriver mediante la cual llama a los metodos encargados del calculo de la covarianza
 */
public class CovDriver extends Configured implements Tool{
	
	public int run(String[] args) throws Exception {
		
		
		
		/*Obtenemos la ruta de los ficheros de entrada y salida y la situacion del fichero de medias introducida mediante 
		 * parametros al invocar el programa.
		 */
		
		String input = args[0];
		String output = args[1];
		String meanDir = args[2];

		//String input = "/home/cloudera/CI10.txt";;
		//String output = "/home/cloudera/Desktop/SalidaCov";
		//String meanDir = "/home/cloudera/Desktop/CarteraInversion";
		

		Path oPath = new Path(output);
	
		/* Indicamos las carpetas HDFS de entrada y de salida del job
		 * Importante: la carpeta de resultados (salida) no debe existir
		 * cuando lanzamos el job
		 */
		
		Configuration conf = getConf();
		conf.set("MeanDir", meanDir);
		FileSystem.get(oPath.toUri(), conf).delete(oPath, true);
		Job job = new Job(conf,"CovDriver");
		job.setJarByClass(CovDriver.class);
				
			
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		/* Se definen la clase Map, la clase Combine y la clase Reduce del job */
		
		
		job.setMapperClass(Covarianza.CovMapper.class);
		job.setCombinerClass(Covarianza.CovCombiner.class);
		job.setReducerClass(Covarianza.CovReducer.class);
		
	 		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
				
		boolean success = job.waitForCompletion(true);
		
		System.exit(success ? 0 : 1);
		return 0;
		
	
	
	}
	public static void main(String args[]) throws Exception {
		ToolRunner.run(new CovDriver(), args);
	}
}
