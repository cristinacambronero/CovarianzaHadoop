package org.cristina;


import java.io.IOException;
import org.apache.mahout.math.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;
import org.apache.hadoop.mapreduce.Mapper;


/* Clase que convierte un fichero de entrada en un SequenceFile. SOlo es necesario un proceso map */

	public class PreprocesadoMapper extends Mapper<LongWritable, Text, IntWritable, VectorWritable> {
		
		private VectorWritable vw = new VectorWritable();
		
		@Override
		
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {

			/* Se analiza cada linea de log de entrada al mapper */

			String row[] = value.toString().split("\t");
			
			/*Convertimos cada elemento String a un array de Double que sera la 
			 * el argumento del VectorWritable que queremos generar como salida.
			 */
			double[] arrDouble = new double[row.length];
			
			/* Se descarta el componente 0 que será el número de línea que estemos analizando*/
			
			for(int i=1; i<row.length; i++)
			{
			   arrDouble[i] = Double.parseDouble((row[i]));
			}

			Vector vector = new DenseVector(arrDouble);
			String keyInt = key.toString();	
			vw.set(vector);
			
			context.write(new IntWritable(Integer.parseInt(keyInt)), vw );

			
		}
	}
