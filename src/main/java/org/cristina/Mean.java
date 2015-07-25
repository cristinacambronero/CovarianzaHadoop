package org.cristina;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Mean {
	
	/* Clase Mapper que lee el fichero de entrada y separa el valor de cada empresa para poder 
	 * calcular la media. Como salida tenemos la dupla <#empresa, (valor,1)>.
	 */

	public static class MeanMapper extends
			Mapper<LongWritable, Text, Text, MeanWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			/* Se analiza cada linea de fichero de entrada al mapper */

			String row[] = value.toString().split("\t");

			for (int i = 1; i < row.length; i++) {

				context.write(new Text(Integer.toString(i)), new MeanWritable(
						new FloatWritable(Float.parseFloat(row[i])),
						new FloatWritable(1)));

			}

		}

	}

	/*
	 * Clase combiner mediante la cual se suman los componentes iguales para
	 * cada clave de cada mapper de esta forma se reducen el envio de elementos
	 * por red al reducer. Como salida se generera la misma key y como valor un
	 * nuevo objeto MeanWritable
	 */

	public static class MeanCombiner extends
			Reducer<Text, MeanWritable, Text, MeanWritable> {

		public void reduce(Text key, Iterable<MeanWritable> values,
				Context context) throws IOException, InterruptedException {

			float suma = 0;
			float longitud = 0;

			for (MeanWritable value : values) {

				suma = suma + (value.getSumaMedia()).get();
				longitud = longitud + (value.getLongitud()).get();

			}

			context.write(key, new MeanWritable(new FloatWritable(suma),
					new FloatWritable(longitud)));

		}
	}

	
	/* La clase Reducer suma todas las entradas con la misma key y divide entre la longitud para 
	 * calcular la media.
	 */
	
	public static class MeanReducer extends
			Reducer<Text, MeanWritable, Text, MeanWritable> {

		public void reduce(Text key, Iterable<MeanWritable> values,
				Context context) throws IOException, InterruptedException {

			float suma = 0;
			float longitud = 0;

			for (MeanWritable value : values) {

				suma = suma + (value.getSumaMedia()).get();
				longitud = longitud + (value.getLongitud()).get();

			}
			float mediaAritmetica = suma / longitud;

			context.write(key, new MeanWritable(new FloatWritable(
					mediaAritmetica), new FloatWritable(longitud)));

		}
	}

}
