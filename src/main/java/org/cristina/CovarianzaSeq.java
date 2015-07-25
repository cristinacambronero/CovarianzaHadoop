package org.cristina;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.hadoop.io.Text;

public class CovarianzaSeq {
	
	/* Tras cada linea de entrada guardada en forma de VectorWritable, se realizan multiplicaciones 
	 * para cada par de valores existentes en el vector y se convierten a double. Como
	 * clave utilizaremos el identificador de las empresas multiplicadas.
	 */

	public static class CovMapper extends
			Mapper<IntWritable, VectorWritable, Text, DoubleWritable> {

		@Override
		public void map(IntWritable key, VectorWritable value, Context context)
				throws IOException, InterruptedException {

			Vector vector = value.get();

			for (int i = 1; i < vector.size(); ++i) {
				for (int j = i; j < vector.size(); ++j) {

					context.write(new Text(i + "-" + j), new DoubleWritable(
							vector.get(i) * vector.get(j)));
				}
			}
		}
	}
	
	/*
	 * Clase combiner mediante la cual se suman los componentes iguales para
	 * cada clave de cada mapper de esta forma se reducen el envio de elementos
	 * por red al reducer. 
	 */

	public static class CovCombiner extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {

			double sum = 0;

			for (DoubleWritable value : values) {
				sum = sum + value.get();
			}

			context.write(key, new DoubleWritable(sum));
		}
	}
	
	/* Se lee el fichero de salida del job Mean a traves del método Setup y se guardan los 
	 * datos en un HashMap para un acceso rápido. Posteriormente en el método reduce se 
	 * implementa la fórmula final de la covarianza y se emite el valor final.
	 */

	public static class CovReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		private Map<String, String> medias = new HashMap<String, String>();

		@Override
		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();

			String archives = conf.get("MeanDir");
			Path meanPath = new Path(archives);
			FileSystem fs = FileSystem.get(conf);

			/****/
			FileStatus[] status_list = fs.listStatus(meanPath);
			Map<String, String> ficheroMedias = new HashMap<String, String>();
			if (status_list != null) {
				for (FileStatus status : status_list) {
					String filename = status.getPath().getName();
					String pattern = "part-r-*";
					Pattern regex = Pattern.compile(pattern);
					Matcher matcher = regex.matcher(filename);

					Path fullFilePath = new Path(meanPath + "/" + filename);
					System.out.println(fullFilePath.getName());

					/****/
					if (matcher.find()) {

						try (SequenceFile.Reader reader = new SequenceFile.Reader(
								fs, fullFilePath, conf)) {
							Text key = new Text();
							MeanWritable val = new MeanWritable();

							while (reader.next(key, val)) {

								ficheroMedias.put(key.toString(),
										val.toString());

							}
							reader.close();

						}

					}
				}
			}
			this.medias = ficheroMedias;

		}

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {

			String splitkey[] = key.toString().split("-");

			String infoMedias1[] = (medias.get(splitkey[0])).split(" ");
			double media1 = Double.parseDouble(infoMedias1[0]);
			double longitud = Double.parseDouble(infoMedias1[1]);

			String infoMedias2[] = medias.get(splitkey[1]).split(" ");
			double media2 = Double.parseDouble(infoMedias2[0]);
			double sum = 0;

			for (DoubleWritable value : values) {
				sum = sum + value.get();
			}

			System.out.println("sum/lon " + media1 + media2);
			double inter = sum / longitud;
			System.out.println(inter);
			double covarianza = (sum / longitud) - media1 * media2;

			context.write(key, new DoubleWritable(covarianza));
		}

	}
}
