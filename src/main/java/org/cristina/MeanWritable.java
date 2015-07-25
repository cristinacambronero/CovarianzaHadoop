package org.cristina;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;

/* Clase MeanWritable definida para  guardar dos FloatWritable en un mismo value
 */
 
public class MeanWritable implements WritableComparable<MeanWritable> {

	private FloatWritable sumaMedia;
	private FloatWritable longitud;

	public MeanWritable() {
		sumaMedia = new FloatWritable();
		longitud= new FloatWritable();
	}

	public MeanWritable(FloatWritable sumaMedia, FloatWritable longitud) {
		this.sumaMedia = sumaMedia;
		this.longitud = longitud;
	}

	public FloatWritable getSumaMedia() {
		return sumaMedia;
	}

	public void setSumaMedia(FloatWritable sumaMedia) {
		this.sumaMedia = sumaMedia;
	}

	public FloatWritable getLongitud() {
		return longitud;
	}

	public void setLongitud(FloatWritable longitud) {
		this.longitud = longitud;
	}

	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((longitud == null) ? 0 : longitud.hashCode());
		result = prime * result
				+ ((sumaMedia == null) ? 0 : sumaMedia.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MeanWritable other = (MeanWritable) obj;
		if (longitud == null) {
			if (other.longitud != null)
				return false;
		} else if (!longitud.equals(other.longitud))
			return false;
		if (sumaMedia == null) {
			if (other.sumaMedia != null)
				return false;
		} else if (!sumaMedia.equals(other.sumaMedia))
			return false;
		return true;
	}

	public void readFields(DataInput in) throws IOException {
		sumaMedia.readFields(in);
		longitud.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		sumaMedia.write(out);
		longitud.write(out);
	}


	public int compareTo(MeanWritable o) {
		int cmp = sumaMedia.compareTo(o.getSumaMedia());
		return cmp;
		
	}

	@Override
	public String toString() {
		return sumaMedia + " " + longitud;
	}
	

	
}
