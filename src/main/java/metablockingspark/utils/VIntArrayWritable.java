/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package metablockingspark.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
	
public class VIntArrayWritable extends ArrayWritable {
		
	public VIntArrayWritable() { 
		super(VIntWritable.class); 
	} 
	
	public VIntArrayWritable(VIntWritable[] values) {
        super(VIntWritable.class, values);
    }
	
	@Override
	public VIntWritable[] get() {
	    return (VIntWritable[]) super.get();
	}
	
	@Override
	public void set(Writable[] values) {
	    super.set((VIntWritable[]) values);
	}
	
	@Override
	public String toString() {
		return Arrays.toString(get());
	}
	
        @Override
	public void write(DataOutput out) {
		try {
			//first store a counter for the size of the data written
			out.writeInt(get().length);
			//out.writeShort(get().length); //2 bytes range = [-32768, 32767] 
			//out.writeByte(get().length); //use this instead, if maximum size < 256 = 2^8
			//writeByte just reduces disk usage by |entities| bytes (few MBs)
			for (VIntWritable i : get()) {						
				//out.writeInt(i.get());
				//i.write(out);
				WritableUtils.writeVInt(out, i.get());
			}
		} catch (IOException e) {					
                    System.err.println(e);
		}
	}
	
        @Override
	public void readFields(DataInput in) {		
		try {
			int counter = in.readInt();
			//short counter = in.readShort();
			//byte counter = in.readByte(); //use this instead, if writeByte is used in write()
			
			VIntWritable[] values = new VIntWritable[counter];
			for (int i = 0; i < counter; ++i){				
				values[i] = new VIntWritable(WritableUtils.readVInt(in));				
			}			
			
			set(values);
		} catch (IOException e) {
                    System.err.println(e);
		}
	}
	
}
