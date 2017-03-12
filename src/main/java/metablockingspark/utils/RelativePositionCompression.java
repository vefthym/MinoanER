/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package metablockingspark.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.VIntWritable;

public class RelativePositionCompression {
	
	/**
	 * Returns a compression of the input set with length equal to input collection's size,
	 * containing all the elements of uncompressed as their difference to their previous element. <br/>
	 * Example: [1, 3, 14, 17, 25, 40] --> [1, 2, 11, 3, 8, 15]
	 * @param uncompressed the set to be compressed
	 * @return a compressed array representation of the uncompressed collection
	 */
	public static VIntArrayWritable compress(Set<VIntWritable> uncompressed) {
		Set<VIntWritable> blocks = new TreeSet<>(uncompressed); //in case they are unordered
		
		
		VIntWritable[] compressed = new VIntWritable[blocks.size()];
		int i = 0;
		int prevBlock = 0;
		int currBlock;
		Iterator<VIntWritable> it = blocks.iterator();
		while (it.hasNext()) {			
			currBlock = it.next().get();
			compressed[i++] = new VIntWritable(currBlock - prevBlock);
			prevBlock = currBlock;
		}
		
		return new VIntArrayWritable(compressed);
	}
	
	
	
	/**
	 * Returns a compression of the input set with length equal to input collection's size,
	 * containing all the elements of uncompressed as their difference to their previous element. <br/>
	 * Example: [1, 3, 14, 17, 25, 40] --> [1, 2, 11, 3, 8, 15]
	 * @param uncompressed the array to be compressed
	 * @return a compressed array representation of the uncompressed array
	 */
	public static VIntArrayWritable compress(VIntArrayWritable uncompressed) {
		VIntWritable[] input = uncompressed.get();
		return compress(input);
	}
	
	/**
	 * Returns a compression of the input set with length equal to input collection's size,
	 * containing all the elements of uncompressed as their difference to their previous element. <br/>
	 * IMPORTANT: to save time, it requires that the input uncompressed array is sorted
	 * Example: [1, 3, 14, 17, 25, 40] --> [1, 2, 11, 3, 8, 15]
	 * @param uncompressed the (sorted!) array to be compressed
	 * @return a compressed array representation of the uncompressed array
	 */
	public static VIntArrayWritable compress(VIntWritable[] uncompressed) {		
		if (uncompressed.length < 2) { //no change applicable => return the input
			return new VIntArrayWritable(uncompressed); 
		}
		//Arrays.sort(uncompressed); //assume input uncompressed array is sorted
		
		VIntWritable[] compressed = new VIntWritable[uncompressed.length];
				
		int currElement;
		int prevElement = uncompressed[0].get();
		compressed[0] = uncompressed[0];
		for (int i = 1; i< compressed.length; ++i) {		
			currElement = uncompressed[i].get();
			compressed[i] = new VIntWritable(currElement - prevElement);
			prevElement = currElement;
		}
		
		return new VIntArrayWritable(compressed);
	}
	
	
	public static VIntArrayWritable compressFromSecond(VIntWritable[] uncompressed) {		
		if (uncompressed.length < 3) { //no change applicable => return the input
			return new VIntArrayWritable(uncompressed); 
		}
		//Arrays.sort(uncompressed); //assume input uncompressed array is sorted
		
		VIntWritable[] compressed = new VIntWritable[uncompressed.length];
		compressed[0] = uncompressed[0]; //ignore the first element
		
		int currElement;
		int prevElement = uncompressed[1].get();
		compressed[1] = uncompressed[1];
		for (int i = 2; i< compressed.length; ++i) {		
			currElement = uncompressed[i].get();
			compressed[i] = new VIntWritable(currElement - prevElement);
			prevElement = currElement;
		}
		
		return new VIntArrayWritable(compressed);
	}
	
	
	
	
	
	/**
	 * Uncompresses the input set with length equal to input array's length,
	 * containing all the elements of compressed by adding the current compressed element to its previous uncompressed element. <br/>
	 * Example: [1, 2, 11, 3, 8, 15] --> [1, 3, 14, 17, 25, 40] 
	 * @param compressed the array to be uncompressed
	 * @return an uncompressed representation of compressed array
	 */
	public static VIntWritable[] uncompress(VIntArrayWritable compressed) {	
		VIntWritable[] compressedArray = compressed.get();
		if (compressedArray.length < 2) {
			return compressed.get();
		}
		VIntWritable[] uncompressed = new VIntWritable[compressedArray.length];		
				
		int prevElement = compressedArray[0].get();
		int currElement;
		uncompressed[0] = compressedArray[0];
		for (int i = 1; i < compressedArray.length; ++i) {
			currElement = prevElement + compressedArray[i].get();
			prevElement = currElement;
			uncompressed[i] = new VIntWritable(currElement);
		}
		
		return uncompressed;
	}
	
	
	public static VIntWritable[] uncompressFromSecond(VIntArrayWritable compressed) {	
		VIntWritable[] compressedArray = compressed.get();
		if (compressedArray.length < 3) {
			return compressed.get();
		}
		VIntWritable[] uncompressed = new VIntWritable[compressedArray.length];		
				
		//ignore the first element
		uncompressed[0] = compressedArray[0];
		
		int prevElement = compressedArray[1].get();
		int currElement;
		uncompressed[1] = compressedArray[1];
		for (int i = 2; i < compressedArray.length; ++i) {
			currElement = prevElement + compressedArray[i].get();
			prevElement = currElement;
			uncompressed[i] = new VIntWritable(currElement);
		}
		
		return uncompressed;
	}
	
	
	
	
	/**
	 * Uncompresses the input set with length equal to input array's length,
	 * containing all the elements of compressed by adding the current compressed element to its previous uncompressed element. <br/>
	 * Example: [1, 2, 11, 3, 8, 15] --> [1, 3, 14, 17, 25, 40] 
	 * @param compressed the array to be uncompressed
	 * @return an uncompressed representation of compressed array
	 */
	public static Integer[] uncompress(Integer[] compressed) {
		if (compressed.length < 1) {
			return null;
		}
		Integer[] uncompressed = new Integer[compressed.length];		
				
		Integer prevBlock = compressed[0];
		Integer currBlock;
		uncompressed[0] = prevBlock;
		for (int i = 1; i < compressed.length; ++i) {
			currBlock = prevBlock + compressed[i];
			prevBlock = currBlock;
			uncompressed[i] = currBlock;
		}
		
		return uncompressed;
	}
	
	
	/**
	 * Uncompresses the input set with length equal to input array's length,
	 * containing all the elements of compressed by adding the current compressed element to its previous uncompressed element. <br/>
	 * Example: [1, 2, 11, 3, 8, 15] --> [1, 3, 14, 17, 25, 40] 
	 * @param compressed the array to be uncompressed
	 * @return an uncompressed representation of compressed array
	 */
	public static VIntArrayWritable uncompress(Collection<Integer> compressed) {	
		Integer[] compressedArray = new Integer[compressed.size()]; 
		compressedArray = compressed.toArray(compressedArray);
		if (compressedArray.length < 1) {
			return null;
		}
		VIntWritable[] uncompressed = new VIntWritable[compressedArray.length];		
				
		int prevBlock = compressedArray[0];
		int currBlock;
		uncompressed[0] = new VIntWritable(prevBlock);
		for (int i = 1; i < compressedArray.length; ++i) {
			currBlock = prevBlock + compressedArray[i];
			prevBlock = currBlock;
			uncompressed[i] = new VIntWritable(currBlock);
		}
		
		return new VIntArrayWritable(uncompressed);
	}
	
	public static void main (String[] args) {
		Set<VIntWritable> blocks = new TreeSet<>();
		blocks.add(new VIntWritable(3));
		blocks.add(new VIntWritable(1));
		blocks.add(new VIntWritable(17));
		blocks.add(new VIntWritable(25));
		blocks.add(new VIntWritable(40));
		blocks.add(new VIntWritable(14));		
		System.out.println(Arrays.toString(blocks.toArray()));
		
		VIntWritable[] compressed = compress(blocks).get();
		System.out.println(Arrays.toString(compressed));
		System.out.println(Arrays.toString(uncompress(new VIntArrayWritable(compressed))));
		
		List<Integer> test = new ArrayList<>(compressed.length);
		for (int i = 0; i < compressed.length; ++i) {
			test.add(compressed[i].get());
		}
		System.out.println(Arrays.toString(uncompress(test).get()));
		
		System.out.println("Testing negative arrays:");
		VIntWritable[] blocksArray = new VIntWritable[6];		
		blocksArray[0] = new VIntWritable(-3);
		blocksArray[1] = new VIntWritable(-1);
		blocksArray[2] = new VIntWritable(-17);
		blocksArray[3] = new VIntWritable(-25);
		blocksArray[4] = new VIntWritable(-40);
		blocksArray[5] = new VIntWritable(-14);
		Arrays.sort(blocksArray, Collections.reverseOrder());
		System.out.println(Arrays.toString(blocksArray));
		
		VIntArrayWritable compressed2 = compress(blocksArray);
		System.out.println(Arrays.toString(compressed2.get()));
		System.out.println(Arrays.toString(uncompress(compressed2)));
				
	}

}
