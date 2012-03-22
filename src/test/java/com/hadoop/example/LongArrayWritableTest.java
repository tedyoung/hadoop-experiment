package com.hadoop.example;

import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 *
 */
public class LongArrayWritableTest {
  @Test
  public void testToStringWithMultipleValuesDoesNotHaveCommaAtEnd() throws Exception {
    LongWritable longWritable1 = new LongWritable();
    LongWritable longWritable2 = new LongWritable();
    LongWritable longWritable3 = new LongWritable();
    longWritable1.set(10);
    longWritable2.set(20);
    longWritable3.set(30);
    LongWritable[] longs = new LongWritable[]{longWritable1, longWritable2, longWritable3};
    LongArrayWritable longArrayWritable = new LongArrayWritable(longs);
    assertEquals("10, 20, 30", longArrayWritable.toString());
  }

  @Test
  public void testToStringWithOneValueDoesNotHaveComma() throws Exception {
    LongWritable longWritable1 = new LongWritable();
    longWritable1.set(10);
    LongArrayWritable longArrayWritable = new LongArrayWritable(longWritable1);
    assertEquals("10", longArrayWritable.toString());
  }
}
