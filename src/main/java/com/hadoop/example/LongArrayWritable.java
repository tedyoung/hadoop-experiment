package com.hadoop.example;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * An array of longs
 */
public class LongArrayWritable extends ArrayWritable {

  public LongArrayWritable(LongWritable[] values) {
    super(LongWritable.class, values);
  }

  public LongArrayWritable(LongWritable value) {
    super(LongWritable.class, new LongWritable[]{value});
  }

  public LongArrayWritable() {
    super(LongWritable.class);
  }

  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    LongWritable[] values = (LongWritable[]) get();
    for (int i = 0, valuesLength = values.length; i < valuesLength; i++) {
      writeValue(stringBuilder, values[i], i, valuesLength);
    }
    return stringBuilder.toString();
  }

  private void writeValue(StringBuilder stringBuilder, LongWritable value, int i, int valuesLength) {
    stringBuilder.append(value.get());
    writeValueSeparator(stringBuilder, i, valuesLength);
  }

  private void writeValueSeparator(StringBuilder stringBuilder, int i, int valuesLength) {
    if (!lastValue(i, valuesLength)) {
      stringBuilder.append(", ");
    }
  }

  private boolean lastValue(int index, int lastIndex) {
    return index == lastIndex - 1;
  }

}
