public java.lang.Object generate(Object[] references) {
  return new SpecificUnsafeRowJoiner();
}

class SpecificUnsafeRowJoiner extends org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowJoiner {
  private byte[] buf = new byte[64];
  private UnsafeRow out = new UnsafeRow(4); //输出结果

  // 经过 UnsafeCartesianRDD 且 condition eval 后的结果是 (left-row, right-row)
  public UnsafeRow join(UnsafeRow row1, UnsafeRow row2) { //将 left-row,right-row 合并到一个 buffer 中
    // row1: 2 fields, 1 words in bitset
    // row2: 2, 1 words in bitset
    // output: 4 fields, 1 words in bitset
    final int sizeInBytes = row1.getSizeInBytes() + row2.getSizeInBytes() - 8;
    if (sizeInBytes > buf.length) {
      buf = new byte[sizeInBytes];
    }

    final java.lang.Object obj1 = row1.getBaseObject();
    final long offset1 = row1.getBaseOffset();
    final java.lang.Object obj2 = row2.getBaseObject();
    final long offset2 = row2.getBaseOffset();

    Platform.putLong(buf, Platform.BYTE_ARRAY_OFFSET + 0, Platform.getLong(obj1, offset1 + 0) | (Platform.getLong(obj2, offset2) << 2));


    // Copy fixed length data for row1
    Platform.copyMemory(
      obj1, offset1 + 8,
      buf, Platform.BYTE_ARRAY_OFFSET + 8,
      16);


    // Copy fixed length data for row2
    Platform.copyMemory(
      obj2, offset2 + 8,
      buf, Platform.BYTE_ARRAY_OFFSET + 24,
      16);


    // Copy variable length data for row1
    long numBytesVariableRow1 = row1.getSizeInBytes() - 24;
    Platform.copyMemory(
      obj1, offset1 + 24,
      buf, Platform.BYTE_ARRAY_OFFSET + 40,
      numBytesVariableRow1);


    // Copy variable length data for row2
    long numBytesVariableRow2 = row2.getSizeInBytes() - 24;
    Platform.copyMemory(
      obj2, offset2 + 24,
      buf, Platform.BYTE_ARRAY_OFFSET + 40 + numBytesVariableRow1,
      numBytesVariableRow2);

    long existingOffset;

    existingOffset = Platform.getLong(buf, Platform.BYTE_ARRAY_OFFSET + 16);
    if (existingOffset != 0) {
      Platform.putLong(buf, Platform.BYTE_ARRAY_OFFSET + 16, existingOffset + (16L << 32));
    }

    existingOffset = Platform.getLong(buf, Platform.BYTE_ARRAY_OFFSET + 24);
    if (existingOffset != 0) {
      Platform.putLong(buf, Platform.BYTE_ARRAY_OFFSET + 24, existingOffset + ((16L + numBytesVariableRow1) << 32));
    }


    out.pointTo(buf, sizeInBytes);

    return out;
  }
}

