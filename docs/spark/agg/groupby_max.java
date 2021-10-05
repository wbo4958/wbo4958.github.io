// codegenStageId=1
final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
  private Object[] references;
  private scala.collection.Iterator[] inputs;
  private boolean agg_initAgg_0;
  private boolean agg_bufIsNull_0;
  private int agg_bufValue_0;
  private agg_FastHashMap_0 agg_fastHashMap_0;
  private org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow> agg_fastHashMapIter_0;
  private org.apache.spark.unsafe.KVIterator agg_mapIter_0;
  private org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap agg_hashMap_0;
  private org.apache.spark.sql.execution.UnsafeKVExternalSorter agg_sorter_0;
  private scala.collection.Iterator localtablescan_input_0;
  private boolean agg_agg_isNull_3_0;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] agg_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[2];

  public GeneratedIteratorForCodegenStage1(Object[] references) {
    this.references = references;
  }

  public void init(int index, scala.collection.Iterator[] inputs) {
    partitionIndex = index;
    this.inputs = inputs;

    localtablescan_input_0 = inputs[0]; // Local Scan data iterator
    // group by key
    agg_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 32);
    // agg value
    agg_mutableStateArray_0[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 32);
  }

  /**
   * 使用 RowBasedKeyValueBatch 作为 backend 来保存依次插入的 (group by key, agg buffer)
   * 首先查看是否已经有 group by key，如果有则取出 agg buffer 进行 update,
   * 如果没有，则设置 initial buffer, 并插入到 RowBasedKeyValueBatch
   *
   * FastHashMap 不会 rehash, 尝试2次如果没有找到，直接返回
   */
  public class agg_FastHashMap_0 {
    private org.apache.spark.sql.catalyst.expressions.RowBasedKeyValueBatch batch;
    private int[] buckets; // 65536 记录 row number
    private int capacity = 1 << 16;
    private double loadFactor = 0.5;
    private int numBuckets = (int) (capacity / loadFactor); // 131072
    private int maxSteps = 2; //如果有碰撞冲突，只尝试两次
    private int numRows = 0;
    private Object emptyVBase;
    private long emptyVOff;
    private int emptyVLen;
    private boolean isBatchFull = false;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter agg_rowWriter;

    public agg_FastHashMap_0(
        org.apache.spark.memory.TaskMemoryManager taskMemoryManager,
        InternalRow emptyAggregationBuffer) {
      batch = org.apache.spark.sql.catalyst.expressions.RowBasedKeyValueBatch
          .allocate(((org.apache.spark.sql.types.StructType) references[1] /* keySchemaTerm */), ((org.apache.spark.sql.types.StructType) references[2] /* valueSchemaTerm */), taskMemoryManager, capacity);

      final UnsafeProjection valueProjection = UnsafeProjection.create(((org.apache.spark.sql.types.StructType) references[2] /* valueSchemaTerm */));
      final byte[] emptyBuffer = valueProjection.apply(emptyAggregationBuffer).getBytes();

      emptyVBase = emptyBuffer;
      emptyVOff = Platform.BYTE_ARRAY_OFFSET;
      emptyVLen = emptyBuffer.length;

      agg_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter( 1, 32);

      buckets = new int[numBuckets];
      java.util.Arrays.fill(buckets, -1);
    }

    public org.apache.spark.sql.catalyst.expressions.UnsafeRow findOrInsert(UTF8String agg_key_0) {
      long h = hash(agg_key_0);
      int step = 0;
      int idx = (int) h & (numBuckets - 1);
      while (step < maxSteps) {
        // Return bucket index if it's either an empty slot or already contains the key
        if (buckets[idx] == -1) {
          if (numRows < capacity && !isBatchFull) {
            agg_rowWriter.reset();
            agg_rowWriter.zeroOutNullBytes();
            agg_rowWriter.write(0, agg_key_0);
            org.apache.spark.sql.catalyst.expressions.UnsafeRow agg_result
                = agg_rowWriter.getRow();
            Object kbase = agg_result.getBaseObject();
            long koff = agg_result.getBaseOffset();
            int klen = agg_result.getSizeInBytes();

            UnsafeRow vRow
                = batch.appendRow(kbase, koff, klen, emptyVBase, emptyVOff, emptyVLen);
            if (vRow == null) {
              isBatchFull = true;
            } else {
              buckets[idx] = numRows++;
            }
            return vRow;
          } else {
            // No more space
            return null;
          }
        } else if (equals(idx, agg_key_0)) {
          return batch.getValueRow(buckets[idx]);
        }
        idx = (idx + 1) & (numBuckets - 1);
        step++;
      }
      // Didn't find it
      return null;
    }

    private boolean equals(int idx, UTF8String agg_key_0) {
      UnsafeRow row = batch.getKeyRow(buckets[idx]);
      return (row.getUTF8String(0).equals(agg_key_0));
    }

    private long hash(UTF8String agg_key_0) {
      long agg_hash_0 = 0;

      int agg_result_0 = 0;
      byte[] agg_bytes_0 = agg_key_0.getBytes();
      for (int i = 0; i < agg_bytes_0.length; i++) {
        int agg_hash_1 = agg_bytes_0[i];
        agg_result_0 = (agg_result_0 ^ (0x9e3779b9)) + agg_hash_1 + (agg_result_0 << 6) + (agg_result_0 >>> 2);
      }

      agg_hash_0 = (agg_hash_0 ^ (0x9e3779b9)) + agg_result_0 + (agg_hash_0 << 6) + (agg_hash_0 >>> 2);

      return agg_hash_0;
    }

    public org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow> rowIterator() {
      return batch.rowIterator();
    }

    public void close() {
      batch.close();
    }

  }

  private void agg_doAggregateWithKeysOutput_0(UnsafeRow agg_keyTerm_0, UnsafeRow agg_bufferTerm_0)
      throws java.io.IOException {
    ((org.apache.spark.sql.execution.metric.SQLMetric) references[8] /* numOutputRows */).add(1);

    boolean agg_isNull_6 = agg_keyTerm_0.isNullAt(0);
    UTF8String agg_value_7 = agg_isNull_6 ?
        null : (agg_keyTerm_0.getUTF8String(0));
    boolean agg_isNull_7 = agg_bufferTerm_0.isNullAt(0);
    int agg_value_8 = agg_isNull_7 ?
        -1 : (agg_bufferTerm_0.getInt(0));

    agg_mutableStateArray_0[1].reset();

    agg_mutableStateArray_0[1].zeroOutNullBytes();


    // 写入 group by key
    if (agg_isNull_6) {
      agg_mutableStateArray_0[1].setNullAt(0);
    } else {
      agg_mutableStateArray_0[1].write(0, agg_value_7);
    }

    // 写入 agg value
    if (agg_isNull_7) {
      agg_mutableStateArray_0[1].setNullAt(1);
    } else {
      agg_mutableStateArray_0[1].write(1, agg_value_8);
    }
    append((agg_mutableStateArray_0[1].getRow()));

  }

  private void agg_doConsume_0(InternalRow localtablescan_row_0, UTF8String agg_expr_0_0, boolean agg_exprIsNull_0_0, int agg_expr_1_0) throws java.io.IOException {
    UnsafeRow agg_unsafeRowAggBuffer_0 = null;
    UnsafeRow agg_fastAggBuffer_0 = null;

    if (!agg_exprIsNull_0_0) {
      agg_fastAggBuffer_0 = agg_fastHashMap_0.findOrInsert(agg_expr_0_0); //查找 Fast HashMap 中是否有 group by key
    }
    // Cannot find the key in fast hash map, try regular hash map.
    if (agg_fastAggBuffer_0 == null) { // Fast HashMap 里没有
      // generate grouping key
      agg_mutableStateArray_0[0].reset();
      agg_mutableStateArray_0[0].zeroOutNullBytes();
      if (agg_exprIsNull_0_0) {
        agg_mutableStateArray_0[0].setNullAt(0);
      } else {
        agg_mutableStateArray_0[0].write(0, agg_expr_0_0);
      }
      // 计算 group by key 的 hash值
      int agg_unsafeRowKeyHash_0 = (agg_mutableStateArray_0[0].getRow()).hashCode();
      if (true) {
        // try to get the buffer from hash map 从 HashMap 中获得 agg buffer
        agg_unsafeRowAggBuffer_0 =
            agg_hashMap_0.getAggregationBufferFromUnsafeRow((agg_mutableStateArray_0[0].getRow()),
                agg_unsafeRowKeyHash_0);
      }
      // Can't allocate buffer from the hash map. Spill the map and fallback to sort-based
      // aggregation after processing all input rows.
      if (agg_unsafeRowAggBuffer_0 == null) { // spill 到磁盘
        if (agg_sorter_0 == null) {
          agg_sorter_0 = agg_hashMap_0.destructAndCreateExternalSorter();
        } else {
          agg_sorter_0.merge(agg_hashMap_0.destructAndCreateExternalSorter());
        }

        // the hash map had be spilled, it should have enough memory now,
        // try to allocate buffer again.
        // 再重试
        agg_unsafeRowAggBuffer_0 = agg_hashMap_0.getAggregationBufferFromUnsafeRow(
            (agg_mutableStateArray_0[0].getRow()), agg_unsafeRowKeyHash_0);
        if (agg_unsafeRowAggBuffer_0 == null) {
          // failed to allocate the first page
          throw new org.apache.spark.memory.SparkOutOfMemoryError("No enough memory for aggregation");
        }
      }
    }

    // Updates the proper row buffer
    if (agg_fastAggBuffer_0 != null) {
      agg_unsafeRowAggBuffer_0 = agg_fastAggBuffer_0;
    }
    // common sub-expressions
    // evaluate aggregate functions and update aggregation buffers

    // 下面是 agg function 生成的代码，本例是求最大值
    agg_agg_isNull_3_0 = true;
    int agg_value_4 = -1;

    boolean agg_isNull_4 = agg_unsafeRowAggBuffer_0.isNullAt(0);
    int agg_value_5 = agg_isNull_4 ? -1 : (agg_unsafeRowAggBuffer_0.getInt(0));

    if (!agg_isNull_4 && (agg_agg_isNull_3_0 ||  agg_value_5 > agg_value_4)) {
      agg_agg_isNull_3_0 = false;
      agg_value_4 = agg_value_5;
    }

    if (!false && (agg_agg_isNull_3_0 ||
        agg_expr_1_0 > agg_value_4)) {
      agg_agg_isNull_3_0 = false;
      agg_value_4 = agg_expr_1_0;
    }

    agg_unsafeRowAggBuffer_0.setInt(0, agg_value_4);
  }

  private void agg_doAggregateWithKeys_0() throws java.io.IOException {
    while (localtablescan_input_0.hasNext()) { //依次遍历所有的数据进行 agg
      InternalRow localtablescan_row_0 = (InternalRow) localtablescan_input_0.next();
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[7] /* numOutputRows */).add(1);
      boolean localtablescan_isNull_0 = localtablescan_row_0.isNullAt(0);
      UTF8String localtablescan_value_0 = localtablescan_isNull_0 ? // group by key
          null : (localtablescan_row_0.getUTF8String(0));
      int localtablescan_value_1 = localtablescan_row_0.getInt(1); // agg value

      agg_doConsume_0(localtablescan_row_0, localtablescan_value_0, localtablescan_isNull_0, localtablescan_value_1);
      // shouldStop check is eliminated
    }

    // 获得处理好的数据 iterator
    agg_fastHashMapIter_0 = agg_fastHashMap_0.rowIterator();
    agg_mapIter_0 = ((org.apache.spark.sql.execution.aggregate.HashAggregateExec) references[0] /* plan */)
        .finishAggregate(agg_hashMap_0, agg_sorter_0, ((org.apache.spark.sql.execution.metric.SQLMetric) references[3] /* peakMemory */),
            ((org.apache.spark.sql.execution.metric.SQLMetric) references[4] /* spillSize */),
            ((org.apache.spark.sql.execution.metric.SQLMetric) references[5] /* avgHashProbe */),
            ((org.apache.spark.sql.execution.metric.SQLMetric) references[6] /* numTasksFallBacked */));

  }

  protected void processNext() throws java.io.IOException {
    if (!agg_initAgg_0) {
      agg_initAgg_0 = true;
      // 生成 FastHashMap
      agg_fastHashMap_0 = new agg_FastHashMap_0(((org.apache.spark.sql.execution.aggregate.HashAggregateExec) references[0] /* plan */).getTaskContext().taskMemoryManager(),
          ((org.apache.spark.sql.execution.aggregate.HashAggregateExec) references[0] /* plan */).getEmptyAggregationBuffer());

      ((org.apache.spark.sql.execution.aggregate.HashAggregateExec) references[0] /* plan */).getTaskContext().addTaskCompletionListener(
          new org.apache.spark.util.TaskCompletionListener() {
            @Override
            public void onTaskCompletion(org.apache.spark.TaskContext context) {
              agg_fastHashMap_0.close();
            }
          });

      // 生成 regular hash map
      agg_hashMap_0 = ((org.apache.spark.sql.execution.aggregate.HashAggregateExec) references[0] /* plan */)
          .createHashMap();
      long wholestagecodegen_beforeAgg_0 = System.nanoTime();
      agg_doAggregateWithKeys_0(); //进行 aggragate
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[9] /* aggTime */).add((System.nanoTime() - wholestagecodegen_beforeAgg_0) / 1000000);
    }
    // output the result

    //先从  fast hashmap中依次获得数据
    while (agg_fastHashMapIter_0.next()) {
      UnsafeRow agg_aggKey_0 = (UnsafeRow) agg_fastHashMapIter_0.getKey();
      UnsafeRow agg_aggBuffer_0 = (UnsafeRow) agg_fastHashMapIter_0.getValue();
      agg_doAggregateWithKeysOutput_0(agg_aggKey_0, agg_aggBuffer_0);

      if (shouldStop()) return;
    }
    agg_fastHashMap_0.close();

    // 再从 regular hashmap 中获得数据
    while (agg_mapIter_0.next()) {
      UnsafeRow agg_aggKey_0 = (UnsafeRow) agg_mapIter_0.getKey();
      UnsafeRow agg_aggBuffer_0 = (UnsafeRow) agg_mapIter_0.getValue();
      agg_doAggregateWithKeysOutput_0(agg_aggKey_0, agg_aggBuffer_0);
      if (shouldStop()) return;
    }
    agg_mapIter_0.close();
    if (agg_sorter_0 == null) {
      agg_hashMap_0.free();
    }
  }
}
