public Object generate(Object[] references) {
  return new GeneratedIteratorForCodegenStage1(references);
}

// codegenStageId=1
final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
  private Object[] references;
  private scala.collection.Iterator[] inputs;
  private scala.collection.Iterator localtablescan_input_0;
  private org.apache.spark.sql.execution.joins.UnsafeHashedRelation bhj_relation_0;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] bhj_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[2];

  public GeneratedIteratorForCodegenStage1(Object[] references) {
    this.references = references;
  }

  public void init(int index, scala.collection.Iterator[] inputs) {
    partitionIndex = index;
    this.inputs = inputs;
    localtablescan_input_0 = inputs[0];

    bhj_relation_0 = ((org.apache.spark.sql.execution.joins.UnsafeHashedRelation) ((org.apache.spark.broadcast.TorrentBroadcast) references[1] /* broadcast */).value()).asReadOnlyCopy();
    incPeakExecutionMemory(bhj_relation_0.estimatedSize());

    bhj_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 32);
    bhj_mutableStateArray_0[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 32);

  }

  protected void processNext() throws java.io.IOException {
    while ( localtablescan_input_0.hasNext()) {
      InternalRow localtablescan_row_0 = (InternalRow) localtablescan_input_0.next();
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);
      boolean localtablescan_isNull_1 = localtablescan_row_0.isNullAt(1);
      UTF8String localtablescan_value_1 = localtablescan_isNull_1 ?
      null : (localtablescan_row_0.getUTF8String(1));

      // generate join key for stream side
      bhj_mutableStateArray_0[0].reset();

      bhj_mutableStateArray_0[0].zeroOutNullBytes();

      if (localtablescan_isNull_1) {
        bhj_mutableStateArray_0[0].setNullAt(0);
      } else {
        bhj_mutableStateArray_0[0].write(0, localtablescan_value_1);
      }
      // find matches from HashRelation
      scala.collection.Iterator bhj_matches_0 = (bhj_mutableStateArray_0[0].getRow()).anyNull() ? null : (scala.collection.Iterator)bhj_relation_0.get((bhj_mutableStateArray_0[0].getRow()));

      // 在 buildPlan 中找到 matches
      if (bhj_matches_0 != null) {
        boolean bhj_found_0 = false;
        while (!bhj_found_0 && bhj_matches_0.hasNext()) {
          UnsafeRow bhj_buildRow_0 = (UnsafeRow) bhj_matches_0.next();
          {
            bhj_found_0 = true;
          }
        }
        if (bhj_found_0) { //只写入 streamPlan
          ((org.apache.spark.sql.execution.metric.SQLMetric) references[2] /* numOutputRows */).add(1);

          int localtablescan_value_0 = localtablescan_row_0.getInt(0);
          bhj_mutableStateArray_0[1].reset();

          bhj_mutableStateArray_0[1].zeroOutNullBytes();

          bhj_mutableStateArray_0[1].write(0, localtablescan_value_0);

          if (localtablescan_isNull_1) {
            bhj_mutableStateArray_0[1].setNullAt(1);
          } else {
            bhj_mutableStateArray_0[1].write(1, localtablescan_value_1);
          }
          append((bhj_mutableStateArray_0[1].getRow()));

        }
      }
      if (shouldStop()) return;
    }
  }

}

