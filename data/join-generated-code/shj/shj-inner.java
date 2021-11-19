public Object generate(Object[] references) {
  return new GeneratedIteratorForCodegenStage1(references);
}

// codegenStageId=1
final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
  private Object[] references;
  private scala.collection.Iterator[] inputs;
  private scala.collection.Iterator inputadapter_input_0;
  private org.apache.spark.sql.execution.joins.HashedRelation shj_relation_0;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] shj_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[2];

  public GeneratedIteratorForCodegenStage1(Object[] references) {
    this.references = references;
  }

  public void init(int index, scala.collection.Iterator[] inputs) { // stream 和 build iterators
    partitionIndex = index;
    this.inputs = inputs;
    inputadapter_input_0 = inputs[0]; // stream iterator

    // 对 build paln build HashRelation, 这个和 BroadcastHashJoin 建立 HashRelation 是一样的， UnsafeHashedRelation 或 LongHashedRelation
    shj_relation_0 = ((org.apache.spark.sql.execution.joins.ShuffledHashJoinExec) references[0] /* plan */).buildHashedRelation(inputs[1]);
    shj_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 32); // 一个 column 的 field, 保存join key
    shj_mutableStateArray_0[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(4, 64); // 4个 column 的 field

  }

  protected void processNext() throws java.io.IOException {
    while ( inputadapter_input_0.hasNext()) { // streaming plan
      InternalRow inputadapter_row_0 = (InternalRow) inputadapter_input_0.next(); //streaming row

      boolean inputadapter_isNull_1 = inputadapter_row_0.isNullAt(1);
      // 取出 stream row 的join key
      UTF8String inputadapter_value_1 = inputadapter_isNull_1 ?  null : (inputadapter_row_0.getUTF8String(1));

      // generate join key for stream side
      shj_mutableStateArray_0[0].reset();
      shj_mutableStateArray_0[0].zeroOutNullBytes();

      if (inputadapter_isNull_1) {
        shj_mutableStateArray_0[0].setNullAt(0);
      } else {
        shj_mutableStateArray_0[0].write(0, inputadapter_value_1); //保存 join key
      }
      // find matches from HashRelation
      // 从 HashRelation 中查找 match 的 所有 row
      scala.collection.Iterator shj_matches_0 = (shj_mutableStateArray_0[0].getRow()).anyNull() ?
      null : (scala.collection.Iterator)shj_relation_0.get((shj_mutableStateArray_0[0].getRow()));
      if (shj_matches_0 != null) { // 没有 match 上
        while (shj_matches_0.hasNext()) { // match 上了，开始进行 inner join
          UnsafeRow shj_buildRow_0 = (UnsafeRow) shj_matches_0.next(); // build plan 中的一条 row
          {
            ((org.apache.spark.sql.execution.metric.SQLMetric) references[1] /* numOutputRows */).add(1);

            int inputadapter_value_0 = inputadapter_row_0.getInt(0);
            boolean shj_isNull_1 = shj_buildRow_0.isNullAt(0);
            UTF8String shj_value_1 = shj_isNull_1 ?
            null : (shj_buildRow_0.getUTF8String(0));
            // 获得 build plan 的join key
            int shj_value_2 = shj_buildRow_0.getInt(1);
            shj_mutableStateArray_0[1].reset();

            shj_mutableStateArray_0[1].zeroOutNullBytes();

            shj_mutableStateArray_0[1].write(0, inputadapter_value_0);

            if (inputadapter_isNull_1) {
              shj_mutableStateArray_0[1].setNullAt(1);
            } else {
              shj_mutableStateArray_0[1].write(1, inputadapter_value_1);
            }

            if (shj_isNull_1) {
              shj_mutableStateArray_0[1].setNullAt(2);
            } else {
              shj_mutableStateArray_0[1].write(2, shj_value_1);
            }

            shj_mutableStateArray_0[1].write(3, shj_value_2);
            append((shj_mutableStateArray_0[1].getRow()).copy());

          }
        }
      }
      if (shouldStop()) return;
    }
  }

}
