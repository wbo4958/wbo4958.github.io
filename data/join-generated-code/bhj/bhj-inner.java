import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.unsafe.types.UTF8String;

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

    // 获得 relation value
    bhj_relation_0 = ((org.apache.spark.sql.execution.joins.UnsafeHashedRelation)
        ((org.apache.spark.broadcast.TorrentBroadcast) references[1] /* broadcast */).value()).asReadOnlyCopy();
    incPeakExecutionMemory(bhj_relation_0.estimatedSize());

    bhj_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 32);
    bhj_mutableStateArray_0[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(4, 64);
  }

  protected void processNext() throws java.io.IOException {
    //依次遍历 streamPlan 所有的输入
    while ( localtablescan_input_0.hasNext()) {
      // 获得当前行
      InternalRow localtablescan_row_0 = (InternalRow) localtablescan_input_0.next();
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);

      // 获得 Join key
      boolean localtablescan_isNull_1 = localtablescan_row_0.isNullAt(1);
      UTF8String localtablescan_value_1 = localtablescan_isNull_1 ?
          null : (localtablescan_row_0.getUTF8String(1));

      // generate join key for stream side
      bhj_mutableStateArray_0[0].reset();
      bhj_mutableStateArray_0[0].zeroOutNullBytes();

      // 将 Join key 写入 UnsafeRow
      if (localtablescan_isNull_1) {
        bhj_mutableStateArray_0[0].setNullAt(0);
      } else {
        bhj_mutableStateArray_0[0].write(0, localtablescan_value_1);
      }

      // find matches from HashRelation
      // 通过 Join Key 在 HashRelation中查找 match 的 行
      scala.collection.Iterator bhj_matches_0 = (bhj_mutableStateArray_0[0].getRow()).anyNull() ?
          null : (scala.collection.Iterator)bhj_relation_0.get((bhj_mutableStateArray_0[0].getRow()));
      if (bhj_matches_0 != null) {
        while (bhj_matches_0.hasNext()) { // buildPlan 有很多 key 相同的row, 依次找出进行 Join
          UnsafeRow bhj_buildRow_0 = (UnsafeRow) bhj_matches_0.next();
          {
            ((org.apache.spark.sql.execution.metric.SQLMetric) references[2] /* numOutputRows */).add(1);
            int localtablescan_value_0 = localtablescan_row_0.getInt(0);
            boolean bhj_isNull_1 = bhj_buildRow_0.isNullAt(0);
            UTF8String bhj_value_1 = bhj_isNull_1 ?
                null : (bhj_buildRow_0.getUTF8String(0));
            int bhj_value_2 = bhj_buildRow_0.getInt(1);

            // 写入 streamPlan Row
            bhj_mutableStateArray_0[1].reset();
            bhj_mutableStateArray_0[1].zeroOutNullBytes();
            bhj_mutableStateArray_0[1].write(0, localtablescan_value_0);

            if (localtablescan_isNull_1) {
              bhj_mutableStateArray_0[1].setNullAt(1);
            } else {
              bhj_mutableStateArray_0[1].write(1, localtablescan_value_1);
            }

            // 写入 buildPlan Row
            if (bhj_isNull_1) {
              bhj_mutableStateArray_0[1].setNullAt(2);
            } else {
              bhj_mutableStateArray_0[1].write(2, bhj_value_1);
            }

            bhj_mutableStateArray_0[1].write(3, bhj_value_2);
            append((bhj_mutableStateArray_0[1].getRow()).copy());
          }
        }
      }
      if (shouldStop()) return;
    }
  }

}
