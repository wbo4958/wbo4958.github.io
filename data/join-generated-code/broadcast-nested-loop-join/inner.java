import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

// codegenStageId=1
final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
  private Object[] references;
  private scala.collection.Iterator[] inputs;
  private scala.collection.Iterator localtablescan_input_0;
  private InternalRow[] bnlj_buildRowArray_0; // build plan 中所有的 InternalRow
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] bnlj_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[1];

  public GeneratedIteratorForCodegenStage1(Object[] references) {
    this.references = references;
  }

  public void init(int index, scala.collection.Iterator[] inputs) {
    partitionIndex = index;
    this.inputs = inputs;
    localtablescan_input_0 = inputs[0];

    // 获得 build plan 的所有的 row
    bnlj_buildRowArray_0 = (InternalRow[]) ((org.apache.spark.broadcast.TorrentBroadcast) references[1] /* broadcastTerm */).value();
    bnlj_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(4, 64);
  }

  protected void processNext() throws java.io.IOException {
    while ( localtablescan_input_0.hasNext()) { // 依次遍历 Stream Plan 所有的行
      InternalRow localtablescan_row_0 = (InternalRow) localtablescan_input_0.next();
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);
      int localtablescan_value_0 = localtablescan_row_0.getInt(0); // 获得 stream plan 的 join key

      for (int bnlj_arrayIndex_0 = 0; bnlj_arrayIndex_0 < bnlj_buildRowArray_0.length; bnlj_arrayIndex_0++) { //依次遍历所有的 build plan 的行
        UnsafeRow bnlj_buildRow_0 = (UnsafeRow) bnlj_buildRowArray_0[bnlj_arrayIndex_0];

        int bnlj_value_1 = bnlj_buildRow_0.getInt(1); // 获得 build plan 的 join key

        boolean bnlj_value_2 = false;
        bnlj_value_2 = localtablescan_value_0 > bnlj_value_1; // 执行 join 条件
        if (!(false || !bnlj_value_2)) //是否满足 join 条件
        {
          ((org.apache.spark.sql.execution.metric.SQLMetric) references[2] /* numOutputRows */).add(1);

          boolean localtablescan_isNull_1 = localtablescan_row_0.isNullAt(1);
          UTF8String localtablescan_value_1 = localtablescan_isNull_1 ?
              null : (localtablescan_row_0.getUTF8String(1));
          boolean bnlj_isNull_0 = bnlj_buildRow_0.isNullAt(0);
          UTF8String bnlj_value_0 = bnlj_isNull_0 ?
              null : (bnlj_buildRow_0.getUTF8String(0));
          bnlj_mutableStateArray_0[0].reset();

          bnlj_mutableStateArray_0[0].zeroOutNullBytes();
          bnlj_mutableStateArray_0[0].write(0, localtablescan_value_0);

          if (localtablescan_isNull_1) {
            bnlj_mutableStateArray_0[0].setNullAt(1);
          } else {
            bnlj_mutableStateArray_0[0].write(1, localtablescan_value_1);
          }

          if (bnlj_isNull_0) {
            bnlj_mutableStateArray_0[0].setNullAt(2);
          } else {
            bnlj_mutableStateArray_0[0].write(2, bnlj_value_0);
          }

          bnlj_mutableStateArray_0[0].write(3, bnlj_value_1);
          append((bnlj_mutableStateArray_0[0].getRow()).copy());

        }
      }
      if (shouldStop()) return;
    }
  }

}

