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

    bhj_relation_0 = ((org.apache.spark.sql.execution.joins.UnsafeHashedRelation) ((org.apache.spark.broadcast.TorrentBroadcast) references[1] /* broadcast */).value()).asReadOnlyCopy();
    incPeakExecutionMemory(bhj_relation_0.estimatedSize());

    bhj_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 32);
    bhj_mutableStateArray_0[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(4, 64);

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
      boolean bhj_found_0 = false;
      // the last iteration of this loop is to emit an empty row if there is no matched rows.
      // bhj_found_0=false, 即将 buildPlan 没有对应的 row 时，此时加上 null
      while (bhj_matches_0 != null && bhj_matches_0.hasNext() || !bhj_found_0) {
        UnsafeRow bhj_matched_0 = bhj_matches_0 != null && bhj_matches_0.hasNext() ?
            (UnsafeRow) bhj_matches_0.next() : null;
        final boolean bhj_conditionPassed_0 = true;
        if (bhj_conditionPassed_0) {
          bhj_found_0 = true;
          ((org.apache.spark.sql.execution.metric.SQLMetric) references[2] /* numOutputRows */).add(1);

          int localtablescan_value_0 = localtablescan_row_0.getInt(0);
          boolean bhj_isNull_2 = true;
          UTF8String bhj_value_2 = null; // 针对为 buildPlan 中没有对应 key 的情况
          if (bhj_matched_0 != null) {
            boolean bhj_isNull_1 = bhj_matched_0.isNullAt(0);
            UTF8String bhj_value_1 = bhj_isNull_1 ?
                null : (bhj_matched_0.getUTF8String(0));
            bhj_isNull_2 = bhj_isNull_1;
            bhj_value_2 = bhj_value_1;
          }
          boolean bhj_isNull_4 = true;
          int bhj_value_4 = -1;
          if (bhj_matched_0 != null) {
            int bhj_value_3 = bhj_matched_0.getInt(1);
            bhj_isNull_4 = false;
            bhj_value_4 = bhj_value_3;
          }
          bhj_mutableStateArray_0[1].reset();
          bhj_mutableStateArray_0[1].zeroOutNullBytes();
          bhj_mutableStateArray_0[1].write(0, localtablescan_value_0);

          if (localtablescan_isNull_1) {
            bhj_mutableStateArray_0[1].setNullAt(1);
          } else {
            bhj_mutableStateArray_0[1].write(1, localtablescan_value_1);
          }

          if (bhj_isNull_2) {
            bhj_mutableStateArray_0[1].setNullAt(2);
          } else {
            bhj_mutableStateArray_0[1].write(2, bhj_value_2);
          }

          if (bhj_isNull_4) {
            bhj_mutableStateArray_0[1].setNullAt(3);
          } else {
            bhj_mutableStateArray_0[1].write(3, bhj_value_4);
          }
          append((bhj_mutableStateArray_0[1].getRow()).copy());

        }
      }
      if (shouldStop()) return;
    }
  }

}
