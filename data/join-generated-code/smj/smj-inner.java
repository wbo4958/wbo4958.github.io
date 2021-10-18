// codegenStageId=1
// 对 streamPlan shuffle后的数据按照 join key 进行排序,
final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
  private Object[] references;
  private scala.collection.Iterator[] inputs;
  private boolean sort_needToSort_0;
  private org.apache.spark.sql.execution.UnsafeExternalRowSorter sort_sorter_0;
  private org.apache.spark.executor.TaskMetrics sort_metrics_0;
  private scala.collection.Iterator<UnsafeRow> sort_sortedIter_0;
  private scala.collection.Iterator inputadapter_input_0;

  public GeneratedIteratorForCodegenStage1(Object[] references) {
    this.references = references;
  }

  public void init(int index, scala.collection.Iterator[] inputs) {
    partitionIndex = index;
    this.inputs = inputs;
    sort_needToSort_0 = true;
    sort_sorter_0 = ((org.apache.spark.sql.execution.SortExec) references[0] /* plan */).createSorter();
    sort_metrics_0 = org.apache.spark.TaskContext.get().taskMetrics();

    inputadapter_input_0 = inputs[0];

  }

  private void sort_addToSorter_0() throws java.io.IOException {
    while ( inputadapter_input_0.hasNext()) { //对输入数据进行排序
      InternalRow inputadapter_row_0 = (InternalRow) inputadapter_input_0.next();

      sort_sorter_0.insertRow((UnsafeRow)inputadapter_row_0); //采用 UnsafeExternalRowSoter 对输入数据进行排序
      // shouldStop check is eliminated
    }

  }

  protected void processNext() throws java.io.IOException {
    if (sort_needToSort_0) {
      long sort_spillSizeBefore_0 = sort_metrics_0.memoryBytesSpilled();
      sort_addToSorter_0();
      sort_sortedIter_0 = sort_sorter_0.sort();
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[3] /* sortTime */).add(sort_sorter_0.getSortTimeNanos() / 1000000);
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[1] /* peakMemory */).add(sort_sorter_0.getPeakMemoryUsage());
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[2] /* spillSize */).add(sort_metrics_0.memoryBytesSpilled() - sort_spillSizeBefore_0);
      sort_metrics_0.incPeakExecutionMemory(sort_sorter_0.getPeakMemoryUsage());
      sort_needToSort_0 = false;
    }

    while ( sort_sortedIter_0.hasNext()) { //依次获得排序后的结果
      UnsafeRow sort_outputRow_0 = (UnsafeRow)sort_sortedIter_0.next();

      append(sort_outputRow_0);

      if (shouldStop()) return;
    }
  }

}

// codegenStageId=2
// 对 buildPlan shuffle后的数据按照 join key 进行排序,
final class GeneratedIteratorForCodegenStage2 extends org.apache.spark.sql.execution.BufferedRowIterator {
  private Object[] references;
  private scala.collection.Iterator[] inputs;
  private boolean sort_needToSort_0;
  private org.apache.spark.sql.execution.UnsafeExternalRowSorter sort_sorter_0;
  private org.apache.spark.executor.TaskMetrics sort_metrics_0;
  private scala.collection.Iterator<UnsafeRow> sort_sortedIter_0;
  private scala.collection.Iterator inputadapter_input_0;

  public GeneratedIteratorForCodegenStage2(Object[] references) {
    this.references = references;
  }

  public void init(int index, scala.collection.Iterator[] inputs) {
    partitionIndex = index;
    this.inputs = inputs;
    sort_needToSort_0 = true;
    sort_sorter_0 = ((org.apache.spark.sql.execution.SortExec) references[0] /* plan */).createSorter();
    sort_metrics_0 = org.apache.spark.TaskContext.get().taskMetrics();

    inputadapter_input_0 = inputs[0];

  }

  private void sort_addToSorter_0() throws java.io.IOException {
    while ( inputadapter_input_0.hasNext()) { //采用 UnsafeExternalRowSoter 对输入数据进行排序
      InternalRow inputadapter_row_0 = (InternalRow) inputadapter_input_0.next();

      sort_sorter_0.insertRow((UnsafeRow)inputadapter_row_0);
      // shouldStop check is eliminated
    }

  }

  protected void processNext() throws java.io.IOException {
    if (sort_needToSort_0) {
      long sort_spillSizeBefore_0 = sort_metrics_0.memoryBytesSpilled();
      sort_addToSorter_0();
      sort_sortedIter_0 = sort_sorter_0.sort();
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[3] /* sortTime */).add(sort_sorter_0.getSortTimeNanos() / 1000000);
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[1] /* peakMemory */).add(sort_sorter_0.getPeakMemoryUsage());
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[2] /* spillSize */).add(sort_metrics_0.memoryBytesSpilled() - sort_spillSizeBefore_0);
      sort_metrics_0.incPeakExecutionMemory(sort_sorter_0.getPeakMemoryUsage());
      sort_needToSort_0 = false;
    }

    while ( sort_sortedIter_0.hasNext()) {
      UnsafeRow sort_outputRow_0 = (UnsafeRow)sort_sortedIter_0.next();

      append(sort_outputRow_0);

      if (shouldStop()) return;
    }
  }

}
// codegenStageId=3
final class GeneratedIteratorForCodegenStage3 extends org.apache.spark.sql.execution.BufferedRowIterator {
  private Object[] references;
  private scala.collection.Iterator[] inputs;
  private scala.collection.Iterator smj_streamedInput_0;
  private scala.collection.Iterator smj_bufferedInput_0;
  private InternalRow smj_streamedRow_0;
  private InternalRow smj_bufferedRow_0;
  private org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray smj_matches_0;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] smj_mutableStateArray_1 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[1];
  private UTF8String[] smj_mutableStateArray_0 = new UTF8String[2];

  public GeneratedIteratorForCodegenStage3(Object[] references) {
    this.references = references;
  }

  public void init(int index, scala.collection.Iterator[] inputs) {
    partitionIndex = index;
    this.inputs = inputs;
    smj_streamedInput_0 = inputs[0];
    smj_bufferedInput_0 = inputs[1];

    smj_matches_0 = new org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray(2147483632, 2147483647);
    smj_mutableStateArray_1[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(4, 64);

  }

  private boolean smj_findNextJoinRows_0(
    scala.collection.Iterator streamedIter,
    scala.collection.Iterator bufferedIter) {
    smj_streamedRow_0 = null;
    int comp = 0;
    while (smj_streamedRow_0 == null) {
      if (!streamedIter.hasNext()) return false;
      smj_streamedRow_0 = (InternalRow) streamedIter.next();
      boolean smj_isNull_0 = smj_streamedRow_0.isNullAt(1); // streamPlan 的 join key 是否为null
      UTF8String smj_value_0 = smj_isNull_0 ?  null : (smj_streamedRow_0.getUTF8String(1));
      if (smj_isNull_0) { // streamPlan 的 join key 为 null，直接 pass
        smj_streamedRow_0 = null;
        continue;

      }

      // smj_matches_0 保存的是 buildPlan 中所有和 streamPlan 当前行相等的 join key 的所有的行
      if (!smj_matches_0.isEmpty()) {
        comp = 0;
        if (comp == 0) {
          // streamPaln和buildPlan当前行的 join key 是不是一样
          comp = smj_value_0.compare(smj_mutableStateArray_0[1]); // smj_mutableStateArray_0 表示当前 buildPlan的行
        }

        if (comp == 0) {
          return true;
        }
        smj_matches_0.clear();
      }

      do {
        if (smj_bufferedRow_0 == null) {
          if (!bufferedIter.hasNext()) { // 是否 buildPlan 没有数据了
            smj_mutableStateArray_0[1] = smj_value_0.clone();
            return !smj_matches_0.isEmpty();
          }
          smj_bufferedRow_0 = (InternalRow) bufferedIter.next(); //获得 buildPlan的行
          boolean smj_isNull_1 = smj_bufferedRow_0.isNullAt(0);
          UTF8String smj_value_1 = smj_isNull_1 ?  null : (smj_bufferedRow_0.getUTF8String(0));
          if (smj_isNull_1) { // 取出的 buildPlan 的行的数据为null
            smj_bufferedRow_0 = null;
            continue;
          }
          smj_mutableStateArray_0[0] = smj_value_1.clone(); //buildPlan 的join key
        }

        comp = 0;
        if (comp == 0) {
          comp = smj_value_0.compare(smj_mutableStateArray_0[0]); //比较 streamPaln和buildPlan当前行的 join key 是不是一样
        }

        if (comp > 0) {
          // streamPlan 超前了
          smj_bufferedRow_0 = null;
        } else if (comp < 0) {
          if (!smj_matches_0.isEmpty()) {
            smj_mutableStateArray_0[1] = smj_value_0.clone();
            return true;
          } else {
            smj_streamedRow_0 = null;
          }
        } else {
          // match 了
          smj_matches_0.add((UnsafeRow) smj_bufferedRow_0);
          smj_bufferedRow_0 = null;
        }
      } while (smj_streamedRow_0 != null);
    }
    return false; // unreachable
  }

  protected void processNext() throws java.io.IOException {
    while (smj_findNextJoinRows_0(smj_streamedInput_0, smj_bufferedInput_0)) {
      int smj_value_2 = -1;

      boolean smj_isNull_2 = false;
      UTF8String smj_value_3 = null;

      smj_value_2 = smj_streamedRow_0.getInt(0);
      smj_isNull_2 = smj_streamedRow_0.isNullAt(1);
      smj_value_3 = smj_isNull_2 ? null : (smj_streamedRow_0.getUTF8String(1));
      scala.collection.Iterator<UnsafeRow> smj_iterator_0 = smj_matches_0.generateIterator();

      while (smj_iterator_0.hasNext()) {
        InternalRow smj_bufferedRow_1 = (InternalRow) smj_iterator_0.next();

        ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);

        boolean smj_isNull_3 = smj_bufferedRow_1.isNullAt(0);
        UTF8String smj_value_4 = smj_isNull_3 ?
        null : (smj_bufferedRow_1.getUTF8String(0));
        int smj_value_5 = smj_bufferedRow_1.getInt(1);
        smj_mutableStateArray_1[0].reset();

        smj_mutableStateArray_1[0].zeroOutNullBytes();

        smj_mutableStateArray_1[0].write(0, smj_value_2);

        if (smj_isNull_2) {
          smj_mutableStateArray_1[0].setNullAt(1);
        } else {
          smj_mutableStateArray_1[0].write(1, smj_value_3);
        }

        if (smj_isNull_3) {
          smj_mutableStateArray_1[0].setNullAt(2);
        } else {
          smj_mutableStateArray_1[0].write(2, smj_value_4);
        }

        smj_mutableStateArray_1[0].write(3, smj_value_5);
        append((smj_mutableStateArray_1[0].getRow()).copy());

      }
      if (shouldStop()) return;
    }
    ((org.apache.spark.sql.execution.joins.SortMergeJoinExec) references[1] /* plan */).cleanupResources();
  }

}
