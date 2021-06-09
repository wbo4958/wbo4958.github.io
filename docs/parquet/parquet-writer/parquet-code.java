package org.apache.parquet.example.data;

public class GroupWriter {
  public void write(Group group) {
    recordConsumer.startMessage();
    writeGroup(group, schema);
    recordConsumer.endMessage();
  }

  private void writeGroup(Group group, GroupType type) {
    int fieldCount = type.getFieldCount(); //将 group数据写入到parquet文件中
    for (int field = 0; field < fieldCount; ++field) { //group中有多少个field
      int valueCount = group.getFieldRepetitionCount(field); //获得field的重复出现的个数
      if (valueCount > 0) {
        Type fieldType = type.getType(field); //获得field的类型
        String fieldName = fieldType.getName();
        recordConsumer.startField(fieldName, field); //开始写 field
        for (int index = 0; index < valueCount; ++index) { //将重复的数据依次写入
          if (fieldType.isPrimitive()) { //如果 field是primitive type,直接写
            group.writeValue(field, index, recordConsumer);
          } else { //如果 field 是 group, 则进入 writeGroup
            recordConsumer.startGroup();
            writeGroup(group.getGroup(field, index), fieldType.asGroupType());
            recordConsumer.endGroup();
          }
        }
        recordConsumer.endField(fieldName, field);
      }
    }
  }
}

/**
 * Message level of the IO structure
 */
public class MessageColumnIO extends GroupColumnIO {

    @Override
    public void startMessage() {
      currentColumnIO = MessageColumnIO.this; //更新到Root schema 的 MessageColumnIO
      r[0] = 0; //设置 repeptition level [0] = 0
      int numberOfFieldsToVisit = ((GroupColumnIO) currentColumnIO).getChildrenCount();
      // fieldsWritten 表示每个 group的 field是否已经写过了
      fieldsWritten[0].reset(numberOfFieldsToVisit);
    }

    @Override
    public void endMessage() {
      //是否需要写入 null
      writeNullForMissingFieldsAtCurrentLevel();

      // We need to flush the cached null values before ending the record to ensure that everything is sent to the
      // writer before the current page would be closed
      // flush到parquet文件当中
      if (columns.isColumnFlushNeeded()) {
        flush();
      }

      columns.endRecord();
    }

    @Override
    public void startField(String field, int index) {
      try {
        currentColumnIO = ((GroupColumnIO) currentColumnIO).getChild(index);
        emptyField = true;
      } catch (RuntimeException e) {
        throw new ParquetEncodingException("error starting field " + field + " at " + index, e);
      }
    }

    @Override
    public void endField(String field, int index) {
      // 结束一个 field的写入
      currentColumnIO = currentColumnIO.getParent(); //回到field 的parent
      if (emptyField) {
        throw new ParquetEncodingException("empty fields are illegal, the field should be ommited completely instead");
      }
      fieldsWritten[currentLevel].markWritten(index); // mark 当前 group 的 field已经写过了
      //将 repetition 回退到 parent 的 repetition
      r[currentLevel] = currentLevel == 0 ? 0 : r[currentLevel - 1];
    }

    private void writeNullForMissingFieldsAtCurrentLevel() {
      int currentFieldsCount = ((GroupColumnIO) currentColumnIO).getChildrenCount();
      for (int i = 0; i < currentFieldsCount; i++) {
        //如果某个字段没有写过，那该字段需要写入null, 其实主要是写入 repetition 和 definition
        if (!fieldsWritten[currentLevel].isWritten(i)) { 
          try {
            ColumnIO undefinedField = ((GroupColumnIO) currentColumnIO).getChild(i);
            int d = currentColumnIO.getDefinitionLevel(); //获得是在哪个 level 的null
            writeNull(undefinedField, r[currentLevel], d);
          } catch (RuntimeException e) {
            throw new ParquetEncodingException("error while writing nulls for fields of indexes " + i + " . current index: " + fieldsWritten[currentLevel], e);
          }
        }
      }
    }

    private void writeNull(ColumnIO undefinedField, int r, int d) {
      if (undefinedField.getType().isPrimitive()) {
        // 将 null 的 r/d 写入到 repetition/definition column 中
        columnWriter[((PrimitiveColumnIO) undefinedField).getId()].writeNull(r, d);
      } else {
        GroupColumnIO groupColumnIO = (GroupColumnIO) undefinedField;
        // only cache the repetition level, the definition level should always be the definition level of the parent node
        cacheNullForGroup(groupColumnIO, r);
      }
    }

    private void cacheNullForGroup(GroupColumnIO group, int r) {
      IntArrayList nulls = groupNullCache.get(group);
      if (nulls == null) {
        nulls = new IntArrayList();
        groupNullCache.put(group, nulls);
      }
      nulls.add(r);
    }

    private void writeNullToLeaves(GroupColumnIO group) {
      IntArrayList nullCache = groupNullCache.get(group);
      if (nullCache == null || nullCache.isEmpty())
        return;

      int parentDefinitionLevel = group.getParent().getDefinitionLevel();
      for (ColumnWriter leafWriter : groupToLeafWriter.get(group)) {
        for (IntIterator iter = nullCache.iterator(); iter.hasNext();) {
          int repetitionLevel = iter.nextInt();
          leafWriter.writeNull(repetitionLevel, parentDefinitionLevel);
        }
      }
      nullCache.clear();
    }

    private void setRepetitionLevel() {
      // 设置当前的 repetition level
      r[currentLevel] = currentColumnIO.getRepetitionLevel();
    }

    @Override
    public void startGroup() {
      // 开始写一个 group
      GroupColumnIO group = (GroupColumnIO) currentColumnIO;

      // current group is not null, need to flush all the nulls that were cached before
      // 当前group不为null, 将之前缓存的null group刷到parquet中
      // 需要将该group中所有字段写入 null对应的 repetition和definition
      if (hasNullCache(group)) {
        flushCachedNulls(group);
      }

      ++currentLevel; //更新 repetition level
      r[currentLevel] = r[currentLevel - 1];

      int fieldsCount = ((GroupColumnIO) currentColumnIO).getChildrenCount();
      fieldsWritten[currentLevel].reset(fieldsCount);
    }

    private boolean hasNullCache(GroupColumnIO group) {
      IntArrayList nulls = groupNullCache.get(group);
      return nulls != null && !nulls.isEmpty();
    }


    private void flushCachedNulls(GroupColumnIO group) {
      //flush children first
      for (int i = 0; i < group.getChildrenCount(); i++) {
        ColumnIO child = group.getChild(i);
        if (child instanceof GroupColumnIO) {
          flushCachedNulls((GroupColumnIO) child);
        }
      }
      //then flush itself
      writeNullToLeaves(group);
    }

    @Override
    public void endGroup() {
      // 结束一个 group 的写入
      emptyField = false;
      writeNullForMissingFieldsAtCurrentLevel();
      --currentLevel; //退到上一层

      setRepetitionLevel(); //更新 repetition level
    }

    private ColumnWriter getColumnWriter() {
      return columnWriter[((PrimitiveColumnIO) currentColumnIO).getId()];
    }

    /**
     * Flush null for all groups
     */
    @Override
    public void flush() {
      flushCachedNulls(MessageColumnIO.this);
    }
}
