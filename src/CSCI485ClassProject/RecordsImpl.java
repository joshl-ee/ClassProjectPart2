package CSCI485ClassProject;

import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.sql.Array;
import java.util.*;

public class RecordsImpl implements Records{

  private final Database db;

  public RecordsImpl() {
    db = FDBHelper.initialization();
  }

  @Override
  public StatusCode insertRecord(String tableName, String[] primaryKeys, Object[] primaryKeysValues, String[] attrNames, Object[] attrValues) {
    Transaction tx = FDBHelper.openTransaction(db);
    // Check if table exists
    if (!FDBHelper.doesSubdirectoryExists(tx, Collections.singletonList(tableName))) {
      FDBHelper.abortTransaction(tx);
      FDBHelper.closeTransaction(tx);
      return StatusCode.TABLE_NOT_FOUND;
    }

    // Check if given primary keys are valid
    if (primaryKeys == null || primaryKeysValues == null || primaryKeys.length == 0 || primaryKeysValues.length == 0 || primaryKeys.length != primaryKeysValues.length) {
      FDBHelper.abortTransaction(tx);
      FDBHelper.closeTransaction(tx);
      return StatusCode.DATA_RECORD_PRIMARY_KEYS_UNMATCHED;
    }

    // Check if given attributes are valid
    if (attrNames == null || attrValues == null || attrNames.length == 0 || attrValues.length == 0 || attrNames.length != attrValues.length) {
      FDBHelper.abortTransaction(tx);
      FDBHelper.closeTransaction(tx);
      return StatusCode.DATA_RECORD_CREATION_ATTRIBUTES_INVALID;
    }

    // Add newly introduced attributes
    TableManagerImpl tableManager = new TableManagerImpl();
    HashMap<String, AttributeType> tableAttributes = tableManager.listTables().get(tableName).getAttributes();
    HashMap<String, Object> newAttributes = new HashMap<>();
    for (int i = 0; i < attrNames.length; i++){
      newAttributes.put(attrNames[i], attrValues[i]);
    }
    for (String attribute : attrNames) {
      if (tableAttributes.containsKey(attribute)) {
        newAttributes.remove(attribute);
      }
    }
    // Set type of new attributes
    for (String newAttribute : newAttributes.keySet()) {
      if (newAttributes.get(newAttribute) instanceof Integer) tableManager.addAttribute(tableName, newAttribute, AttributeType.INT);
      if (newAttributes.get(newAttribute) instanceof Long) tableManager.addAttribute(tableName, newAttribute, AttributeType.INT);
      if (newAttributes.get(newAttribute) instanceof Double) tableManager.addAttribute(tableName, newAttribute, AttributeType.DOUBLE);
      if (newAttributes.get(newAttribute) instanceof String) tableManager.addAttribute(tableName, newAttribute, AttributeType.VARCHAR);
    }
    HashMap<String, AttributeType> tableAttributesNew = tableManager.listTables().get(tableName).getAttributes();
    tableManager.closeDatabase();

    // Check if given PKs are correct type
    for (int i = 0; i < primaryKeys.length; i++) {
      Object primaryKey = primaryKeysValues[i];
      if (!(primaryKey instanceof String && tableAttributesNew.get(primaryKeys[i]) == AttributeType.VARCHAR) &&
          !((primaryKey instanceof Integer || primaryKey instanceof Long) && tableAttributesNew.get(primaryKeys[i]) == AttributeType.INT) &&
          !(primaryKey instanceof Double && tableAttributesNew.get(primaryKeys[i]) == AttributeType.DOUBLE)) {
        FDBHelper.abortTransaction(tx);
        FDBHelper.closeTransaction(tx);
        return StatusCode.DATA_RECORD_PRIMARY_KEYS_UNMATCHED;
      }
    }

    // Check if given attributes are correct type
    for (int i = 0; i < attrNames.length; i++) {
      Object attribute = attrValues[i];
      if (!(attribute instanceof String && tableAttributesNew.get(attrNames[i]) == AttributeType.VARCHAR) &&
              !((attribute instanceof Integer || attribute instanceof Long) && tableAttributesNew.get(attrNames[i]) == AttributeType.INT) &&
              !(attribute instanceof Double && tableAttributesNew.get(attrNames[i]) == AttributeType.DOUBLE)) {
        FDBHelper.abortTransaction(tx);
        FDBHelper.closeTransaction(tx);
        return StatusCode.DATA_RECORD_CREATION_ATTRIBUTE_TYPE_UNMATCHED;
      }
    }

    // Pre-work. Get directory information.
    TableMetadataTransformer transformer = new TableMetadataTransformer(tableName);
    List<String> recordPath = transformer.getTableRecordStorePath();
    DirectorySubspace dir = FDBHelper.openSubspace(tx, recordPath);

    // For each attribute make KVPair where (K, V) -> (PK + AttributeName, AttibuteValue)
    for (int i = 0; i < attrNames.length; i++) {
      // Create keyTuple
      Tuple keyTuple = new Tuple();
      for (Object primaryKey : primaryKeysValues) {
        keyTuple = keyTuple.addObject(primaryKey);
      }
      keyTuple = keyTuple.addObject(attrNames[i]);

      // Check if key already exists
      if (FDBHelper.getCertainKeyValuePairInSubdirectory(dir, tx, keyTuple, recordPath) != null) {
//        System.out.println(recordPath);
//        System.out.println(primaryKeysValues[0]);
//        System.out.println(keyTuple.get(0) + " " + keyTuple.get(1));
        FDBHelper.abortTransaction(tx);
        FDBHelper.closeTransaction(tx);
        return StatusCode.DATA_RECORD_CREATION_RECORD_ALREADY_EXISTS;
      }

      // Create valueTuple
      Tuple valueTuple = new Tuple();
      valueTuple = valueTuple.addObject(attrValues[i]);

      // Add to FDB
      FDBHelper.setFDBKVPair(dir, tx, new FDBKVPair(recordPath, keyTuple, valueTuple));
    }

    // Done
    FDBHelper.commitTransaction(tx);
    FDBHelper.closeTransaction(tx);
    return StatusCode.SUCCESS;
  }

  @Override
  public Cursor openCursor(String tableName, Cursor.Mode mode) {
    // Call cursor const. method and assign a tx to it
    Transaction tx = FDBHelper.openTransaction(db);
    return new Cursor(tableName, mode, tx);
  }


  @Override
  public Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex) {
    // Call cursor const. method and assign a tx to it
    Transaction tx = FDBHelper.openTransaction(db);
    return new Cursor(tableName, attrName, attrValue, operator, mode, isUsingIndex, tx);
  }

  @Override
  public Record getFirst(Cursor cursor) {
    // Call cursor getFirst method
    List<FDBKVPair> KVPair = cursor.getFirst();
    if (KVPair == null) {
      return null;
    }
    return KVPairListToRecord(KVPair, cursor.getTableName());
  }

  @Override
  public Record getLast(Cursor cursor) {
    // Call cursor getLast method
    List<FDBKVPair> KVPair = cursor.getLast();
    if (KVPair == null) {
      return null;
    }
    return KVPairListToRecord(KVPair, cursor.getTableName());
  }

  public Record KVPairListToRecord(List<FDBKVPair> KVPair, String tableName) {
    // Go from List<FDBFVPair> to Record
    Record record = new Record();

    // Set primary keys
    TableManagerImpl tableManager = new TableManagerImpl();
    List<String> primaryKeys = tableManager.listTables().get(tableName).getPrimaryKeys();
    tableManager.closeDatabase();
    int i;
    for (i = 0; i < primaryKeys.size(); i++) {
      record.setAttrNameAndValue(primaryKeys.get(i), KVPair.get(i).getKey().get(i));
    }

    // Set attributes
    for (FDBKVPair kvpair: KVPair) {
      record.setAttrNameAndValue(kvpair.getKey().getString(i), kvpair.getValue().get(0));
    }
    return record;
  }
  @Override
  public Record getNext(Cursor cursor) {
    // Call cursor method getNext
    List<FDBKVPair> KVPair = cursor.getNext();
    if (KVPair == null) {
      return null;
    }

    return KVPairListToRecord(KVPair, cursor.getTableName());
  }

  @Override
  public Record getPrevious(Cursor cursor) {
    // Call cursor method getPrevious
    List<FDBKVPair> KVPair = cursor.getPrevious();
    if (KVPair == null) {
      return null;
    }

    return KVPairListToRecord(KVPair, cursor.getTableName());
  }

  @Override
  public StatusCode updateRecord(Cursor cursor, String[] attrNames, Object[] attrValues) {
    // Call cursor method update
    return cursor.update(attrNames, attrValues);
  }

  @Override
  public StatusCode deleteRecord(Cursor cursor) {
    // Call cursor method delete
    return cursor.delete();
  }

  @Override
  public StatusCode commitCursor(Cursor cursor) {
    // Commit cursor
    boolean committed = cursor.commit();
    if (committed) return StatusCode.SUCCESS;
    else return StatusCode.CURSOR_INVALID;
  }

  @Override
  public StatusCode abortCursor(Cursor cursor) {
    // Call abort method
    cursor.abort();
    return null;
  }

  @Override
  public StatusCode deleteDataRecord(String tableName, String[] attrNames, Object[] attrValues) {
    return null;
  }
  @Override
  public void closeDatabase() {
    // Custom method to close database
    FDBHelper.close(db);
  }
}
