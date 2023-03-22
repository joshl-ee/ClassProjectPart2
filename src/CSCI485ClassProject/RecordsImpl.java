package CSCI485ClassProject;

import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class RecordsImpl implements Records{

  private Database db;

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

    TableManagerImpl tableManager = new TableManagerImpl();
    HashMap<String, AttributeType> tableAttributes = tableManager.listTables().get(tableName).getAttributes();
    tableManager.closeDatabase();

    // Check if given PKs are correct type
    for (int i = 0; i < primaryKeys.length; i++) {
      Object primaryKey = primaryKeysValues[i];
      if (!(primaryKey instanceof String && tableAttributes.get(primaryKeys[i]) == AttributeType.VARCHAR) &&
          !((primaryKey instanceof Integer || primaryKey instanceof Long) && tableAttributes.get(primaryKeys[i]) == AttributeType.INT) &&
          !(primaryKey instanceof Double && tableAttributes.get(primaryKeys[i]) == AttributeType.DOUBLE)) {
        FDBHelper.abortTransaction(tx);
        FDBHelper.closeTransaction(tx);
        return StatusCode.DATA_RECORD_PRIMARY_KEYS_UNMATCHED;
      }
    }

    // Check if given attributes are correct type
    for (int i = 0; i < attrNames.length; i++) {
      Object attribute = attrValues[i];
      if (!(attribute instanceof String && tableAttributes.get(attrNames[i]) == AttributeType.VARCHAR) &&
              !((attribute instanceof Integer || attribute instanceof Long) && tableAttributes.get(attrNames[i]) == AttributeType.INT) &&
              !(attribute instanceof Double && tableAttributes.get(attrNames[i]) == AttributeType.DOUBLE)) {
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

      // Check if keyAlready exists
      if (FDBHelper.getCertainKeyValuePairInSubdirectory(dir, tx, keyTuple, recordPath) != null) {
        System.out.println(recordPath);
        System.out.println(primaryKeysValues[0]);
        System.out.println(keyTuple.get(0) + " " + keyTuple.get(1));
        FDBHelper.abortTransaction(tx);
        FDBHelper.closeTransaction(tx);
        return StatusCode.DATA_RECORD_CREATION_RECORD_ALREADY_EXISTS;
      }

      // Create valueTuple
      Tuple valueTuple = new Tuple();
      valueTuple.addObject(attrValues[i]);

      // Add to FDB
      FDBHelper.setFDBKVPair(dir, tx, new FDBKVPair(recordPath, keyTuple, valueTuple));
    }

    FDBHelper.commitTransaction(tx);
    FDBHelper.closeTransaction(tx);
    return StatusCode.SUCCESS;
  }

  @Override
  public Cursor openCursor(String tableName, Cursor.Mode mode) {
    Transaction tx = FDBHelper.openTransaction(db);
    Cursor cursor = new Cursor(tableName, mode, db, tx);



    return cursor;
  }


  @Override
  public Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex) {
    Transaction tx = FDBHelper.openTransaction(db);
    Cursor cursor = new Cursor(tableName, attrName, attrValue, operator, mode, isUsingIndex, db, tx);
    return cursor;
  }

  @Override
  public Record getFirst(Cursor cursor) {
    FDBKVPair KVPair = cursor.getFirst();
    if (KVPair == null) {
      return null;
    }
    Record record = new Record();
    Tuple values = KVPair.getValue();
    // TODO: we cannot assume that attributes will be given in order
//    int i = 0;
//    for (String attribute : cursor.getMetadata().getAttributes().keySet()) {
//      record.setAttrNameAndValue(attribute, values.getNestedList(i));
//      i++;
//    }
    return record;
  }

  @Override
  public Record getLast(Cursor cursor) {
    FDBKVPair KVPair = cursor.getLast();
    return null;
  }

  @Override
  public Record getNext(Cursor cursor) {
    return null;
  }

  @Override
  public Record getPrevious(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode updateRecord(Cursor cursor, String[] attrNames, Object[] attrValues) {
    return null;
  }

  @Override
  public StatusCode deleteRecord(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode commitCursor(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode abortCursor(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode deleteDataRecord(String tableName, String[] attrNames, Object[] attrValues) {
    return null;
  }
  @Override
  public void closeDatabase() {
    FDBHelper.close(db);
  }
}
