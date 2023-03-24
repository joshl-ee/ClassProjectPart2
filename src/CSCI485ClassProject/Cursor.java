package CSCI485ClassProject;

import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.sql.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static CSCI485ClassProject.FDBHelper.getAllKeyValuePairsOfSubdirectory;
import static CSCI485ClassProject.FDBHelper.tryCommitTx;
import static java.util.Objects.isNull;

public class Cursor {
  public enum Mode {
    READ,
    READ_WRITE
  }

  // THIS IS CONSTANT SPACE.
  private Mode mode;
  private final String tableName;
  private final DirectorySubspace subspace;
  private final TableMetadata metadata;
  private final List<String> path;
  private final Transaction tx;
  private KeyValue curr;
  // Except this, which holds as a single record
  private List<FDBKVPair> currRecord;
  private Tuple currPK;
  private AsyncIterator<KeyValue> iterator;
  private Boolean startFromBeginning = null;
  private String attrName;
  private Object attrValue;
  private ComparisonOperator operator = null;
  private boolean isUsingIndex;
  public Cursor(String tableName, Cursor.Mode mode, Transaction tx) {
    this.mode = mode;
    this.tableName = tableName;
    TableManager tableManager = new TableManagerImpl();
    this.metadata = tableManager.listTables().get(tableName);
    tableManager.closeDatabase();

    TableMetadataTransformer transformer = new TableMetadataTransformer(tableName);
    this.path = transformer.getTableRecordStorePath();
    this.subspace = FDBHelper.openSubspace(tx, path);
    this.tx = tx;
  }

  public Cursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex, Transaction tx) {
    this.mode = mode;
    this.tableName = tableName;
    TableManager tableManager = new TableManagerImpl();
    this.metadata = tableManager.listTables().get(tableName);
    tableManager.closeDatabase();
    TableMetadataTransformer transformer = new TableMetadataTransformer(tableName);
    this.path = transformer.getTableRecordStorePath();
    this.subspace = FDBHelper.openSubspace(tx, path);
    this.tx = tx;
    this.attrName = attrName;
    this.attrValue = attrValue;
    this.operator = operator;
    this.isUsingIndex = isUsingIndex;
  }

  public List<FDBKVPair> getFirst() {
    // Check if initialization happened already
    if (startFromBeginning != null) return null;
    startFromBeginning = true;

    // Create the iterator
    iterator = initialize(false);

    // Iterates into first key
    List<KeyValue> keyvalueList = new ArrayList<>();
    if (iterator.hasNext()) {
      curr = iterator.next();
    }
    else return null;

    // Find all KeyValues that have the first PK
    currRecord = keyvalueToFDBKVPair(getNextSetOfFDBKVPairs(keyvalueList));
    return currRecord;
  }

  public List<FDBKVPair> getLast() {
    // Check if initialization happened already
    if (startFromBeginning != null) return null;
    startFromBeginning = false;

    // Create the iterator
    iterator = initialize(true);

    // Iterate into first key
    List<KeyValue> keyvalueList = new ArrayList<>();
    if (iterator.hasNext()) {
      curr = iterator.next();
    }
    else return null;

    // Find all KeyValues that have the first PK
    currRecord = keyvalueToFDBKVPair(getNextSetOfFDBKVPairs(keyvalueList));
    return currRecord;
  }

  public List<FDBKVPair> getNext() {
    // Check that traversal protocol is correct
    if (!startFromBeginning) return null;

    // Iterate into next KeyValue with new PK
    if (iterator.hasNext()) {
      List<KeyValue> keyvalueList = new ArrayList<>();
      // Find all KeyValues that have the new PK
      currRecord =  keyvalueToFDBKVPair(getNextSetOfFDBKVPairs(keyvalueList));
      return currRecord;
    }
    else {
      // Else, end is reached.
      currRecord = null;
      return null;
    }
  }

  public List<FDBKVPair> getPrevious() {
    // Check that traversal protocol is correct
    if (startFromBeginning) return null;

    // Iterate into next KeyValue with new PK
    if (iterator.hasNext()) {
      List<KeyValue> keyvalueList = new ArrayList<>();
      // Find all KeyValues that have the new PK
      currRecord =  keyvalueToFDBKVPair(getNextSetOfFDBKVPairs(keyvalueList));
      return currRecord;
    }
    else {
      // Else, end is reached.
      currRecord = null;
      return null;
    }
  }

  public StatusCode delete() {
    // Miscellaneous checks for stuff
    if (mode == Mode.READ) return StatusCode.CURSOR_INVALID;
    if (startFromBeginning == null) return StatusCode.CURSOR_NOT_INITIALIZED;
    if (mode == null) return StatusCode.CURSOR_INVALID;
    if (currRecord == null) return StatusCode.CURSOR_REACH_TO_EOF;

    // Delete the record currently being pointed to
    for (FDBKVPair kvpair : currRecord) {
      FDBHelper.removeKeyValuePair(tx, subspace, kvpair.getKey());
    }
    return StatusCode.SUCCESS;
  }

  public StatusCode update(String[] attrNames, Object[] attrValues) {
    // Miscellaneous checks for stuff
    if (mode == Mode.READ) return StatusCode.CURSOR_INVALID;
    if (startFromBeginning == null) return StatusCode.CURSOR_NOT_INITIALIZED;
    if (mode == null) return StatusCode.CURSOR_INVALID;
    if (currRecord == null) return StatusCode.CURSOR_REACH_TO_EOF;

    // Create a map to hold attributes not yet changed
    HashMap<String, Integer> list = new HashMap<>();
    for (int i = 0; i < attrNames.length; i++) {
      list.put(attrNames[i], i);
    }

    // Update non-key attributes
    for (FDBKVPair kvpair : currRecord) {
      for (int i = 0; i < attrNames.length; i++) {
        if (kvpair.getKey().getString(metadata.getPrimaryKeys().size()).equals(attrNames[i])) {
          FDBHelper.removeKeyValuePair(tx, subspace, kvpair.getKey());
          list.remove(attrNames[i]);
          Tuple newValue = new Tuple().addObject(attrValues[i]);
          FDBHelper.setFDBKVPair(subspace, tx, new FDBKVPair(path, kvpair.getKey(), newValue));
        }
      }
    }

    // Update PK attributes
    for (int i = 0; i < attrNames.length; i++) {
      if (metadata.getPrimaryKeys().contains(attrNames[i])) {
        list.remove(attrNames[i]);
        for (FDBKVPair kvpair : currRecord) {
          FDBHelper.removeKeyValuePair(tx, subspace, kvpair.getKey());
          Tuple newKey = new Tuple().addObject(attrValues[i]).addObject(kvpair.getKey().getString(metadata.getPrimaryKeys().size()));
          Tuple newValue = new Tuple().addObject(kvpair.getValue().get(0));
          FDBHelper.setFDBKVPair(subspace, tx, new FDBKVPair(path, newKey, newValue));
        }
      }
    }

    // Check if new attributes need to be added
    if (list.isEmpty()) return StatusCode.SUCCESS;
    else {
      for (String attribute : list.keySet()) {
        Tuple newKey = new Tuple().addObject(currPK).addObject(attribute);
        Tuple newValue = new Tuple().addObject(attrValues[list.get(attribute)]);
        FDBHelper.setFDBKVPair(subspace, tx, new FDBKVPair(path, newKey, newValue));
      }
      return StatusCode.CURSOR_UPDATE_ATTRIBUTE_NOT_FOUND;
    }
  }

  public void abort() {
    // Abort method
    tx.cancel();
    this.commit();
  }

  private List<KeyValue> getNextSetOfFDBKVPairs( List<KeyValue> keyvalueList) {
    // Get the PK of the new record to obtain
    currPK = getPKFromKeyValue(curr);
    keyvalueList.add(curr);

    // Find all KeyValues of the current PK and add it to list
    boolean newPk = false;
    if (!iterator.hasNext()) return new ArrayList<>();
    while (iterator.hasNext() && !newPk) {
      curr = iterator.next();
      if (getPKFromKeyValue(curr).equals(currPK)) {
        keyvalueList.add(curr);
      }
      else newPk = true;
    }

    // Check if the record conforms to comparison. If no, find the next one.
    if (comparison(keyvalueList)) {
      return keyvalueList;
    }
    else {
      List<KeyValue> keyvalueListRedo = new ArrayList<>();
      return getNextSetOfFDBKVPairs(keyvalueListRedo);
    }
  }

  private boolean comparison(List<KeyValue> keyvalueList) {
    // Ensure that comparison constructor has been called
    if (operator == null) return true;

    // Find the attribute being compared
    String comparedAttribute;
    KeyValue attrKV = null;
    for (KeyValue keyvalue : keyvalueList) {
      // Find the correct kv for attribute
      comparedAttribute = Tuple.fromBytes(keyvalue.getKey()).getString(metadata.getPrimaryKeys().size()+1);
      if (comparedAttribute.equals(attrName)) {
        attrKV = keyvalue;
        break;
      }
    }

    // Check if comparison is being done on primary key. Previous lines will not find it if this is the case.
    Object valueOf = null;
    boolean searchOnPrimary = false;
    for (String pk: metadata.getPrimaryKeys()) {
      if (pk.equals(attrName))  {
        searchOnPrimary = true;
        attrKV = keyvalueList.get(0);
        valueOf = Tuple.fromBytes(keyvalueList.get(0).getKey()).get(1);
      }
    }

    // If attribute doesn't exist, it fails the comparison
    if (attrKV == null) return false;

    // If attribute is not PK, set it as previously found
    if (!searchOnPrimary) {
      valueOf = Tuple.fromBytes(attrKV.getValue()).get(0);
    }

    // Do the comparison, with type checking.
    if (operator == ComparisonOperator.EQUAL_TO) {
      if (valueOf instanceof Integer) {
        return valueOf.equals(attrValue);
      }
      if (valueOf instanceof Long) {
        if (attrValue instanceof Integer)  return ((Long) valueOf).intValue() == (Integer) attrValue;
        return ((Long) valueOf).longValue() == ((Long) attrValue).longValue();
      }
      if (valueOf instanceof Double) {
        return valueOf.equals(attrValue);
      }
      if (valueOf instanceof String) {
        return ((String) valueOf).compareTo((String) attrValue) == 0;
      }
    }
    if (operator == ComparisonOperator.GREATER_THAN) {
      if (valueOf instanceof Integer) {
        return (Integer) valueOf > (Integer) attrValue;
      }
      if (valueOf instanceof Long) {
        if (attrValue instanceof Integer)  return (Long) valueOf >= (Integer) attrValue;
        return (Long) valueOf > (Long) attrValue;
      }
      if (valueOf instanceof Double) {
        return (Double) valueOf > (Double) attrValue;
      }
      if (valueOf instanceof String) {
        return ((String) valueOf).compareTo((String) attrValue) > 0;
      }
    }
    if (operator == ComparisonOperator.LESS_THAN) {
      if (valueOf instanceof Integer) {
        return (Integer) valueOf < (Integer) attrValue;
      }
      if (valueOf instanceof Long) {
        if (attrValue instanceof Integer)  return (Long) valueOf >= (Integer) attrValue;
        return (Long) valueOf < (Long) attrValue;
      }
      if (valueOf instanceof Double) {
        return (Double) valueOf < (Double) attrValue;
      }
      if (valueOf instanceof String) {
        return ((String) valueOf).compareTo((String) attrValue) < 0;
      }
    }
    if (operator == ComparisonOperator.LESS_THAN_OR_EQUAL_TO) {
      if (valueOf instanceof Integer) {
        return (Integer) valueOf <= (Integer) attrValue;
      }
      if (valueOf instanceof Long) {
        if (attrValue instanceof Integer)  return (Long) valueOf >= (Integer) attrValue;
        return (Long) valueOf <= (Long) attrValue;
      }
      if (valueOf instanceof Double) {
        return (Double) valueOf <= (Double) attrValue;
      }
      if (valueOf instanceof String) {
        return ((String) valueOf).compareTo((String) attrValue) <= 0;
      }
    }
    if (operator == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO) {
      if (valueOf instanceof Integer) {
        return (Integer) valueOf >= (Integer) attrValue;
      }
      if (valueOf instanceof Long) {
        if (attrValue instanceof Integer)  return (Long) valueOf >= (Integer) attrValue;
        return (Long) valueOf >= (Long) attrValue;
      }
      if (valueOf instanceof Double) {
        return (Double) valueOf >= (Double) attrValue;
      }
      if (valueOf instanceof String) {
        return ((String) valueOf).compareTo((String) attrValue) >= 0;
      }
    }
    return false;
  }
  private Tuple getPKFromKeyValue(KeyValue keyvalue) {
    // Get PK from KeyValue object
    Tuple pk = new Tuple();
    for (int i = 0; i < metadata.getPrimaryKeys().size(); i++) {
      pk = pk.addObject(Tuple.fromBytes(keyvalue.getKey()).popFront().get(i));
    }
    return pk;
  }

  private AsyncIterator<KeyValue> initialize(boolean reverse) {
    // Initialize the iterator.
    Range range = subspace.range();
    AsyncIterable<KeyValue> iterable = tx.getRange(range, ReadTransaction.ROW_LIMIT_UNLIMITED, reverse);
    return iterable.iterator();
  }

  private List<FDBKVPair> keyvalueToFDBKVPair(List<KeyValue> keyvalueList) {
    // Method to change List<KeyValue> to List<FDBKVPair>
    List<FDBKVPair> FDBKVPairList = new ArrayList<>();
    for (KeyValue keyvalue : keyvalueList) {
      Tuple keyTuple = Tuple.fromBytes(keyvalue.getKey()).popFront();
      Tuple valueTuple = Tuple.fromBytes(keyvalue.getValue());
      FDBKVPairList.add(new FDBKVPair(path, keyTuple, valueTuple));
    }
    if (FDBKVPairList.size() == 0) return null;
    return FDBKVPairList;
  }

  public String getTableName() {
    return tableName;
  }

  public boolean commit() {
    // Commit.
    boolean success = tryCommitTx(tx, 20);

    if (success) {
      // These ensure that calling any methods on a committed tx will throw error.
      iterator = null;
      operator = null;
      mode = null;
      return true;
    }
    return false;
  }
}

// Best mashed potatoes recipe.
// 685 Grams russet potatoes
// 1 tablespoon butter
// quarter cup milk
//  salt (2-3 tsp or to taste)
//  garlic powder (1.5 tsp or to taste)
//  MSG (~1 tsp)

// 740 grams of potatoes 1 calorie per gram
