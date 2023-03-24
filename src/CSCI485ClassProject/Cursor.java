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
import java.util.List;

import static CSCI485ClassProject.FDBHelper.getAllKeyValuePairsOfSubdirectory;
import static CSCI485ClassProject.FDBHelper.tryCommitTx;
import static java.util.Objects.isNull;

public class Cursor {
  public enum Mode {
    READ,
    READ_WRITE
  }

  // your code here
  private final Mode mode;
  private final String tableName;
  private final DirectorySubspace subspace;
  private final TableMetadata metadata;
  private final List<String> path;
  private final Transaction tx;
  private final Database db;
  private KeyValue curr;
  private List<FDBKVPair> currRecord;
  private Tuple currPK;
  private AsyncIterator<KeyValue> iterator;
  private Boolean startFromBeginning = null;
  private String attrName;
  private Object attrValue;
  private ComparisonOperator operator = null;
  private boolean isUsingIndex;
  public Cursor(String tableName, Cursor.Mode mode, Database db, Transaction tx) {
    this.mode = mode;
    this.tableName = tableName;
    TableManager tableManager = new TableManagerImpl();
    this.metadata = tableManager.listTables().get(tableName);
    tableManager.closeDatabase();

    TableMetadataTransformer transformer = new TableMetadataTransformer(tableName);
    this.path = transformer.getTableRecordStorePath();
    this.subspace = FDBHelper.openSubspace(tx, path);
    this.tx = tx;
    this.db = db;
  }

  public Cursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex, Database db, Transaction tx) {
    this.mode = mode;
    this.tableName = tableName;
    TableManager tableManager = new TableManagerImpl();
    this.metadata = tableManager.listTables().get(tableName);
    tableManager.closeDatabase();
    TableMetadataTransformer transformer = new TableMetadataTransformer(tableName);
    this.path = transformer.getTableRecordStorePath();
    this.subspace = FDBHelper.openSubspace(tx, path);
    this.tx = tx;
    this.db = db;
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
    //System.out.println("Has first value: " + iterator.hasNext());
    // Find all attributes of the first primary key. Sets the cursor's pointer to the first attribute keyvalue of the next pk
    List<KeyValue> keyvalueList = new ArrayList<>();
    if (iterator.hasNext()) {
      curr = iterator.next();
    }
    else return null;
    currRecord = keyvalueToFDBKVPair(getNextSetOfFDBKVPairs(keyvalueList));
    return currRecord;
  }

  public List<FDBKVPair> getLast() {
    // Check if initialization happened already
    if (startFromBeginning != null) return null;
    startFromBeginning = false;
    // Create the iterator
    iterator = initialize(true);
    //System.out.println("Has first value: " + iterator.hasNext());
    // Find all attributes of the last primary key
    List<KeyValue> keyvalueList = new ArrayList<>();
    if (iterator.hasNext()) {
      curr = iterator.next();
    }
    else return null;
    currRecord = keyvalueToFDBKVPair(getNextSetOfFDBKVPairs(keyvalueList));
    return currRecord;
  }

  public List<FDBKVPair> getNext() {
    if (!startFromBeginning) return null;
    if (iterator.hasNext()) {
      List<KeyValue> keyvalueList = new ArrayList<>();
      currRecord =  keyvalueToFDBKVPair(getNextSetOfFDBKVPairs(keyvalueList));
      return currRecord;
    }
    else return null;
  }

  public List<FDBKVPair> getPrevious() {
    if (startFromBeginning) return null;
    if (iterator.hasNext()) {
      List<KeyValue> keyvalueList = new ArrayList<>();
      currRecord =  keyvalueToFDBKVPair(getNextSetOfFDBKVPairs(keyvalueList));
      return currRecord;
    }
    else return null;
  }

  public StatusCode delete() {
    System.out.println("attrname: " + currRecord.get(0).getKey().get(metadata.getPrimaryKeys().size()-1));
    for (FDBKVPair kvpair : currRecord) {
      FDBHelper.removeKeyValuePair(tx, subspace, kvpair.getKey());
    }
    return StatusCode.SUCCESS;
  }

  private List<KeyValue> getNextSetOfFDBKVPairs( List<KeyValue> keyvalueList) {
    // Get PK of first record
    currPK = getPKFromKeyValue(curr);
    keyvalueList.add(curr);

    // Get rest of attributes of first PK
    boolean newPk = false;
    if (!iterator.hasNext()) return new ArrayList<>();
    while (iterator.hasNext() && !newPk) {
      curr = iterator.next();
      if (getPKFromKeyValue(curr).equals(currPK)) {
        //System.out.println("While loop entered");
        keyvalueList.add(curr);
      }
      else newPk = true;
    }
    // Do comparison here. If comparison fails, call getNextSetofFDBKVPairs again
    if (comparison(keyvalueList)) {
      return keyvalueList;
    }
    else {
      List<KeyValue> keyvalueListRedo = new ArrayList<>();
      return getNextSetOfFDBKVPairs(keyvalueListRedo);
    }
  }

  private boolean comparison(List<KeyValue> keyvalueList) {
    //System.out.println("Comparison entered");
    if (operator == null) return true;

    String comparedAttribute;
    KeyValue attrKV = null;
    for (KeyValue keyvalue : keyvalueList) {
      // Find the correct kv for attribute
      comparedAttribute = Tuple.fromBytes(keyvalue.getKey()).getString(metadata.getPrimaryKeys().size()+1);
      if (comparedAttribute.equals(attrName)) {
        //System.out.println("Attribute found: " + comparedAttribute + " matched to " + attrName);
        attrKV = keyvalue;
        break;
      }
    }
    //System.out.println("Search complete");
    // If atrtribute doens't exist, it fails the comparison
    if (attrKV == null) return false;
    //System.out.println("Attribute KV found"  );
    // Do comparison
    Object valueOf = Tuple.fromBytes(attrKV.getValue()).get(0);
    if (operator == ComparisonOperator.EQUAL_TO) {
      if (valueOf instanceof Integer) {
        return ((Integer) valueOf).equals((Integer) attrValue);
      }
      if (valueOf instanceof Long) {
        System.out.println("Here??");
        if (attrValue instanceof Integer)  return ((Long) valueOf).intValue() == (Integer) attrValue;
        System.out.println("Comparison here is: " + (((Long) valueOf).longValue() == ((Long) attrValue).longValue()));
        return ((Long) valueOf).longValue() == ((Long) attrValue).longValue();
      }
      if (valueOf instanceof Double) {
        return ((Double) valueOf).equals((Double) attrValue);
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
    };
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
    };
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
    };
    if (operator == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO) {
      if (valueOf instanceof Integer) {
        //System.out.println("I am here");
        return (Integer) valueOf >= (Integer) attrValue;
      }
      if (valueOf instanceof Long) {
        if (attrValue instanceof Integer)  return (Long) valueOf >= (Integer) attrValue;
        //System.out.println("I am here LONG");
        return (Long) valueOf >= (Long) attrValue;
      }
      if (valueOf instanceof Double) {
        return (Double) valueOf >= (Double) attrValue;
      }
      if (valueOf instanceof String) {
        return ((String) valueOf).compareTo((String) attrValue) >= 0;
      }
    };
    return false;
  }
  private Tuple getPKFromKeyValue(KeyValue keyvalue) {
    Tuple pk = new Tuple();
    //System.out.println("Size of primary keys: " + metadata.getPrimaryKeys().size());
    for (int i = 0; i < metadata.getPrimaryKeys().size(); i++) {
      //System.out.println("pk is null: " + isNull(pk) + "keyvalue is null: " + isNull(keyvalue));
      // popFront is needed since first value in key is for something else
      pk = pk.addObject(Tuple.fromBytes(keyvalue.getKey()).popFront().get(i));
    }
    return pk;
  }
  private AsyncIterator<KeyValue> initialize(boolean reverse) {
      Range range = subspace.range();
      AsyncIterable<KeyValue> iterable = tx.getRange(range, ReadTransaction.ROW_LIMIT_UNLIMITED, reverse);
      return iterable.iterator();
  }

  private List<FDBKVPair> keyvalueToFDBKVPair(List<KeyValue> keyvalueList) {
    List<FDBKVPair> FDBKVPairList = new ArrayList<>();
    for (KeyValue keyvalue : keyvalueList) {
      Tuple keyTuple = Tuple.fromBytes(keyvalue.getKey()).popFront();
      Tuple valueTuple = Tuple.fromBytes(keyvalue.getValue());
      FDBKVPairList.add(new FDBKVPair(path, keyTuple, valueTuple));
    }
    if (FDBKVPairList.size() == 0) return null;
    return FDBKVPairList;
  }

  public TableMetadata getMetadata() {
    return metadata;
  }

  public String getTableName() {
    return tableName;
  }

  public boolean commit() {
    boolean success = tryCommitTx(tx, 20);
    if (success) {
      startFromBeginning = null;
      iterator = null;
      operator = null;
      return true;
    }
    return false;
  }
}

// 685 Grams russet potatoes
// 1 tablespoon butter
// quarter cup milk
//  salt (2-3 tsp or to taste)
//  garlic powder (1.5 tsp or to taste)
//  MSG (~1 tsp)

// 740 grams of potatoes 1 calorie per gram
