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
  private AsyncIterator<KeyValue> iterator;
  private Boolean startFromBeginning = null;
  public Cursor(String tableName, Cursor.Mode mode, Database db, Transaction tx) {
    this.mode = mode;
    this.tableName = tableName;
    this.metadata = new TableManagerImpl().listTables().get(tableName);
    TableMetadataTransformer transformer = new TableMetadataTransformer(tableName);
    this.path = transformer.getTableRecordStorePath();
    this.subspace = FDBHelper.openSubspace(tx, path);
    this.tx = tx;
    this.db = db;
  }

  public Cursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex, Database db, Transaction tx) {
    this.mode = mode;
    this.tableName = tableName;
    this.metadata = new TableManagerImpl().listTables().get(tableName);
    TableMetadataTransformer transformer = new TableMetadataTransformer(tableName);
    this.path = transformer.getTableRecordStorePath();
    this.subspace = FDBHelper.openSubspace(tx, path);
    this.tx = tx;
    this.db = db;
  }

  public List<FDBKVPair> getFirst() {
    // Check if initialization happened already
    if (startFromBeginning != null) return null;
    startFromBeginning = true;
    // Create the iterator
    iterator = initialize(false);
    System.out.println("Has first value: " + iterator.hasNext());
    // Find all attributes of the first primary key. Sets the cursor's pointer to the first attribute keyvalue of the next pk
    List<KeyValue> keyvalueList = new ArrayList<>();
    Tuple pk = new Tuple();
    // Get PK of first record
    if (iterator.hasNext()) {
      KeyValue first = iterator.next();
//      System.out.println("Firstvalue: " + Tuple.fromBytes(first.getKey()).get(0));
//      System.out.println("Secondvalue: " + Tuple.fromBytes(first.getKey()).get(1));
//      System.out.println("Thurdvalue: " + Tuple.fromBytes(first.getKey()).get(2));

      pk = getPKFromKeyValue(first, pk);
      keyvalueList.add(first);
    }
    while (curr != null && getPKFromKeyValue(curr, pk).equals(pk)) {
      keyvalueList.add(curr);
      curr = iterator.next();
    }
    return keyvalueToFDBKVPair(keyvalueList);
  }

  public List<FDBKVPair> getLast() {
    // Check if initialization happened already
    if (startFromBeginning != null) return null;
    startFromBeginning = false;
    // Create the iterator
    iterator = initialize(true);
    System.out.println("Has first value: " + iterator.hasNext());
    // Find all attributes of the last primary key
    List<KeyValue> keyvalueList = new ArrayList<>();
    Tuple pk = new Tuple();
    // Get PK of first record
    if (iterator.hasNext()) {
      KeyValue first = iterator.next();
      pk = getPKFromKeyValue(first, pk);
      keyvalueList.add(first);
    }
    while (curr != null && getPKFromKeyValue(curr, pk.add(false)).equals(pk)) {
      keyvalueList.add(curr);
      curr = iterator.next();
    }
    return keyvalueToFDBKVPair(keyvalueList);
  }

  public Tuple getPKFromKeyValue(KeyValue keyvalue, Tuple pk) {
    System.out.println("Size of primary keys: " + metadata.getPrimaryKeys().size());
    for (int i = 0; i < metadata.getPrimaryKeys().size(); i++) {
      System.out.println("pk is null: " + isNull(pk) + "keyvalue is null: " + isNull(keyvalue));
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
}

// 685 Grams russet potatoes
// 1 tablespoon butter
// quarter cup milk
//  salt (2-3 tsp or to taste)
//  garlic powder (1.5 tsp or to taste)
//  MSG (~1 tsp)

// 740 grams of potatoes 1 calorie per gram
