package CSCI485ClassProject;

import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;

import java.util.List;

import static CSCI485ClassProject.FDBHelper.getAllKeyValuePairsOfSubdirectory;

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
  private byte[] st;
  private byte[] end;
  private FDBKVPair currKey;
  private final Database db;

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

  public FDBKVPair getFirst() {
    if (startFromBeginning != null) return null;
    startFromBeginning = true;
    System.out.println("here1");
    List<FDBKVPair> KVPairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx, path);
    if (KVPairs.isEmpty()) return null;
    return KVPairs.get(0);
  }

  public FDBKVPair getLast() {
    if (startFromBeginning != null) return null;
    startFromBeginning = false;
    List<FDBKVPair> KVPairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx, path);
    if (KVPairs.isEmpty()) return null;
    return KVPairs.get(KVPairs.size()-1);
  }

  public TableMetadata getMetadata() {
    return metadata;
  }
}

// 685 Grams russet potatoes
// 1 tablespoon butter
// quarter cup milk
//  salt (2-3 tsp)
//  garlic powder (1.5 tsp)
//  MSG (~1 tsp)

// 740 grams of potatoes 1 calorie per gram
