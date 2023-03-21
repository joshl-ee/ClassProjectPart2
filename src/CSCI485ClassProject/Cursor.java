package CSCI485ClassProject;

import CSCI485ClassProject.models.ComparisonOperator;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;

public class Cursor {
  public enum Mode {
    READ,
    READ_WRITE
  }

  // your code here
  private final Mode mode;
  private final String tableName;
  private final Transaction tx;

  public Cursor(String tableName, Cursor.Mode mode, Transaction tx) {
    TableMetadataTransformer transformer = new TableMetadataTransformer(tableName);
    DirectorySubspace directory = FDBHelper.openSubspace(tx, transformer.getTableRecordStorePath());
    this.mode = mode;
    this.tableName = tableName;
    this.tx = tx;
  }

  public Cursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex, Transaction tx) {
    this.mode = mode;
    this.tableName = tableName;
    this.tx = tx;
  }

}

// 685 Grams russet potatoes
// 1 tablespoon butter
// quarter cup milk
//  salt (2-3 tsp)
//  garlic powder (1.5 tsp)
//  MSG (~1 tsp)

// 740 grams of potatoes 1 calorie per gram
