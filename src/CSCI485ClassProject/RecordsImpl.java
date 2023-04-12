package CSCI485ClassProject;

import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Record;

import java.security.KeyStore.Entry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.directory.*;

import CSCI485ClassProject.Cursor.Mode;
import CSCI485ClassProject.models.*;

public class RecordsImpl implements Records{

  private Database db = null;
  private static int MAX_TRANSACTION_COMMIT_RETRY_TIMES = 20;

  /**
   * Init database. 
   */
  public RecordsImpl() {
    FDB fdb = FDB.selectAPIVersion(710);
    try {
        db = fdb.open();
    } catch (Exception e) {
        System.out.println(e);
    }
  }
  
  public static TableMetadata getTableByName(String tableName, Transaction t) { 
    List<String> names = DirectoryLayer.getDefault().list(t).join();

    for (String tblName : names) {
      // equal tableNames 
      if (tblName.compareTo(tableName) == 0) {

        DirectorySubspace dir = DirectoryLayer.getDefault().createOrOpen(t, getAttributePathForTable(tableName)).join();
        Range range = dir.range();

        List<KeyValue> kvs = t.getRange(range).asList().join();

        TableMetadata tableMetadata = new TableMetadata();
        List<String> primaryKeys = new ArrayList<>();

        for (KeyValue kv : kvs) {
          Tuple key = dir.unpack(kv.getKey());
          Tuple value = Tuple.fromBytes(kv.getValue());
  
          String attributeName = key.getString(0);
          tableMetadata.addAttribute(attributeName, AttributeType.values() [Math.toIntExact((Long) value.get(0))]);
          boolean isPrimaryKey = value.getBoolean(1);
          if (isPrimaryKey) {
            primaryKeys.add(attributeName);
          }
        }
        tableMetadata.setPrimaryKeys(primaryKeys);
        return tableMetadata; 
      }
    }// end for loop 

    return null; 
  }


  @Override
  public StatusCode insertRecord(String tableName, String[] primaryKeys, Object[] primaryKeysValues, String[] attrNames, Object[] attrValues) {
    
    if (primaryKeys == null || primaryKeysValues == null || attrNames == null || attrValues == null) return StatusCode.DATA_RECORD_CREATION_ATTRIBUTES_INVALID;
    if (primaryKeys.length != primaryKeysValues.length || attrValues.length != attrNames.length) return StatusCode.DATA_RECORD_CREATION_ATTRIBUTES_INVALID;
   
    Transaction t = db.createTransaction();

    TableMetadata insertTo = getTableByName(tableName, t);
    if (insertTo == null) { 
      t.cancel();
      return StatusCode.TABLE_NOT_FOUND;
    }

    List<String> tablePK = insertTo.getPrimaryKeys();
    HashMap<String, AttributeType> attributesInCurr = insertTo.getAttributes(); 

    // error check: if there is a pk unmatched
    if (!tablePK.equals(Arrays.asList(primaryKeys))) {
      t.cancel();
      return StatusCode.DATA_RECORD_PRIMARY_KEYS_UNMATCHED; 
    }

    /**
     * Data Model: keyTuple is (PK, attrName) = attrValue. 
     * Verify that data types match by using Record functions and comparing 
     * to TableMetadata. 
     */
    List<String> dataPath = getDataPathForTable(tableName);
    DirectorySubspace dir = DirectoryLayer.getDefault().createOrOpen(t, dataPath).join();
  
    Record r = new Record(); 
    Tuple keyTuple = new Tuple(); 
    Tuple valueTuple = new Tuple(); 

    for (int i = 0; i < primaryKeysValues.length; i++) {
      /**
       * Sets the primary keys in the data path (in case theres 1+ PKs)
       */
      for (int x = 0; x < primaryKeys.length; x++) {
        r.setAttrNameAndValue(primaryKeys[x], primaryKeysValues[x]);
        keyTuple = Tuple.from(r.getValueForGivenAttrName(primaryKeys[x])).add(primaryKeys[x]);
        valueTuple = Tuple.from(r.getValueForGivenAttrName(primaryKeys[x])); 

        boolean exists = DirectoryLayer.getDefault().exists(t, dataPath).join();
        if (exists) {
          AsyncIterable<KeyValue> itr = t.getRange(Range.startsWith(dir.pack(keyTuple)), ReadTransaction.ROW_LIMIT_UNLIMITED, false);
          if(itr.iterator().hasNext()) {
            t.cancel();
            return StatusCode.DATA_RECORD_CREATION_RECORD_ALREADY_EXISTS;
          }
        }
        t.set(dir.pack(keyTuple), valueTuple.pack());
        
      }
      
      for (int j = 0; j < attrValues.length; j++) {
        r.setAttrNameAndValue(attrNames[j], attrValues[j]);
        if(!attributesInCurr.containsKey(attrNames[j])) {
          addAttributeToTable(tableName, r.getTypeForGivenAttrName(attrNames[j]), attrNames[j], t);
          insertTo = getTableByName(tableName, t);
          attributesInCurr = insertTo.getAttributes(); 
        }
        if(!attributesInCurr.get(attrNames[j]).equals(r.getTypeForGivenAttrName(attrNames[j]))) {
          t.cancel();
          return StatusCode.DATA_RECORD_CREATION_ATTRIBUTE_TYPE_UNMATCHED;
        }

        keyTuple = Tuple.from(r.getValueForGivenAttrName(primaryKeys[i])).add(attrNames[j]);
        valueTuple = Tuple.from(r.getValueForGivenAttrName(attrNames[j])); 

        t.set(dir.pack(keyTuple), valueTuple.pack());
       // System.out.println(keyTuple + " -> " +valueTuple);
      } // end for loop(attrValues)
    } // end for loop (primaryKeysValues) 

    TableManagerImpl.commit(t);
    t = db.createTransaction();
    IndexesImpl.updateIndex(r, tableName, insertTo, t);
    TableManagerImpl.commit(t);
    return StatusCode.SUCCESS;

  }

  @Override
  public Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex) {
    Transaction t = db.createTransaction();

    TableMetadata tblMetadata = getTableByName(tableName, t);

    // check if the given attribute exists
    if (!tblMetadata.doesAttributeExist(attrName)) {
      t.cancel();
      return null;
    }

    Cursor cursor = new Cursor(mode, tableName, tblMetadata, t);
    Record.Value attrVal = new Record.Value();
    StatusCode initVal = attrVal.setValue(attrValue);
    if (initVal != StatusCode.SUCCESS) {
      // check if the new value's type matches the table schema
      t.cancel();
      return null;
    }
    cursor.enablePredicate(attrName, attrVal, operator);

    /*
     * Part 3: Uses index structure. 
     */
    if (isUsingIndex || mode == Mode.READ_WRITE) {
      if(IndexesImpl.isIndexed(tableName, attrName, t)) cursor.enableIndexStructure();
      else return null; 
    }
    
    return cursor;
  }

  @Override
  public Cursor openCursor(String tableName, Cursor.Mode mode) {
    Transaction t = db.createTransaction(); 

    TableMetadata tblMetadata = getTableByName(tableName, t);
    return new Cursor(mode, tableName, tblMetadata, t);
  }

  @Override
  public Record getFirst(Cursor cursor) {
    Record r = cursor.getFirst(); 
    return r; 
  }

  @Override
  public Record getLast(Cursor cursor) {
    return cursor.getLast();
  }

  @Override
  public Record getNext(Cursor cursor) {
    Record r = cursor.next(false); 
    return r; 
  }

  @Override
  public Record getPrevious(Cursor cursor) {
    return cursor.next(true);
  }

  @Override
  public StatusCode updateRecord(Cursor cursor, String[] attrNames, Object[] attrValues) {
    return cursor.updateCurrentRecord(attrNames, attrValues);
  }

  @Override
  public StatusCode deleteRecord(Cursor cursor) {
    if (cursor == null || cursor.getTransaction() == null) return StatusCode.CURSOR_INVALID;
    if (!cursor.isInitialized()) return StatusCode.CURSOR_NOT_INITIALIZED;
    if (cursor.getCurrentRecord() == null) return StatusCode.CURSOR_REACH_TO_EOF;
    
    Record recordToDelete = cursor.getCurrentRecord();
    IndexesImpl.clearIndexOnRecord(recordToDelete, cursor.getTableName(), cursor.getTableMetadata(), cursor.getTransaction());
    Set<String> attrDiffSet = new HashSet<>();
    Transaction tx = cursor.getTransaction();

    // Open another cursor and scan the table, see if the table schema needs to change because of the deletion
    Cursor scanCursor = new Cursor(Cursor.Mode.READ, cursor.getTableName(), cursor.getTableMetadata(), tx);
    boolean isScanCursorInit = false;
    while (true) {
      Record record;
      if (!isScanCursorInit) {
        isScanCursorInit = true;
        record = getFirst(scanCursor);
      } else {
        record = getNext(scanCursor);
      }
      if (record == null) {
        break;
      }
      Set<String> attrSet = record.getMapAttrNameToValue().keySet();
      Set<String> attrSetToDelete = new HashSet<>(recordToDelete.getMapAttrNameToValue().keySet());
      attrSetToDelete.removeAll(attrSet);
      attrDiffSet.addAll(attrSetToDelete);
    }
    if (!attrDiffSet.isEmpty()) {
      // drop the attributes of the table
      List<String> dataPath = getDataPathForTable(cursor.getTableName());
      DirectorySubspace tableAttrDir = DirectoryLayer.getDefault().open(tx, dataPath).join();

      for (String attrNameToDrop : attrDiffSet) {
        Tuple attrKeyTuple = new Tuple().add(attrNameToDrop);
        tx.clear(tableAttrDir.pack(attrKeyTuple));
      }
    }
    return cursor.deleteCurr();
  }

  @Override
  public StatusCode commitCursor(Cursor cursor) {
    if (cursor == null) return StatusCode.CURSOR_INVALID;
    cursor.commit();
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode abortCursor(Cursor cursor) {
    if (cursor == null) return StatusCode.CURSOR_INVALID;
    cursor.abort();
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteDataRecord(String tableName, String[] attrNames, Object[] attrValues) {
    return null;
  }


  public static List<String> getDataPathForTable(String tableName) {
    List<String> path = new ArrayList<>(); 
    path.add(tableName);
    path.add("dataRecords");
    return path; 
  }

  public static List<String> getAttributePathForTable(String tableName) {
    List<String> path = new ArrayList<>(); 
    path.add(tableName);
    path.add("attributesStored");
    return path; 
  }

  public void addAttributeToTable(String tableName, AttributeType type, String name, Transaction t) {
    List<String> dataPath = getAttributePathForTable(tableName);
    DirectorySubspace dir = DirectoryLayer.getDefault().createOrOpen(t, dataPath).join();

    Tuple keyTuple = new Tuple().add(name); 
    Tuple valueTuple = new Tuple().add(type.ordinal()).add(false); 
    t.set(dir.pack(keyTuple), valueTuple.pack());
  }
}
