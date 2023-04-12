package CSCI485ClassProject;

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
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.directory.*;

import CSCI485ClassProject.Cursor.Mode;
import CSCI485ClassProject.models.*;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.Record.Value;

public class IndexesImpl implements Indexes{

  private Database db = null;

  public IndexesImpl() { 
    FDB fdb = FDB.selectAPIVersion(710);
    try {
        db = fdb.open();
    } catch (Exception e) {
        System.out.println(e);
    }
  }

  @Override
  public StatusCode createIndex(String tableName, String attrName, IndexType indexType) {
    Transaction t = db.createTransaction();

    if (isIndexed(tableName, attrName, t)) {
      TableManagerImpl.commit(t);
      return StatusCode.INDEX_ALREADY_EXISTS_ON_ATTRIBUTE;
    }

    if (indexType == IndexType.NON_CLUSTERED_HASH_INDEX) {
      return createNonClusteredHashIndex(tableName, attrName,t);
    } else {
      return createBPlusTree(tableName, attrName, t);
    }
    
  }

  @Override
  public StatusCode dropIndex(String tableName, String attrName) {
    Transaction t = db.createTransaction(); 
    AsyncIterable<KeyValue> itr = getIterableForIndexedAttribute(tableName, attrName, t);
    if (itr != null) {
      AsyncIterator<KeyValue> iterator = itr.iterator(); 
      while (iterator.hasNext()) {
        t.clear(iterator.next().getKey());
      }
    }
    TableManagerImpl.commit(t);
    return StatusCode.SUCCESS;
  }

  /**
   * Returns the index path for a given table. 
   * @param tableName
   * @return
   */
  public static List<String> getIndexPathForTable(String tableName) {
    List<String> path = new ArrayList<>(); 
    path.add(tableName);
    path.add("indexStructures");
    return path; 
  }

  /**
   * Hash value for an object. 
   * @param o
   * @return
   */
  public static int hash(Object o) {
    int code = o.hashCode();
    if (code < 0) code *= -1; 
    return code%100; 
  }

  /**
   * Creates a non clustered hash index. A key-value pair looks like: 
   * (attrName, "HashNonCluster", hashValue, primaryKey) -> ''
   * @param tableName
   * @param attrName
   */
  private StatusCode createNonClusteredHashIndex(String tableName, String attrName, Transaction t) { 
    List<String> indexPath = getIndexPathForTable(tableName);
    DirectorySubspace dir = DirectoryLayer.getDefault().createOrOpen(t, indexPath).join();
    // use a cursor to iterate through the table. 
    RecordsImpl records = new RecordsImpl(); 
    Cursor c = records.openCursor(tableName, Mode.READ);
    TableMetadata tblMetadata = c.getTableMetadata();

    Record curr = records.getFirst(c);

    while (c.hasNext()) {
      Object val = curr.getValueForGivenAttrName(attrName);
      if (val != null) {
        Tuple keyTuple = new Tuple().add(attrName).add("HashNonCluster");
        int hashKey = hash(val);
        keyTuple = keyTuple.add(hashKey);
        for (String pkName : tblMetadata.getPrimaryKeys()) {
          keyTuple = keyTuple.addObject(curr.getValueForGivenAttrName(pkName));
        }
        Tuple valTuple = new Tuple(); 
       // System.out.println(keyTuple + "->" + valTuple); 
        t.set(dir.pack(keyTuple), valTuple.pack());
      }
      curr = records.getNext(c);
    }
    records.commitCursor(c);
    TableManagerImpl.commit(t);
    return StatusCode.SUCCESS; 
  }

  /**
   * Creates a B+ tree non clustered index. A key-value pair looks like:
   * (attrName, "BPlusNonCluster", attrValue, primaryKey)
   * @param tableName
   * @param attrName
   * @param t
   * @return
   */
  private StatusCode createBPlusTree(String tableName, String attrName, Transaction t) { 
    List<String> indexPath = getIndexPathForTable(tableName);
    DirectorySubspace dir = DirectoryLayer.getDefault().createOrOpen(t, indexPath).join();
    // use a cursor to iterate through the table. 
    RecordsImpl records = new RecordsImpl(); 
    Cursor c = records.openCursor(tableName, Mode.READ);
    TableMetadata tblMetadata = c.getTableMetadata();

    Record curr = records.getFirst(c);

    while (c.hasNext()) {
      Object val = curr.getValueForGivenAttrName(attrName);
      if (val != null) {
        Tuple keyTuple = new Tuple().add(attrName).add("BPlusNonCluster");
        keyTuple = keyTuple.addObject(val);
        for (String pkName : tblMetadata.getPrimaryKeys()) {
          keyTuple = keyTuple.addObject(curr.getValueForGivenAttrName(pkName));
        }
        Tuple valTuple = new Tuple(); 
       // System.out.println(keyTuple + "->" + valTuple); 
        t.set(dir.pack(keyTuple), valTuple.pack());
      }
      curr = records.getNext(c);
    }
    records.commitCursor(c);
    TableManagerImpl.commit(t);
    return StatusCode.SUCCESS; 
  }

  /**
   * Updates index structures based on a given record r. 
   * @param r for the record to index
   * @param tableName the table the structure belongs to
   * @param tblMetadata metadata for such table
   * @param t context
   */
  public static void updateIndex(Record r, String tableName, TableMetadata tblMetadata, Transaction t) {

    HashMap<String, Value> recordData = r.getMapAttrNameToValue();
    
    for(Map.Entry<String, Value> x : recordData.entrySet()) {
      
      IndexType thisType = getIndexedType(tableName, x.getKey(), t);
      List<String> indexPath = getIndexPathForTable(tableName);
      DirectorySubspace dir = DirectoryLayer.getDefault().createOrOpen(t, indexPath).join();

      Object val = x.getValue().getValue();

      if (thisType == IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX) {
        Tuple keyTuple = new Tuple().add(x.getKey()).add("BPlusNonCluster");
        keyTuple = keyTuple.addObject(val);
        for (String pkName : tblMetadata.getPrimaryKeys()) {
          keyTuple = keyTuple.addObject(recordData.get(pkName).getValue());
        }
        Tuple valTuple = new Tuple(); 
        //System.out.println(keyTuple + "->" + valTuple); 
        t.set(dir.pack(keyTuple), valTuple.pack());
      } else if (thisType == IndexType.NON_CLUSTERED_HASH_INDEX) {
        Tuple keyTuple = new Tuple().add(x.getKey()).add("HashNonCluster");
        int hashKey = hash(val);
        keyTuple = keyTuple.add(hashKey);
        for (String pkName : tblMetadata.getPrimaryKeys()) {
          keyTuple = keyTuple.addObject(recordData.get(pkName).getValue());
        }
        Tuple valTuple = new Tuple(); 
       //System.out.println(keyTuple + "->" + valTuple); 
        t.set(dir.pack(keyTuple), valTuple.pack());
      } // end else if 

    } // end for each in map of attr -> val 
  }

  public static void clearIndexOnRecord(Record r, String tableName, TableMetadata tblMetadata, Transaction t) {

    HashMap<String, Value> recordData = r.getMapAttrNameToValue();
    
    for(Map.Entry<String, Value> x : recordData.entrySet()) {

      IndexType thisType = getIndexedType(tableName, x.getKey(), t);
      List<String> indexPath = getIndexPathForTable(tableName);
      DirectorySubspace dir = DirectoryLayer.getDefault().createOrOpen(t, indexPath).join();

      Object val = x.getValue().getValue();

      if (thisType == IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX) {
        Tuple keyTuple = new Tuple().add(x.getKey()).add("BPlusNonCluster");
        keyTuple = keyTuple.addObject(val);
        for (String pkName : tblMetadata.getPrimaryKeys()) {
          keyTuple = keyTuple.addObject(recordData.get(pkName).getValue());
        }
      //  System.out.println("Clearing " + keyTuple); 
        t.clear(dir.pack(keyTuple));
      } else if (thisType == IndexType.NON_CLUSTERED_HASH_INDEX) {
        Tuple keyTuple = new Tuple().add(x.getKey()).add("HashNonCluster");
        int hashKey = hash(val);
        keyTuple = keyTuple.add(hashKey);
        for (String pkName : tblMetadata.getPrimaryKeys()) {
          keyTuple = keyTuple.addObject(recordData.get(pkName).getValue());
        }
      //  System.out.println("Clearing " + keyTuple); 
        t.clear(dir.pack(keyTuple));
      }
    }
  }

  /**
   * Checks if there is an index structure. Does not commit transaction. 
   * @param tableName for the table's index structure, if any
   * @param attrName to specify which attribute should be indexed
   * @param t 
   * @return
   */
  public static boolean isIndexed(String tableName, String attrName, Transaction t) {
    AsyncIterable<KeyValue> itr = getIterableForIndexedAttribute(tableName, attrName, t);
    if (itr == null) return false; 

    if(itr.iterator().hasNext()) {
      return true;
    }
    return false;
  }

  /**
   * Same as isIndexed function, except it returns the type of index that exists. 
   * @param tableName
   * @param attrName
   * @param t
   * @return
   */
  public static IndexType getIndexedType(String tableName, String attrName, Transaction t) { 
    AsyncIterable<KeyValue> itr = getIterableForIndexedAttribute(tableName, attrName, t);
    if (itr == null) return null; 
    if(itr.iterator().hasNext()) {
      List<String> indexPath = getIndexPathForTable(tableName);
      DirectorySubspace dir = DirectoryLayer.getDefault().open(t, indexPath).join();
      byte key[] = itr.iterator().next().getKey();
      Tuple index = dir.unpack(key);
      if (index.getString(1).contains("BPlus")) return IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX;
      else return IndexType.NON_CLUSTERED_HASH_INDEX;
    }
    return null;
  }

  /**
   * Returns an AsyncIterable object to iterate through key values 
   * Intended use is to iterate through an index structure for a given attribute in 
   * a given table. 
   * @param tableName
   * @param attrName
   * @param t
   * @return
   */
  public static AsyncIterable<KeyValue> getIterableForIndexedAttribute(String tableName, String attrName, Transaction t) {
    List<String> indexPath = getIndexPathForTable(tableName);
    Tuple attrTuple = new Tuple().add(attrName);
    try {
      DirectorySubspace dir = DirectoryLayer.getDefault().open(t, indexPath).join();
      boolean exists = DirectoryLayer.getDefault().exists(t, indexPath).join();
      if (exists) {
        AsyncIterable<KeyValue> itr = t.getRange(Range.startsWith(dir.pack(attrTuple)), ReadTransaction.ROW_LIMIT_UNLIMITED, false);
        return itr;
      }
    } catch (Exception e) { System.out.println(e); }
    return null;
  }

}
