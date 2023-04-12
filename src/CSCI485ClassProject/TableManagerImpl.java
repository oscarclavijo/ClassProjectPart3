package CSCI485ClassProject;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import java.io.ObjectInputFilter.Status;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import CSCI485ClassProject.models.*;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.subspace.Subspace;

/**
 * TableManagerImpl implements interfaces in {#TableManager}. You should put your implementation
 * in this class.
 */
public class TableManagerImpl implements TableManager {

  private Database db = null;
  private static int MAX_TRANSACTION_COMMIT_RETRY_TIMES = 20;

  /**
   * Constructor : inits database. 
   */
  public TableManagerImpl() {
    FDB fdb = FDB.selectAPIVersion(710);
    try {
        db = fdb.open();
    } catch (Exception e) {
        System.out.println(e);
    }
  }
  
  @Override
  public StatusCode createTable(String tableName, String[] attributeNames, AttributeType[] attributeType,
                                String[] primaryKeyAttributeNames) {
    /**
     *  Checks that valid parameters were passed. 
     */
    if (attributeType == null || attributeNames == null || primaryKeyAttributeNames == null) 
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID; 

    if (attributeType.length == 0 || attributeNames.length == 0 || primaryKeyAttributeNames.length == 0) 
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;

    if (primaryKeyAttributeNames.length == 0) return StatusCode.TABLE_CREATION_NO_PRIMARY_KEY;
    
    List<String> tableSubdirectory = new ArrayList<>();
    tableSubdirectory.add(tableName);

    Transaction t = db.createTransaction(); 

    TableMetadata tblData = new TableMetadata(attributeNames, attributeType, primaryKeyAttributeNames); 


    StatusCode isPrimaryKeyAdded = tblData.setPrimaryKeys(Arrays.asList(primaryKeyAttributeNames)); 
    if (isPrimaryKeyAdded != StatusCode.SUCCESS) {
      t.cancel();
      return StatusCode.TABLE_CREATION_PRIMARY_KEY_NOT_FOUND; 
    }

    // if(DirectoryLayer.getDefault().exists(t, tableSubdirectory).join()) {
    //   // the table exists within the database, then abort transaction and return the 
    //   // error code 
    //   t.cancel();
    //   return StatusCode.TABLE_ALREADY_EXISTS;
    // }

    List<String> directory = new ArrayList<>();
    directory.add(tableName);
    directory.add("attributesStored");

    // ...tableName/attributesStored 
    DirectorySubspace tableAttributeSpace = DirectoryLayer.getDefault().createOrOpen(t, directory).join();

    // for all attributes in the table, create a Tuple:
    // set ("\(AttributeName)", (0/1/2, 0/1))
    for (Map.Entry<String, AttributeType> entry : tblData.getAttributes().entrySet()) {
      Tuple keyTuple = new Tuple().add(entry.getKey()); 
      boolean isPrimaryKey = tblData.getPrimaryKeys().contains(entry.getKey());
      Tuple valueTuple = new Tuple().add(entry.getValue().ordinal()).add(isPrimaryKey); 
      t.set(tableAttributeSpace.pack(keyTuple), valueTuple.pack());
    }
    commit(t);
    
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    Transaction t = db.createTransaction(); 
    List<String> table = new ArrayList<>(); 

    table.add(tableName);
    boolean exists = DirectoryLayer.getDefault().exists(t, table).join();
    if (!exists) {
      t.cancel();
      return StatusCode.TABLE_NOT_FOUND; 
    }

    DirectoryLayer.getDefault().remove(t, table).join();
    commit(t);
    return StatusCode.SUCCESS;
  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    Transaction readTransaction = db.createTransaction();
    HashMap<String, TableMetadata> tableMap = new HashMap<>();
    List<String> names = DirectoryLayer.getDefault().list(readTransaction).join();


    for (String tblName : names) {
      List<String> storedAt = new ArrayList<>(); 
      storedAt.add(tblName);
      storedAt.add("attributesStored");

      boolean exists = DirectoryLayer.getDefault().exists(readTransaction, storedAt).join();
      if(!exists) {
        System.out.println("Canceling...");
        readTransaction.cancel();
        return tableMap; 
      }

      DirectorySubspace dir = DirectoryLayer.getDefault().createOrOpen(readTransaction, storedAt).join();
      Range range = dir.range();

      List<KeyValue> kvs = readTransaction.getRange(range).asList().join();

      TableMetadata tableMetadata = new TableMetadata();
      List<String> primaryKeys = new ArrayList<>();

      for (KeyValue kv : kvs) {
        Tuple key = dir.unpack(kv.getKey());
        Tuple value = Tuple.fromBytes(kv.getValue());

        System.out.println(key);
        System.out.println(value);
        
        String attributeName = key.getString(0);
        tableMetadata.addAttribute(attributeName, AttributeType.values() [Math.toIntExact((Long) value.get(0))]);
        boolean isPrimaryKey = value.getBoolean(1);
        if (isPrimaryKey) {
          primaryKeys.add(attributeName);
        }
        
      }
      tableMetadata.setPrimaryKeys(primaryKeys);
      tableMap.put(tblName, tableMetadata);
    }

    commit(readTransaction);
    return tableMap;
  }

  @Override
  public StatusCode addAttribute(String tableName, String attributeName, AttributeType attributeType) {

    Transaction t = db.createTransaction(); 
    List<String> path = new ArrayList<>(); 

    path.add(tableName);
    boolean exists = DirectoryLayer.getDefault().exists(t, path).join();
    if (!exists) {
      t.cancel();
      return StatusCode.TABLE_NOT_FOUND; 
    }
    path.add("attributesStored");

    DirectorySubspace tableAttributeSpace = DirectoryLayer.getDefault().createOrOpen(t, path).join();
    byte[] valBytes = t.get(tableAttributeSpace.pack(attributeName)).join();
    if (valBytes != null) { 
      t.cancel();
      return StatusCode.ATTRIBUTE_ALREADY_EXISTS;
    }
    Tuple keyTuple = new Tuple().add(attributeName); 
    Tuple valueTuple = new Tuple().add(attributeType.ordinal()).add(false); 
    t.set(tableAttributeSpace.pack(keyTuple), valueTuple.pack());
    
    commit(t);

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {
    Transaction t = db.createTransaction(); 
    List<String> path = new ArrayList<>(); 

    path.add(tableName);
    boolean exists = DirectoryLayer.getDefault().exists(t, path).join();
    if (!exists) {
      t.cancel();
      return StatusCode.TABLE_NOT_FOUND; 
    }
    path.add("attributesStored");

    DirectorySubspace tableAttributeSpace = DirectoryLayer.getDefault().createOrOpen(t, path).join();
    byte[] valBytes = t.get(tableAttributeSpace.pack(attributeName)).join();
    if (valBytes == null) { 
      t.cancel();
      return StatusCode.ATTRIBUTE_NOT_FOUND;
    }
    Tuple keyTuple = new Tuple().add(attributeName); 
    t.clear(tableAttributeSpace.pack(keyTuple));
    
    commit(t);

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAllTables() {
    Transaction t = db.createTransaction();
    final byte[] st = new Subspace(new byte[]{(byte) 0x00}).getKey();
    final byte[] en = new Subspace(new byte[]{(byte) 0xFF}).getKey();
    t.clear(st, en);
    commit(t);
    return StatusCode.SUCCESS;
  }

  /**
   * HELPER FUNCTIONS.
   */
  public static boolean commit(Transaction t) { 
    int retryCounter = 0; 
    try {
      t.commit().join();
      return true;
    } catch (FDBException e) {
      if (retryCounter < MAX_TRANSACTION_COMMIT_RETRY_TIMES) {
        retryCounter++;
        commit(t);
      } else {
        t.cancel();
        return false;
      }
    }
    return false;
  }
  

}