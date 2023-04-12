package CSCI485ClassProject;


import CSCI485ClassProject.models.*;
import CSCI485ClassProject.models.Record;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Cursor {

  private Mode mode; 
  private String tableName;
  private TableMetadata tableMetadata; 
  private Transaction t; 

  private AsyncIterator<KeyValue> iterator = null; 
  private boolean isInit = false; 
  private DirectorySubspace dir = null; 
  private boolean initLast = false; 

  private Record curr = null; 
  private KeyValue currIndex = null; 
  private boolean usePredicate = false; 

  private boolean isMoved = false; 
  private Tuple currKey = null; 
  private Tuple currVal = null; 

  private String predicateAttributeName; 
  private Record.Value predicateAttributeValue; 
  private ComparisonOperator predicateOperator; 

  private boolean useIndex = false; 

  public enum Mode {
    READ,
    READ_WRITE
  }

  public Cursor(Mode mode, String tableName, TableMetadata tableMetadata, Transaction t) {
    this.mode = mode;
    this.tableName = tableName;
    this.tableMetadata = tableMetadata;
    this.t = t;
  }
  
  /*
   * PART 3
   */

  public void enableReadWrite() { 
    useIndex = true; 
  }
  public void enableIndexStructure() {
    useIndex = true; 
  }

  /**
   * Modeled after the nextRecord() function. Maintains the position of the iterator 
   * on the index structure. However, it creates a new iterator once the correct
   * record is retrieved in order to generate the whole record. 
   * @param init
   * @return
   */
  public Record nextIndexedRecord(boolean init) { 
    List<String> indexPath = IndexesImpl.getIndexPathForTable(tableName);

    if (!init && !isInit) return null; 

    if (init) {
      dir = DirectoryLayer.getDefault().createOrOpen(t, indexPath).join();
      Tuple attrTuple = new Tuple().add(predicateAttributeName);
      AsyncIterable<KeyValue> iterable = t.getRange(Range.startsWith(dir.pack(attrTuple)), ReadTransaction.ROW_LIMIT_UNLIMITED, initLast);
      if (iterable != null) {
        iterator = iterable.iterator(); 
      }
      isInit = true;
    }
    currIndex= null; 
    curr = null; 

    if (dir == null) return null; 
    if (!hasNext()) return null; 

    HashMap<String, Object> row = new HashMap<String, Object>(); 
   // return null; 
    if (iterator.hasNext()) {
      currIndex = iterator.next();
      KeyValue kv = currIndex; 
      Tuple prefix = new Tuple();
      // for all the PKs in a tuple. 
      Tuple indexKey = dir.unpack(kv.getKey());
      for (int i = 3; i <= indexKey.size()-1; i++) {
        prefix = prefix.addObject(indexKey.get(i));
      }
System.out.println("Prefix: " + prefix);
      List<String> dataPath = RecordsImpl.getDataPathForTable(tableName);
      DirectorySubspace dir = DirectoryLayer.getDefault().open(t, dataPath).join();
      AsyncIterable<KeyValue> itr = t.getRange(Range.startsWith(dir.pack(prefix)), ReadTransaction.ROW_LIMIT_UNLIMITED, false);
      AsyncIterator<KeyValue> dataItr = itr.iterator();
      while (dataItr.hasNext()) {
        KeyValue currCell = dataItr.next();
        Tuple keyTuple = dir.unpack(currCell.getKey());
     //   System.out.println("Key: " + keyTuple);
        Tuple valTuple = Tuple.fromBytes(currCell.getValue());
        row.put(keyTuple.getString(keyTuple.size()-1), valTuple.get(0));
      }      
    }
    if (!row.isEmpty()) {
      curr = generateRecordFromRow(row);
    }
    return curr;
  }
  
  public Record getFirst() {
    if (isInit) return null;
    initLast = false;
    Record record; 

    if (useIndex) {
      record = nextIndexedRecord(true);
      if (usePredicate) { 
        while (record != null && !matchesPredicate(record)) {
          record = nextIndexedRecord(false);
        }
      }
    } else {
      record = nextRecord(true);
      if (usePredicate) { 
        while (record != null && !matchesPredicate(record)) {
          record = nextRecord(false);
        }
      }
    }
  
    return record;
  }

  public Record getLast() {
    if (isInit) return null;
    initLast = true;

    Record record; 

    if (useIndex) {
      record = nextIndexedRecord(true);
      if (usePredicate) { 
        while (record != null && !matchesPredicate(record)) {
          record = nextIndexedRecord(false);
        }
      }
    } else {
      record = nextRecord(true);
      if (usePredicate) { 
        while (record != null && !matchesPredicate(record)) {
          record = nextRecord(false);
        }
      }
    }

    return record;
  }

  public void enablePredicate(String attrName, Record.Value value, ComparisonOperator operator) {
    this.predicateAttributeName = attrName;
    this.predicateAttributeValue = value;
    this.predicateOperator = operator;
    this.usePredicate = true;
  }

  public boolean hasNext() {
    return isInit && iterator != null && (iterator.hasNext() || currKey != null);
  }

  public Record next(boolean isGetPrevious) {
    if (!isInit) return null; 
    if (isGetPrevious != initLast) return null; 

    Record record; 
    if (useIndex) {
      record = nextIndexedRecord(false);
      if (usePredicate) { 
        while (record != null && !matchesPredicate(record)) {
          record = nextIndexedRecord(false);
        }
      }
    } else {
      record = nextRecord(false);
      if (usePredicate) { 
        while (record != null && !matchesPredicate(record)) {
          record = nextRecord(false);
        }
      }
    }
    return record; 
   
  }

  public void abort() {
    if (iterator != null) iterator.cancel();
    
    if (t != null) {
      t.cancel();
      t.close(); 
    }

    t = null;
  }

  public Record getCurrentRecord() { 
    return curr;
  }

  public Transaction getTransaction() { 
    return t; 
  }
  public String getTableName() {
    return tableName; 
  }
  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }
  public void commit() { 
    if (iterator != null) iterator.cancel();
    
    if (t != null) TableManagerImpl.commit(t);
    t = null; 
  }

  public Record nextRecord(boolean init) { 
    // iterator not init
    if (!init && !isInit) {
      return null; 
    }
    List<String> dataPath = RecordsImpl.getDataPathForTable(tableName);

    if (init) {
      dir = DirectoryLayer.getDefault().createOrOpen(t, dataPath).join();

      Range dirRange = dir.range();
      AsyncIterable<KeyValue> iterable = t.getRange(dirRange, ReadTransaction.ROW_LIMIT_UNLIMITED, initLast);
      if (iterable != null) {
        iterator = iterable.iterator(); 
      }
      isInit = true;
    }
    curr = null; 

    if (dir == null) return null; 
    if (!hasNext()) return null; 

    HashMap<String, Object> row = new HashMap<String, Object>(); 

    boolean isSavePK = false;
    Tuple pkValTuple = new Tuple();
    Tuple tempPkValTuple = null;

    if (isMoved && currKey != null) {
     // System.out.println(currKey + " -> "+currVal);
      row.put(currKey.getString(currKey.size()-1), currVal.get(0));
      pkValTuple = getKeyFromTuple(currKey); 
      isSavePK = true; 
    }

    isMoved = true; 
    boolean nextExists = false; 

    while (iterator.hasNext()) {

      KeyValue kv = iterator.next();
      Tuple keyTuple = dir.unpack(kv.getKey());
      Tuple valTuple = Tuple.fromBytes(kv.getValue());
      tempPkValTuple = getKeyFromTuple(keyTuple);
      if (!isSavePK) {
        pkValTuple = tempPkValTuple;
        isSavePK = true; 
      } else if (!pkValTuple.equals(tempPkValTuple)) {
        currKey = keyTuple; 
        currVal = valTuple;
        nextExists = true; 
        break; 
      }
      // places attributeName -> attributeVal into the "row" map
    //  System.out.println(keyTuple + " -> "+valTuple);
      row.put(keyTuple.getString(keyTuple.size()-1), valTuple.get(0));
    }
    if (!row.isEmpty()) {
      curr = generateRecordFromRow(row);
    }
    if (!nextExists) {
      currKey = null; 
      currVal = null;
    }
    return curr;
  } // end nextRecord
   
   public static Tuple getKeyFromTuple(Tuple keyTuple) {

    Tuple primTuple = new Tuple();
    for (int i = 0; i<keyTuple.size()-1; i++) {
      Object o = keyTuple.get(i);
      primTuple = primTuple.addObject(o);
    }
    return primTuple; 

   } // end getKeyFromTuple

   public boolean isInitialized() {
    return isInit; 
   }
   public StatusCode deleteCurr() { 
    if (t == null) return StatusCode.CURSOR_INVALID;
    if (!isInit) return StatusCode.CURSOR_NOT_INITIALIZED;
    if (curr == null) return StatusCode.CURSOR_REACH_TO_EOF;

    HashMap<String, Record.Value> attrMap = curr.getMapAttrNameToValue();
    List<Object> primVal = new ArrayList<>();

    List<String> primaryKeys = tableMetadata.getPrimaryKeys();
    Collections.sort(primaryKeys);
    for (String pk : tableMetadata.getPrimaryKeys()) {
      primVal.add(attrMap.get(pk).getValue());
    }
    List<Tuple> keysToDelete = getDeleteKeys(attrMap, primVal);
    for (Tuple x : keysToDelete) {
      t.clear(dir.pack(x));
    }
    return StatusCode.SUCCESS;

   }

   private List<Tuple> getDeleteKeys(HashMap<String, Record.Value> attrMap, List<Object> pkValues) { 
    List<Tuple> keysToDelete = new ArrayList<>(); 
    for (Map.Entry<String, Record.Value> entry : attrMap.entrySet()) {
      String attrName = entry.getKey();
      Tuple keyTuple = new Tuple();
      for (Object v : pkValues) {
        keyTuple = keyTuple.addObject(v);
      }
      keyTuple = keyTuple.add(attrName);
      keysToDelete.add(keyTuple);
      System.out.println(keyTuple);
    }
    return keysToDelete; 
   }

   private Record generateRecordFromRow(HashMap<String, Object> row) {
    Record r = new Record(); 
    
    r.setMapAttrNameToValue(row);

    Map<String, AttributeType> attributes = tableMetadata.getAttributes(); 
    // all cells have a value. no further processing. 
    if (tableMetadata.getAttributes().size() == row.size()) {
      return r; 
    } else { 
      for (Map.Entry<String, AttributeType> a : attributes.entrySet()) {
        if (!row.containsKey(a.getKey())) {
          r.setAttrNameAndValue(a.getKey(), null);
        }
      } // end for each
    } // end else
    return r;
  } // end generateRecordFromRow

  private boolean matchesPredicate(Record r) {

    Object recVal = r.getValueForGivenAttrName(predicateAttributeName);
    AttributeType recType = r.getTypeForGivenAttrName(predicateAttributeName);
    if (recVal == null || recType == null) return false;

    if (recType == AttributeType.INT) {
      return compareInt(recVal, predicateAttributeValue.getValue());
    } else if (recType == AttributeType.DOUBLE){
      return compareDouble(recVal, predicateAttributeValue.getValue());
    } else if (recType == AttributeType.VARCHAR) {
      return compareString(recVal, predicateAttributeValue.getValue());
    }

    return false; 
  }

  public StatusCode updateCurrentRecord(String[] attrNames, Object[] attrValues) {
    if (t == null) return StatusCode.CURSOR_INVALID;
    if (!isInit) {
      System.out.println("IN UPDATE\n");
      return StatusCode.CURSOR_NOT_INITIALIZED;
    }
    if (curr == null) return StatusCode.CURSOR_REACH_TO_EOF;
    
    IndexesImpl.clearIndexOnRecord(curr, tableName, tableMetadata, t);

    Set<String> currentAttrNames = curr.getMapAttrNameToValue().keySet();
    Set<String> primaryKeys = new HashSet<>(tableMetadata.getPrimaryKeys());

    boolean isUpdatingPK = false;
    for (int i = 0; i<attrNames.length; i++) {
      String attrNameToUpdate = attrNames[i];
      Object attrValToUpdate = attrValues[i];
      if (!currentAttrNames.contains(attrNameToUpdate)) return StatusCode.CURSOR_UPDATE_ATTRIBUTE_NOT_FOUND;
      if (!Record.Value.isTypeSupported(attrValToUpdate)) return StatusCode.ATTRIBUTE_TYPE_NOT_SUPPORTED;
      if (!isUpdatingPK && primaryKeys.contains(attrNameToUpdate)) isUpdatingPK = true;
    }

    if (isUpdatingPK) {
      StatusCode deleteStatus = deleteCurr();
      if (deleteStatus != StatusCode.SUCCESS) return deleteStatus;
    }

    for (int i = 0; i<attrNames.length; i++) {
      String attrNameToUpdate = attrNames[i];
      Object attrValToUpdate = attrValues[i];
      curr.setAttrNameAndValue(attrNameToUpdate, attrValToUpdate);
    }

    HashMap<String, Record.Value> attrMap = curr.getMapAttrNameToValue();
    List<Object> pkValues = new ArrayList<>(); 
    for (String pk : tableMetadata.getPrimaryKeys()) {
      pkValues.add(attrMap.get(pk).getValue());
    }
    for (Map.Entry<String, Record.Value> entry : attrMap.entrySet()) {
      String attrName = entry.getKey();
      Object value = entry.getValue().getValue();
      Tuple keyTuple = new Tuple();
      for (Object v : pkValues) {
        keyTuple = keyTuple.addObject(v);
      }
      keyTuple = keyTuple.add(attrName);
      Tuple valTuple = new Tuple().addObject(value);

      t.set(dir.pack(keyTuple), valTuple.pack());
    }
    IndexesImpl.updateIndex(curr, tableName, tableMetadata, t);
    return StatusCode.SUCCESS;
  }

  private boolean compareInt(Object a, Object b) {
    long c;
    if (a instanceof Integer) {
      c = new Long((Integer) a);
    } else {
      c = (long) a;
    }

    long d;
    if (b instanceof Integer) {
      d = new Long((Integer) b);
    } else {
      d = (long) b;
    }

    if (predicateOperator == ComparisonOperator.EQUAL_TO) return c == d; 
    else if (predicateOperator == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO) return c >= d; 
    else if (predicateOperator == ComparisonOperator.GREATER_THAN) return c > d; 
    else if (predicateOperator == ComparisonOperator.LESS_THAN) return c < d; 
    else if (predicateOperator == ComparisonOperator.LESS_THAN_OR_EQUAL_TO) return c <= d; 

    return false; 
  }
  private boolean compareDouble(Object a, Object b) {
    double c = (double) a; 
    double d = (double) b; 

    if (predicateOperator == ComparisonOperator.EQUAL_TO) return c == d; 
    else if (predicateOperator == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO) return c >= d; 
    else if (predicateOperator == ComparisonOperator.GREATER_THAN) return c > d; 
    else if (predicateOperator == ComparisonOperator.LESS_THAN) return c < d; 
    else if (predicateOperator == ComparisonOperator.LESS_THAN_OR_EQUAL_TO) return c <= d; 

    return false; 
  }
  private boolean compareString(Object a, Object b) {
    String c = (String) a; 
    String d = (String) b; 

    if (predicateOperator == ComparisonOperator.EQUAL_TO) return c.compareTo(d) == 0; 
    else if (predicateOperator == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO) return c.compareTo(d) <= 0; 
    else if (predicateOperator == ComparisonOperator.GREATER_THAN) return c.compareTo(d) < 0; 
    else if (predicateOperator == ComparisonOperator.LESS_THAN) return c.compareTo(d) > 0; 
    else if (predicateOperator == ComparisonOperator.LESS_THAN_OR_EQUAL_TO) return c.compareTo(d) >= 0; 

    return false; 
  }


}
