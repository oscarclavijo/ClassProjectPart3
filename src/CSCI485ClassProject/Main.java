package CSCI485ClassProject;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import CSCI485ClassProject.StatusCode;
import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.TableManager;
import CSCI485ClassProject.TableManagerImpl;
import CSCI485ClassProject.models.TableMetadata;
import CSCI485ClassProject.Records;
import CSCI485ClassProject.models.Record;
public class Main {
    
  public static String EmployeeTableName = "Employee";
  public static String SSN = "SSN";
  public static String Name = "Name";
  public static String Email = "Email";
  public static String Age = "Age";
  public static String Address = "Address";
  public static String Salary = "Salary";

  public static String[] EmployeeTableAttributeNames = new String[]{SSN, Name, Email, Age, Address};
  public static String[] EmployeeTableNonPKAttributeNames = new String[]{Name, Email, Age, Address};
  public static AttributeType[] EmployeeTableAttributeTypes =
      new AttributeType[]{AttributeType.INT, AttributeType.VARCHAR, AttributeType.VARCHAR, AttributeType.INT, AttributeType.VARCHAR};

  public static String[] UpdatedEmployeeTableNonPKAttributeNames = new String[]{Name, Email, Age, Address, Salary};
  public static String[] EmployeeTablePKAttributes = new String[]{"SSN"};


  public static int initialNumberOfRecords = 100;
  public static int updatedNumberOfRecords = initialNumberOfRecords / 2;

  private TableManager tableManager;
  private Records records;

  private static String getName(long i) {
    return "Name" + i;
  }

  private static String getEmail(long i) {
    return "ABCDEFGH" + i + "@usc.edu";
  }

  private static long getAge(long i) {
    return (i+25)%90;
  }

  private static String getAddress(long i) {
    return "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + i;
  }

  private static long getSalary(long i) {
    return i + 100;
  }
    public static void main(String[] args) {
      System.out.println("Starting");
        TableManager a = new TableManagerImpl();
        a.dropAllTables();

        Records r = new RecordsImpl(); 

        // create the Employee Table, verify that the table is created
        TableMetadata EmployeeTable = new TableMetadata(EmployeeTableAttributeNames, EmployeeTableAttributeTypes,
            EmployeeTablePKAttributes);
        a.createTable(EmployeeTableName,
            EmployeeTableAttributeNames, EmployeeTableAttributeTypes, EmployeeTablePKAttributes);
        HashMap<String, TableMetadata> tables = a.listTables();
    
        System.out.println("Loop");
        for (int i = 0; i<initialNumberOfRecords; i++) {
          long ssn = i;
          String name = getName(i);
          String email = getEmail(i);
          long age = getAge(i);
          String address = getAddress(i);
    
          Object[] primaryKeyVal = new Object[] {ssn};
          Object[] nonPrimaryKeyVal = new Object[] {name, email, age, address};
    
          r.insertRecord(EmployeeTableName, EmployeeTablePKAttributes, primaryKeyVal, EmployeeTableNonPKAttributeNames, nonPrimaryKeyVal);
        }

        IndexesImpl in = new IndexesImpl();
        in.createIndex(EmployeeTableName, Name, null);
    }
  }