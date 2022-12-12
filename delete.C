#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
  // part 6
  
  Status status;
  int count = 0; 
  RID rid;
  
  HeapFileScan relScan(relation, status);
  
  //Null attrName check
  if(attrName.length() == 0) {  //not sure about length check
      //set offset to 0
      //set length to 0
      //set type to string
      //set filter to null
      status = relScan.startScan(0, 0, STRING, NULL, EQ);    
  } else {
      //Check for type
      const char* holder;
      int holder2;
      switch(type) {
          case STRING:
              holder = attrValue;
              break;
      
          case INTEGER:
              holder2 = atoi(attrValue);
              holder = (char*)&(holder2);
              break;
        
          case FLOAT:
              holder2 = atoi(attrValue);
              holder = (char*)&(holder2);
              break;          
      }
  
      AttrDesc desc;
      status = attrCat->getInfo(relation, attrName, desc);
      if(status != OK) {
          return status;
      }
      status = relScan.startScan(desc.attrOffset, desc.attrLen, type, holder,op);
  }
  
  if(status != OK) {
      return status;
  } 
  
  while(relScan.scanNext(rid) == OK) {
      status = relScan.deleteRecord();
      if(status != OK) {
          return status;
      }    
      count++;
  }   
  
  return OK;
}


