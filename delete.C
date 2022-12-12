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
  if(status != OK) {
      return status;
  }
  int count = 0; 
  RID rid;
  
  HeapFileScan relScan(relation, status);
  
  //Null attrName check
  if(attrName.length() == 0) {
      
  }
  
  //Check for type
  char* holder;
  switch(type) {
      case STRING
          holder = attrValue;
          break;
      
      case INTEGER
          holder = (char*)&(atoi(attrValue));
          break;
      
      case FLOAT
          holder = (char*)&(atof(attrValue));
          break;          
  }
  
  AttrDesc desc;
  status = attrCat->getInfo(relation, attrName, attrDesc);
  if(status != OK) {
      return status;
  }
  
  //Scan for tuples that match
  status = relScan.startSCan(arrtDesc.attrOffset, attrDesc.attrLen, type, holder,op);
  if(status != OK) {
      return status;
  }
  
  while(relScan.scanNext(rid) == OK) {
      status = relScan.deleteRecodr();
      if(status != OK) {
        return status;
      }    
      count++;
  }   
  
  return OK;



}


