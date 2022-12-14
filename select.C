#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;

	Status status;

	AttrDesc attrDescArray[projCnt];
    for (int i = 0; i < projCnt; i++)
    {
        Status status = attrCat->getInfo(projNames[i].relName,
                                         projNames[i].attrName,
                                         attrDescArray[i]);
        if (status != OK)
        {
            return status;
        }
    }

	AttrDesc attrDesc;
	const char *filter;
	if(attr != NULL) {
		status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
		if (status != OK)
		{
			return status;
		}

		int ival;
		float fval;
		switch(attr->attrType) {
			case STRING:
				filter = attrValue;
				break;

			case INTEGER:
				ival = atoi(attrValue);
				filter = (char*)&(ival);
				break;

			case FLOAT:
				fval = atof(attrValue);
				filter = (char*)&(fval);
				break;          
		}
	} else {
		attrDesc = attrDescArray[0];
		char *temp = new char();
		*temp = NULL;
		filter = temp;
	}
	int reclen = 0;
    for (int i = 0; i < projCnt; i++)
    {
        reclen += attrDescArray[i].attrLen;
    }

	

	//cout << "Doing ScanSelect" << endl;
	status = ScanSelect(result, projCnt, attrDescArray, &attrDesc, op, filter, reclen);
	return status;

}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

	Status status;

	InsertFileScan resultRel(result, status);
    if (status != OK) { return status; }

    char outputData[reclen];
    Record outputRec;
    outputRec.data = (void *) outputData;
    outputRec.length = reclen;

	HeapFileScan hScan(string(attrDesc->relName), status);
	if (status != OK) { return status; }
    status = hScan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype) attrDesc->attrType, filter, op);
    if (status != OK) { return status; }

	RID cRID;
	Record cRec;

	while(hScan.scanNext(cRID) == OK) {
        status = hScan.getRecord(cRec);
        ASSERT(status == OK);

		int outputOffset = 0;
		for (int i = 0; i < projCnt; i++) {
			memcpy(outputData + outputOffset, (char *)cRec.data + projNames[i].attrOffset, projNames[i].attrLen);
			outputOffset += projNames[i].attrLen;
		} 

		// add the new record to the output relation
		RID outRID;
		status = resultRel.insertRecord(outputRec, outRID);
		ASSERT(status == OK);
		//cout << "added recordqu" << endl; 
	}
	return OK;
}