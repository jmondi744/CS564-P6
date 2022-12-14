#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{

	Status status;
	Record record;
	int catattrCnt;
	AttrDesc * attrs;
	status = attrCat->getRelInfo(relation, catattrCnt, attrs);
	if(status) {
		return status;
	}
	int length = 0;
	for(int i = 0; i < catattrCnt; i++) {
		length += attrs[i].attrLen;
	}
	record.length = length;
	char data[length];
	record.data = (void *) data;
	for(int i = 0; i < attrCnt; i++) {
		for(int y = 0; y < catattrCnt; y++) {
			if(!strcmp(attrList[i].attrName, attrs[y].attrName)) {
				if(attrList[i].attrValue == NULL) {
					return BADCATPARM;
				} else {
					if(attrList[i].attrType == INTEGER) {
						int a = atoi((char *) attrList[i].attrValue);
						memcpy(data+ attrs[y].attrOffset, (char *)&a, attrs[y].attrLen);
					} else if(attrList[i].attrType == FLOAT) {
						float a = atof((char *) attrList[i].attrValue);
						memcpy(data + attrs[y].attrOffset, (char *)&a, attrs[y].attrLen);
					} else if(attrList[i].attrType == FLOAT) {
						memcpy(data + attrs[y].attrOffset, (char *) attrList[i].attrValue, attrs[y].attrLen);
					} else {
						return ATTRTYPEMISMATCH;
					}
				}
			}
		}
	}
	RID rid;
	InsertFileScan ifs(relation, status);
	if(status) {
		return status;
	}
	status = ifs.insertRecord(record, rid);
	if(status) {
		return status;
	}
// part 6
	return OK;

}
