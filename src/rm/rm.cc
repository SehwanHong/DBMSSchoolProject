#include "src/include/rm.h"

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createTableDescriptor() {
        Attribute attr;
        attr.name = "table-id";
        attr.length = 4;
        attr.type = AttrType::TypeInt;
        tableDescriptor.push_back(attr);
        attr.name = "table-name";
        attr.length = 50;
        attr.type = AttrType::TypeVarChar;
        tableDescriptor.push_back(attr);
        attr.name = "file-name";
        attr.length = 50;
        attr.type = AttrType::TypeVarChar;
        tableDescriptor.push_back(attr);
    }

    RC  RelationManager::createColumnDescriptor() {
        Attribute attr;
        attr.name = "table-id";
        attr.length = 4;
        attr.type = AttrType::TypeInt;
        columnDescriptor.push_back(attr);
        attr.name = "column-name";
        attr.length = 50;
        attr.type = AttrType::TypeVarChar;
        columnDescriptor.push_back(attr);
        attr.name = "column-type";
        attr.length = 4;
        attr.type = AttrType::TypeInt;
        columnDescriptor.push_back(attr);
        attr.name = "column-length";
        attr.length = 4;
        attr.type = AttrType::TypeInt;
        columnDescriptor.push_back(attr);
        attr.name = "column-position";
        attr.length = 4;
        attr.type = AttrType::TypeInt;
        columnDescriptor.push_back(attr);
    }

    RC RelationManager::createTableCatalog() {
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        unsigned short tableAttributeLength = tableDescriptor.size();
        unsigned short nullBytes = tableAttributeLength / 8 + (tableAttributeLength % 8 == 0 ? 0 : 1);
        unsigned int tableNameSize = table.size();
        unsigned short expectedLength = nullBytes + INTSIZE + tableNameSize + INTSIZE + tableNameSize;
        unsigned char * tableData = new unsigned char[expectedLength];

        const char * tableNameCstring = table.c_str();

        unsigned char null_indicator = 128;
        memcpy(tableData, &null_indicator, nullBytes);
        memcpy(tableData + nullBytes, &tableNameSize, INTSIZE);
        memcpy(tableData + nullBytes + INTSIZE, tableNameCstring, tableNameSize);
        memcpy(tableData + nullBytes + INTSIZE + tableNameSize, &tableNameSize, INTSIZE);
        memcpy(tableData + nullBytes + INTSIZE + tableNameSize + INTSIZE, tableNameCstring, tableNameSize);
        RID temp;
        recordBasedFileManager.insertRecord(tableFileHandle, tableDescriptor, tableData, temp);

        delete[] tableData;

        tableNameSize = column.size();
        expectedLength = nullBytes + INTSIZE + tableNameSize + INTSIZE + tableNameSize;
        tableData = new unsigned char[expectedLength];

        tableNameCstring = column.c_str();

        memcpy(tableData, &null_indicator, nullBytes);
        memcpy(tableData + nullBytes, &tableNameSize, INTSIZE);
        memcpy(tableData + nullBytes + INTSIZE, tableNameCstring, tableNameSize);
        memcpy(tableData + nullBytes + INTSIZE + tableNameSize, &tableNameSize, INTSIZE);
        memcpy(tableData + nullBytes + INTSIZE + tableNameSize + INTSIZE, tableNameCstring, tableNameSize);
        recordBasedFileManager.insertRecord(tableFileHandle, tableDescriptor, tableData, temp);

        delete[] tableData;

        return SUCCESS;
    }

    RC RelationManager::createColumnCatalog() {
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();

        int position;
        RC problem = SUCCESS;
        RID rid;

        for(position = 0; position < tableDescriptor.size() ; position++) {
            unsigned char * columnData = createAttributeDataNoTableID(tableDescriptor[position], position + 1);
            problem += recordBasedFileManager.insertRecord(columnFileHandle, columnDescriptor, columnData, rid);
            delete[] columnData;
        }

        for(position = 0; position < columnDescriptor.size() ; position++) {
            unsigned char * columnData = createAttributeDataNoTableID(columnDescriptor[position], position + 1);
            problem += recordBasedFileManager.insertRecord(columnFileHandle, columnDescriptor, columnData, rid);
            delete[] columnData;
        }

        return SUCCESS;
    }

    RC RelationManager::createCatalog() {
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        std::remove(table.c_str());
        std::remove(column.c_str());
        RC problem = recordBasedFileManager.createFile(table);
        recordBasedFileManager.openFile(table, tableFileHandle);
        problem += recordBasedFileManager.createFile(column);
        recordBasedFileManager.openFile(column,columnFileHandle);
        createTableDescriptor();
        createColumnDescriptor();
        catalogCreated = true;
        tableID = 0;

        createTableCatalog();
        createColumnCatalog();

        return problem;
    }

    RC RelationManager::emptyTableDescriptor() {
        tableDescriptor.erase(tableDescriptor.begin(), tableDescriptor.end());
        tableDescriptor.resize(0);
    }

    RC RelationManager::emptyColumnDescriptor() {
        columnDescriptor.erase(columnDescriptor.begin(), columnDescriptor.end());
        columnDescriptor.resize(0);
    }

    RC RelationManager::deleteAllTableFile() {
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        RC problem = recordBasedFileManager.openFile(table, fileHandle);
        std::vector<std::string> projectedAttribute = {"file-name"};
        RBFM_ScanIterator rbfmScanIterator;
        std::vector<std::string> toBeDeleted;
        problem += recordBasedFileManager.scan(fileHandle, tableDescriptor, "", NO_OP, NULL, projectedAttribute, rbfmScanIterator);
        RID temp;
        char * temporary_saving = new char[50];
        while(rbfmScanIterator.getNextRecord(temp, temporary_saving) != RBFM_EOF) {
            int numOfChar = *((int*)(temporary_saving + 1));
            char * fileName = new char[numOfChar + 1];
            memcpy(fileName,temporary_saving + 1 + INTSIZE, numOfChar);
            fileName[numOfChar] = '\0';
            toBeDeleted.push_back(fileName);
            delete[] fileName;
        }
        delete[] temporary_saving;
        for(int i = 0; i < toBeDeleted.size(); i++) {
            deleteTable(toBeDeleted[i]);
        }
        return SUCCESS;
    }

    RC RelationManager::deleteCatalog() {
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        const char * name = table.c_str();
        if (access(name, F_OK) != 0) { // check if the file name exists
            return RC_FILE_NAME_NOT_EXIST;
        }
        name = column.c_str();
        if (access(name, F_OK) != 0) { // check if the file name exists
            return RC_FILE_NAME_NOT_EXIST;
        }

        RC problem = deleteAllTableFile();
        emptyTableDescriptor();
        emptyColumnDescriptor();
        tableFileHandle.closeFile();
        columnFileHandle.closeFile();
        problem += recordBasedFileManager.destroyFile(table);
        problem += recordBasedFileManager.destroyFile(column);
        return problem;
    }

    unsigned char * RelationManager::createTableData(const std::string &tableName) {
        unsigned short tableAttributeLength = tableDescriptor.size();
        unsigned short nullBytes = tableAttributeLength / 8 + (tableAttributeLength % 8 == 0 ? 0 : 1);
        unsigned int tableNameSize = tableName.size();
        unsigned short expectedLength = nullBytes + INTSIZE + INTSIZE + tableNameSize + INTSIZE + tableNameSize;
        unsigned char * tableData = new unsigned char[expectedLength];

        const char * tableNameCstring = tableName.c_str();

        unsigned char null_indicator = 0;
        memcpy(tableData, &null_indicator, nullBytes);
        memcpy(tableData + nullBytes, &tableID, INTSIZE);
        memcpy(tableData + nullBytes + INTSIZE, &tableNameSize, INTSIZE);
        memcpy(tableData + nullBytes + INTSIZE + INTSIZE, tableNameCstring, tableNameSize);
        memcpy(tableData + nullBytes + INTSIZE + INTSIZE + tableName.size(), &tableNameSize, INTSIZE);
        memcpy(tableData + nullBytes + INTSIZE + INTSIZE + tableName.size() + INTSIZE, tableNameCstring, tableNameSize);

        return tableData;
    }

    unsigned char * RelationManager::createAttributeData(Attribute attr, int position) {
        unsigned short columnAttributeLength = columnDescriptor.size();
        unsigned short nullBytes = columnAttributeLength / 8 + (columnAttributeLength % 8 == 0 ? 0 : 1);
        unsigned int attrNameSize = attr.name.size();
        unsigned short expectedLength = nullBytes + INTSIZE + INTSIZE + attrNameSize + INTSIZE + INTSIZE + INTSIZE;
        unsigned char * attributeData = new unsigned char[expectedLength];

        const char * attributeNameCstring = attr.name.c_str();

        unsigned char null_indicator = 0;
        memcpy(attributeData, &null_indicator, nullBytes);
        memcpy(attributeData + nullBytes, &tableID, INTSIZE);
        memcpy(attributeData + nullBytes + INTSIZE, &attrNameSize, INTSIZE);
        memcpy(attributeData + nullBytes + INTSIZE + INTSIZE, attributeNameCstring, attrNameSize);
        memcpy(attributeData + nullBytes + INTSIZE + INTSIZE + attrNameSize, &attr.type, INTSIZE);
        memcpy(attributeData + nullBytes + INTSIZE + INTSIZE + attrNameSize + INTSIZE, &attr.length, INTSIZE);
        memcpy(attributeData + nullBytes + INTSIZE + INTSIZE + attrNameSize + INTSIZE + INTSIZE, &position, INTSIZE);

        return attributeData;
    }

    unsigned char * RelationManager::createAttributeDataNoTableID(Attribute attr, int position) {
        unsigned short columnAttributeLength = columnDescriptor.size();
        unsigned short nullBytes = columnAttributeLength / 8 + (columnAttributeLength % 8 == 0 ? 0 : 1);
        unsigned int attrNameSize = attr.name.size();
        unsigned short expectedLength = nullBytes + INTSIZE + INTSIZE + attrNameSize + INTSIZE + INTSIZE + INTSIZE;
        unsigned char * attributeData = new unsigned char[expectedLength];

        const char * attributeNameCstring = attr.name.c_str();

        unsigned char null_indicator = 128;
        memcpy(attributeData, &null_indicator, nullBytes);
        memcpy(attributeData + nullBytes, &attrNameSize, INTSIZE);
        memcpy(attributeData + nullBytes + INTSIZE, attributeNameCstring, attrNameSize);
        memcpy(attributeData + nullBytes + INTSIZE + attrNameSize, &attr.type, INTSIZE);
        memcpy(attributeData + nullBytes + INTSIZE + attrNameSize + INTSIZE, &attr.length, INTSIZE);
        memcpy(attributeData + nullBytes + INTSIZE + attrNameSize + INTSIZE + INTSIZE, &position, INTSIZE);

        return attributeData;
    }

    RC RelationManager::storeCurrentSystem(const std::string &tableName) {
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        int problem = SUCCESS;
        if (previousTableName != tableName ){ //} && previousRecordDescriptor.size() == 0) {
            previousTableID = getTableID(tableName);
            problem += getAttributes(tableName, previousRecordDescriptor);
            previousTableName = tableName;
            recordBasedFileManager.closeFile(previousFileHandle);
            recordBasedFileManager.openFile(previousTableName, previousFileHandle);
        }
        return problem;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        if (!catalogCreated) {
            return RC_RM_CREATE_CATALOG_ERROR;
        }
        std::remove(tableName.c_str());
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        RID rid;
        RC problem = SUCCESS;
        tableID += 1;
        unsigned char * tableData = createTableData(tableName);
        problem += recordBasedFileManager.insertRecord(tableFileHandle, tableDescriptor, tableData, rid);
        delete[] tableData;

        int position;

        for(position = 0; position < attrs.size() ; position++) {
            unsigned char * columnData = createAttributeData(attrs[position], position + 1);
            problem += recordBasedFileManager.insertRecord(columnFileHandle, columnDescriptor, columnData, rid);
            delete[] columnData;
        }
        if (previousTableName.size() != 0) {
            previousTableName = tableName;
            previousRecordDescriptor.erase(previousRecordDescriptor.begin(), previousRecordDescriptor.end());
            previousRecordDescriptor.insert(previousRecordDescriptor.begin(), attrs.begin(), attrs.end());
            previousTableID = tableID;
            recordBasedFileManager.closeFile(previousFileHandle);
        }
        problem += recordBasedFileManager.createFile(tableName);
        problem += recordBasedFileManager.openFile(tableName, previousFileHandle);

        return problem;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        std::vector<std::string> projectedAttribute = {"table-id"};
        std::vector<RID> rids;
        int table_id = getTableID(tableName);
        RBFM_ScanIterator rbfmScanIterator;
        RC problem = SUCCESS;
        if (table_id == 0) {
            return RC_RM_DELETE_TABLE_ERROR;
        }
        char * temporary_saving = new char[5];

        destroyAllIndex(tableName);

        recordBasedFileManager.scan(tableFileHandle, tableDescriptor, "table-id", EQ_OP, &table_id, projectedAttribute, rbfmScanIterator);
        RID table;
        while(rbfmScanIterator.getNextRecord(table, temporary_saving) != RBFM_EOF) {
            rids.push_back(table);
        }

        for(int i = 0; i < rids.size(); i++) {
            problem += recordBasedFileManager.deleteRecord(tableFileHandle, tableDescriptor, rids[i]);
        }

        rids.erase(rids.begin(), rids.end());

        recordBasedFileManager.scan(columnFileHandle, columnDescriptor, "table-id", EQ_OP, &table_id, projectedAttribute, rbfmScanIterator);
        RID column;
        while(rbfmScanIterator.getNextRecord(column, temporary_saving) != RBFM_EOF) {
            rids.push_back(column);
        }

        for(int i = 0; i < rids.size(); i++) {
            problem += recordBasedFileManager.deleteRecord(columnFileHandle, columnDescriptor, rids[i]);
        }

        delete[] temporary_saving;

        if (previousTableName == tableName) {
            previousRecordDescriptor.erase(previousRecordDescriptor.begin(), previousRecordDescriptor.end());
            previousTableName = "";
            previousTableID = 0;
            recordBasedFileManager.closeFile(previousFileHandle);
        }

        problem += recordBasedFileManager.destroyFile(tableName);
        return problem;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        if (previousTableName == tableName && &previousRecordDescriptor != &attrs) {
            attrs.erase(attrs.begin(), attrs.end());
            attrs.insert(attrs.begin(), previousRecordDescriptor.begin(), previousRecordDescriptor.end());
            return SUCCESS;
        }
        if (table == tableName) {
            attrs.erase(attrs.begin(), attrs.end());
            attrs.insert(attrs.begin(), tableDescriptor.begin(), tableDescriptor.end());
            return SUCCESS;
        }
        if (column == tableName) {
            attrs.erase(attrs.begin(), attrs.end());
            attrs.insert(attrs.begin(), columnDescriptor.begin(), columnDescriptor.end());
            return SUCCESS;
        }
        if (access(tableName.c_str(), F_OK) != 0) {
            return RC_FILE_NAME_NOT_EXIST;
        }
        attrs.erase(attrs.begin(), attrs.end());

        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        std::vector<std::string> projectedAttribute = {"table-id"};
        std::vector<RID> rids;
        int table_id = getTableID(tableName);
        RBFM_ScanIterator rbfmScanIterator;
        char * temporary_saving = new char[5];
        RC problem = SUCCESS;

        recordBasedFileManager.scan(tableFileHandle, tableDescriptor, "table-id", EQ_OP, &table_id, projectedAttribute, rbfmScanIterator);
        RID table;
        while(rbfmScanIterator.getNextRecord(table, temporary_saving) != RBFM_EOF) {
            rids.push_back(table);
        }

        if(rids.size() <=0) {
            return RC_RM_DELETE_TABLE_ERROR;
        }

        rids.erase(rids.begin(), rids.end());

        recordBasedFileManager.scan(columnFileHandle, columnDescriptor, "table-id", EQ_OP, &table_id, projectedAttribute, rbfmScanIterator);
        RID column;
        while(rbfmScanIterator.getNextRecord(column, temporary_saving) != RBFM_EOF) {
            rids.push_back(column);
        }

        int maxPossibleLength = 0;
        for(int i = 0; i < columnDescriptor.size(); i++) {columnDescriptor[i].type == AttrType::TypeVarChar ? maxPossibleLength += columnDescriptor[i].length + 4 : maxPossibleLength += columnDescriptor[i].length; }

        attrs.resize(rids.size());

        for(int i = 0; i < rids.size(); i++) {
            char * record = new char[maxPossibleLength];
            problem += recordBasedFileManager.readRecord(columnFileHandle, columnDescriptor, rids[i], record);

            unsigned int columnDescriptorLength = columnDescriptor.size();
            unsigned int nullBytes = columnDescriptorLength / 8 + (columnDescriptorLength % 8 == 0 ? 0 : 1);\
            unsigned offset = 0;
            offset += nullBytes;
            int tableID = *((int*)(record+offset));
            offset += INTSIZE;
            int columnNameLength = *((int*)(record+offset));
            offset += INTSIZE;
            char * columnName = record+offset;
            offset += columnNameLength;
            int columnType = *((int*)(record+offset));
            offset += INTSIZE;
            int columnLength = *((int*)(record+offset));
            offset += INTSIZE;
            int columnPosition = *((int*)(record+offset));
            offset += INTSIZE;
            switch(columnType) {
                case 0 :
                    attrs[columnPosition - 1].type = AttrType::TypeInt;
                    break;
                case 1 :
                    attrs[columnPosition - 1].type = AttrType::TypeReal;
                    break;
                case 2 :
                    attrs[columnPosition - 1].type = AttrType::TypeVarChar;
                    break;
                default:
                    delete[] record;
                    return RC_COLUMN_READ_ERROR;
            }
            attrs[columnPosition - 1].length = columnLength;
            char * nameString = new char[columnNameLength + 1];
            memcpy(nameString, columnName, columnNameLength);
            nameString[columnNameLength] = '\0';
            attrs[columnPosition - 1].name = nameString;
            delete[] record;
            delete[] nameString;
        }

        if (previousTableName != tableName && &previousRecordDescriptor != &attrs) {
            previousRecordDescriptor.erase(previousRecordDescriptor.begin(), previousRecordDescriptor.end());
            previousRecordDescriptor.insert(previousRecordDescriptor.begin(), attrs.begin(), attrs.end());
            previousTableID = getTableID(tableName);
            previousTableName = tableName;
            recordBasedFileManager.closeFile(previousFileHandle);
            recordBasedFileManager.openFile(previousTableName, previousFileHandle);
        }

        delete[] temporary_saving;
        return problem;
    }

    unsigned RelationManager::getTableID(const std::string &tableName) {
        if (table == tableName) {
            return 0;
        }
        if (column == tableName) {
            return 0;
        }

        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        std::vector<std::string> projectedAttribute = {"table-id"};
        std::vector<RID> rids;
        int table_id = 0;
        RBFM_ScanIterator rbfmScanIterator;
        unsigned tableNameLength = tableName.size();
        char* tableNameCstring = new char[tableName.size() + INTSIZE];
        memcpy(tableNameCstring + INTSIZE, tableName.c_str(), tableName.size());
        memcpy(tableNameCstring, &tableNameLength, INTSIZE);
        RC problem = recordBasedFileManager.scan(tableFileHandle, tableDescriptor, "table-name", EQ_OP, tableNameCstring, projectedAttribute, rbfmScanIterator);
        RID temp;
        char* temporary_saving = new char[5];
        while(rbfmScanIterator.getNextRecord(temp, temporary_saving) != RBFM_EOF) {
            rids.push_back(temp);
            table_id = *((int*)(temporary_saving + 1));
        }
        if (rids.size() == 0) {
            delete[] temporary_saving;
        } else if (rids.size() == 1) {
            problem += SUCCESS;
        } else {
            delete[] temporary_saving;
        }

        return table_id;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        if (access(tableName.c_str(), F_OK) != 0) {
            return RC_FILE_NAME_NOT_EXIST;
        }
        if (table == tableName) {
            return RC_INSERT_TUPLE_ERROR;
        }
        if (column == tableName) {
            return RC_INSERT_TUPLE_ERROR;
        }
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        RC problem = SUCCESS;
        problem += storeCurrentSystem(tableName);
        problem += recordBasedFileManager.insertRecord(previousFileHandle, previousRecordDescriptor, data, rid);
        problem += insertIndex(previousTableName, data, rid);
        return problem;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        if (access(tableName.c_str(), F_OK) != 0) {
            return RC_FILE_NAME_NOT_EXIST;
        }
        if (table == tableName) {
            return RC_INSERT_TUPLE_ERROR;
        }
        if (column == tableName) {
            return RC_INSERT_TUPLE_ERROR;
        }
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        RC problem = SUCCESS;
        problem += storeCurrentSystem(tableName);
        char * data = new char[PAGE_SIZE];
        problem += recordBasedFileManager.readRecord(previousFileHandle, previousRecordDescriptor, rid, data);
        problem += deleteIndex(previousTableName, data, rid);
        problem += recordBasedFileManager.deleteRecord(previousFileHandle, previousRecordDescriptor, rid);
        return problem;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        if (access(tableName.c_str(), F_OK) != 0) {
            return RC_FILE_NAME_NOT_EXIST;
        }
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        RC problem = SUCCESS;
        problem += storeCurrentSystem(tableName);
        char * prevData = new char[PAGE_SIZE];
        problem += recordBasedFileManager.readRecord(previousFileHandle, previousRecordDescriptor, rid, prevData);
        problem += deleteIndex(previousTableName, prevData, rid);
        problem += recordBasedFileManager.updateRecord(previousFileHandle, previousRecordDescriptor, data, rid);
        problem += insertIndex(previousTableName, data, rid);
        return problem;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        if (access(tableName.c_str(), F_OK) != 0) {
            return RC_FILE_NAME_NOT_EXIST;
        }
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        RC problem = SUCCESS;
        problem += storeCurrentSystem(tableName);
        problem += recordBasedFileManager.readRecord(previousFileHandle, previousRecordDescriptor, rid, data);
        return problem;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        RC problem = SUCCESS;
        problem += recordBasedFileManager.printRecord(attrs, data, out);
        return problem;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        if (access(tableName.c_str(), F_OK) != 0) {
            return RC_FILE_NAME_NOT_EXIST;
        }
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        RC problem = SUCCESS;
        problem += storeCurrentSystem(tableName);
        problem += recordBasedFileManager.readAttribute(previousFileHandle, previousRecordDescriptor, rid, attributeName, data);
        return problem;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        if (access(tableName.c_str(), F_OK) != 0) {
            rm_ScanIterator.open(true);
            return RC_FILE_NAME_NOT_EXIST;
        }
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        RC problem = SUCCESS;
        if (table == tableName) {
            rm_ScanIterator.open(tableFileHandle, tableDescriptor, attributeNames, conditionAttribute, compOp, value);
            return SUCCESS;
        }
        if (column == tableName) {
            rm_ScanIterator.open(columnFileHandle, columnDescriptor, attributeNames, conditionAttribute, compOp, value);
            return SUCCESS;
        }
        problem += storeCurrentSystem(tableName);
        rm_ScanIterator.open(previousFileHandle, previousRecordDescriptor, attributeNames, conditionAttribute, compOp, value);
        return problem;
    }

    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName) {
        std::string indexName = tableName + attributeName + ".idx";

        IndexManager &indexManager = IndexManager::instance();
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        indexManager.createFile(indexName);

        int problem = SUCCESS;
        std::vector<std::string> projection;
        projection.push_back(attributeName);

        std::vector<Attribute> indexAttribute;

        createTable(indexName, indexAttribute);
        problem += storeCurrentSystem(tableName);

        RBFM_ScanIterator rbfmScanIterator;

        int attributeLocation;
        for( attributeLocation = 0 ; attributeLocation < previousRecordDescriptor.size(); attributeLocation++ ){
            if (previousRecordDescriptor.at(attributeLocation).name == attributeName) {
                break;
            }
        }

        problem += recordBasedFileManager.scan(previousFileHandle, previousRecordDescriptor, previousRecordDescriptor.at(attributeLocation).name, NO_OP, NULL, projection, rbfmScanIterator);

        RID newRid;
        char* data = new char[PAGE_SIZE];
        while (rbfmScanIterator.getNextRecord(newRid, data) != RBFM_EOF) {
            IXFileHandle indexFileHandle;
            problem += indexManager.openFile(indexName, indexFileHandle);
            problem += indexManager.insertEntry(indexFileHandle, previousRecordDescriptor.at(attributeLocation), data + 1, newRid);
            indexManager.closeFile(indexFileHandle);
        }

        delete[] data;
        return problem;
    }

    RC RelationManager::getAttribute(const void *data, std::vector<Attribute> &recordDescriptor,
                                     int &index, const void *key) {
        // calculate how many bytes data is using
        unsigned int length = recordDescriptor.size();
        unsigned int null_bytes = length / 8 + (length % 8 == 0 ? 0 : 1); // get number of bytes that indicate null

        unsigned char * null_indicator = new unsigned char[null_bytes];
        unsigned offset = 0;
        bool nullBit = false;
        memcpy(null_indicator, (char*)data+offset, null_bytes);
        offset += null_bytes;
        for(int i = 0 ; i < length ; i++ ){
            unsigned int number_of_char = 0;
            unsigned char * readAttr = (unsigned char*) key;
            unsigned char mask = (unsigned)1 << (unsigned) (7 - i % 8);

            nullBit = null_indicator[i/8] & mask;

            if ( i == index ) {
                switch (recordDescriptor[i].type) {
                    case AttrType::TypeInt:
                        memcpy((char*)key, (char*)data + offset, INTSIZE);
                        break;
                    case AttrType::TypeReal:
                        memcpy((char*)key, (char*)data + offset, FLOATSIZE);
                    case AttrType::TypeVarChar:
                        unsigned int number_of_char =  *((unsigned int * )((char*)data+offset));
                        memcpy((char*)key, (char*)data + offset, INTSIZE);
                        offset += INTSIZE;
                        memcpy((char*)key + INTSIZE, (char*)data + offset, number_of_char);
                }
                break;
            }
            if (!nullBit){
                switch (recordDescriptor[i].type) {
                    case AttrType::TypeInt:
                        offset += INTSIZE; break;
                    case AttrType::TypeReal:
                        offset += FLOATSIZE; break;
                    case AttrType::TypeVarChar:
                        unsigned int number_of_char =  *((unsigned int * )((char*)data+offset));
                        offset += INTSIZE;
                        offset += number_of_char;
                }
            }
        }
        delete[] null_indicator;
        return SUCCESS;
    }

    RC RelationManager::insertIndex(const std::string &tableName, const void * data, const RID &rid){
        int problem = SUCCESS;
        problem += storeCurrentSystem(tableName);

        std::vector<int> existingIndexAttributeNumber;
        std::vector<std::string> existingIndexFileName;

        IndexManager &indexManager = IndexManager::instance();
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();

        for(int i = 0; i < previousRecordDescriptor.size(); i++) {
            std::string indexName = tableName + previousRecordDescriptor.at(i).name + ".idx";
            int tableID = getTableID(indexName);
            if (tableID != 0) {
                existingIndexAttributeNumber.push_back(i);
                existingIndexFileName.push_back(indexName);
            }
        }

        char * key = new char[PAGE_SIZE];
        for(int i = 0; i < existingIndexAttributeNumber.size(); i++) {
            int index = existingIndexAttributeNumber.at(i);
            IXFileHandle indexFileHandle;
            problem += indexManager.openFile(existingIndexFileName.at(i), indexFileHandle);
            problem += getAttribute(data, previousRecordDescriptor, index, key);
            problem += indexManager.insertEntry(indexFileHandle, previousRecordDescriptor.at(index), key, rid);
            indexManager.closeFile(indexFileHandle);
        }
        delete[] key;

        return problem;
    }

    RC RelationManager::deleteIndex(const std::string &tableName, const void * data, const RID &rid) {
        int problem = SUCCESS;
        problem += storeCurrentSystem(tableName);

        std::vector<int> existingIndexAttributeNumber;
        std::vector<std::string> existingIndexFileName;

        IndexManager &indexManager = IndexManager::instance();
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();

        for(int i = 0; i < previousRecordDescriptor.size(); i++) {
            std::string indexName = tableName + previousRecordDescriptor.at(i).name + ".idx";
            int tableID = getTableID(indexName);
            if (tableID != 0) {
                existingIndexAttributeNumber.push_back(i);
                existingIndexFileName.push_back(indexName);
            }
        }

        char * key = new char[PAGE_SIZE];
        for(int i = 0; i < existingIndexAttributeNumber.size(); i++) {
            int index = existingIndexAttributeNumber.at(i);
            IXFileHandle indexFileHandle;
            problem += indexManager.openFile(existingIndexFileName.at(i), indexFileHandle);
            problem += getAttribute(data, previousRecordDescriptor, index, key);
            problem += indexManager.deleteEntry(indexFileHandle, previousRecordDescriptor.at(index), key, rid);
            indexManager.closeFile(indexFileHandle);
        }
        delete[] key;

        return problem;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {
        std::string indexName = tableName + attributeName + ".idx";
        int problem = SUCCESS;
        IndexManager &indexManager = IndexManager::instance();
        problem += indexManager.destroyFile(indexName);
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        std::vector<std::string> projectedAttribute = {"table-id"};
        std::vector<RID> rids;
        int table_id = getTableID(indexName);
        RBFM_ScanIterator rbfmScanIterator;
        char * temporary_saving = new char[5];
        if (table_id == 0) {
            return RC_RM_DELETE_TABLE_ERROR;
        }

        recordBasedFileManager.scan(tableFileHandle, tableDescriptor, "table-id", EQ_OP, &table_id, projectedAttribute, rbfmScanIterator);
        RID table;
        while(rbfmScanIterator.getNextRecord(table, temporary_saving) != RBFM_EOF) {
            rids.push_back(table);
        }

        if (rids.size() <= 0) {
            return RC_RM_DELETE_TABLE_ERROR;
        }

        problem += recordBasedFileManager.deleteRecord(tableFileHandle, tableDescriptor, rids.at(0));

        delete[] temporary_saving;
        return problem;
    }

    RC RelationManager::destroyAllIndex(const std::string &tableName) {
        int problem = SUCCESS;
        problem += storeCurrentSystem(tableName);

        IndexManager &indexManager = IndexManager::instance();
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();

        for(int i = 0; i < previousRecordDescriptor.size(); i++) {
            std::string indexName = tableName + previousRecordDescriptor.at(i).name + ".idx";
            int tableID = getTableID(indexName);
            if (tableID != 0) {
                destroyIndex(tableName, previousRecordDescriptor.at(i).name);
            }
        }

        return problem;
    }

    RC RelationManager::indexScan(const std::string &tableName, const std::string &attributeName, const void *lowKey,
                                  const void *highKey, bool lowKeyInclusive, bool highKeyInclusive,
                                  RM_IndexScanIterator &rm_IndexScanIterator) {

        std::string indexName = tableName + attributeName + ".idx";
        if (access(indexName.c_str(), F_OK) != 0) {
            return RC_FILE_NAME_NOT_EXIST;
        }
        int problem = SUCCESS;
        problem = storeCurrentSystem(tableName);
        int index;
        for( index = 0; index < previousRecordDescriptor.size(); index++ ) {
            if (attributeName == previousRecordDescriptor.at(index).name) {
                break;
            }
        }
        problem += rm_IndexScanIterator.open(indexName, previousRecordDescriptor.at(index), lowKey, highKey, lowKeyInclusive, highKeyInclusive);
        return problem;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        return scanIterator.getNextRecord(rid, data);
    }

    RC RM_ScanIterator::close() {
        if (failsOpen) {
            return SUCCESS;
        } else {
            return scanIterator.close();
        }
    }

    RC RM_ScanIterator::open(bool wrongOpen) {
        failsOpen = wrongOpen;
    }

    RC RM_ScanIterator::open(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const std::vector<std::string> &attributeName, const std::string &conditionAttribute, const CompOp comparisonOperation, const void * comparisonValue) {
        storedDescriptor.erase(storedDescriptor.begin(), storedDescriptor.end());
        storedDescriptor.insert(storedDescriptor.begin(), recordDescriptor.begin(), recordDescriptor.end());
        scanIterator.open(fileHandle, storedDescriptor, attributeName, conditionAttribute, comparisonOperation, comparisonValue);
        scanIterator.storedFileHandle.checkFilePointer();
        failsOpen = false;
    }

    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::open(std::string &indexName,
                                  Attribute &attribute,
                                  const void *lowKey,
                                  const void *highKey,
                                  bool lowKeyInclusive,
                                  bool highKeyInclusive) {
        IndexManager &indexManager = IndexManager::instance();
        int problem = SUCCESS;

        problem += indexManager.openFile(indexName, indexFileHandle);
        problem += indexManager.scan(indexFileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, indexScanIterator);
        return problem;
    }

    RC RM_IndexScanIterator::close() {
        IndexManager &indexManager = IndexManager::instance();
        int problem = SUCCESS;
        problem += indexManager.closeFile(indexFileHandle);
        problem += indexScanIterator.close();
        return SUCCESS;
    }

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
        int problem = indexScanIterator.getNextEntry(rid, key);
        if (problem == IX_EOF) {
            problem = RM_EOF;
        }
        return problem;
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

} // namespace PeterDB