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
        delete[] tableFileHandle.savedFileName;
        delete[] columnFileHandle.savedFileName;
        tableFileCollection.erase(tableFileCollection.begin(),tableFileCollection.end());
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
        previousTableName = tableName;
        previousRecordDescriptor.erase(previousRecordDescriptor.begin(), previousRecordDescriptor.end());
        previousRecordDescriptor.insert(previousRecordDescriptor.begin(), attrs.begin(), attrs.end());
        previousTableID = tableID;

        problem += recordBasedFileManager.createFile(tableName);
        FileHandle fileHandle;
        problem += recordBasedFileManager.openFile(tableName, fileHandle);
        tableFileCollection.push_back(fileHandle);

        return problem;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        std::vector<std::string> projectedAttribute = {"table-id"};
        std::vector<RID> rids;
        int table_id = getTableID(tableName);
        RBFM_ScanIterator rbfmScanIterator;
        RC problem = SUCCESS;
        char * temporary_saving = new char[5];
        if (table_id == 0) {
            return RC_RM_DELETE_TABLE_ERROR;
        }

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

        delete[] tableFileCollection.at(table_id-1).savedFileName;

        delete[] temporary_saving;

        problem += recordBasedFileManager.destroyFile(tableName);
        if (previousTableName == tableName) {
            previousRecordDescriptor.erase(previousRecordDescriptor.begin(), previousRecordDescriptor.end());
            previousTableName = "";
            previousTableID = 0;
        }
        return problem;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        if (previousTableName == tableName) {
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
            problem += recordBasedFileManager.readRecord(tableFileCollection.at(table_id-1), columnDescriptor, rids[i], record);

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
                    attrs[columnPosition].type = AttrType::TypeInt;
                    break;
                case 1 :
                    attrs[columnPosition].type = AttrType::TypeReal;
                    break;
                case 2 :
                    attrs[columnPosition].type = AttrType::TypeVarChar;
                    break;
                default:
                    delete[] record;
                    return RC_COLUMN_READ_ERROR;
            }
            attrs[columnPosition].length = columnLength;
            char * nameString = new char[columnNameLength + 1];
            memcpy(nameString, columnName, columnNameLength);
            nameString[columnNameLength] = '\0';
            attrs[columnPosition].name = nameString;
            delete[] record;
            delete[] nameString;
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
        if (previousTableName != tableName || previousRecordDescriptor.size() == 0) {
            problem += getAttributes(tableName, previousRecordDescriptor);
            previousTableID = getTableID(tableName);
            previousTableName = tableName;
        }
        problem += recordBasedFileManager.insertRecord(tableFileCollection[previousTableID-1], previousRecordDescriptor, data, rid);
        return problem;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        if (access(tableName.c_str(), F_OK) != 0) {
            return RC_FILE_NAME_NOT_EXIST;
        }
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        RC problem = SUCCESS;
        if (previousTableName != tableName) {
            problem += getAttributes(tableName, previousRecordDescriptor);
            previousTableID = getTableID(tableName);
            previousTableName = tableName;
        }
        problem += recordBasedFileManager.deleteRecord(tableFileCollection[previousTableID-1], previousRecordDescriptor, rid);
        return problem;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        if (access(tableName.c_str(), F_OK) != 0) {
            return RC_FILE_NAME_NOT_EXIST;
        }
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        RC problem = SUCCESS;
        if (previousTableName != tableName) {
            problem += getAttributes(tableName, previousRecordDescriptor);
            previousTableID = getTableID(tableName);
            previousTableName = tableName;
        }
        problem += recordBasedFileManager.updateRecord(tableFileCollection[previousTableID-1], previousRecordDescriptor, data, rid);
        return problem;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        if (access(tableName.c_str(), F_OK) != 0) {
            return RC_FILE_NAME_NOT_EXIST;
        }
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        RC problem = SUCCESS;
        if (previousTableName != tableName) {
            problem += getAttributes(tableName, previousRecordDescriptor);
            previousTableID = getTableID(tableName);
            previousTableName = tableName;
        }
        problem += recordBasedFileManager.readRecord(tableFileCollection[previousTableID-1], previousRecordDescriptor, rid, data);
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
        if (previousTableName != tableName) {
            problem += getAttributes(tableName, previousRecordDescriptor);
            previousTableID = getTableID(tableName);
            previousTableName = tableName;
        }
        problem += recordBasedFileManager.readAttribute(tableFileCollection[previousTableID-1], previousRecordDescriptor, rid, attributeName, data);
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
        if (previousTableName != tableName) {
            problem += getAttributes(tableName, previousRecordDescriptor);
            previousTableID = getTableID(tableName);
            previousTableName = tableName;
        }
        if (table == tableName) {
            rm_ScanIterator.open(tableFileHandle, tableDescriptor, attributeNames, conditionAttribute, compOp, value);
            return SUCCESS;
        }
        if (column == tableName) {
            rm_ScanIterator.open(columnFileHandle, columnDescriptor, attributeNames, conditionAttribute, compOp, value);
            return SUCCESS;
        }
        rm_ScanIterator.open(tableFileCollection[previousTableID-1], previousRecordDescriptor, attributeNames, conditionAttribute, compOp, value);
        return problem;
    }

    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName) {

        return SUCCESS;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {
        return SUCCESS;
    }

    RC RelationManager::indexScan(const std::string &tableName, const std::string &attributeName, const void *lowKey,
                                  const void *highKey, bool lowKeyInclusive, bool highKeyInclusive,
                                  RM_IndexScanIterator &rm_IndexScanIterator) {
        return SUCCESS;
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
        failsOpen = false;
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