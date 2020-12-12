#include "src/include/rbfm.h"

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        PagedFileManager &pfm = PagedFileManager::instance();
        return pfm.createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        PagedFileManager &pfm = PagedFileManager::instance();
        return pfm.destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        PagedFileManager &pfm = PagedFileManager::instance();
        return pfm.openFile(fileName, fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        PagedFileManager &pfm = PagedFileManager::instance();
        return pfm.closeFile(fileHandle);
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        unsigned number_of_pages = fileHandle.getNumberOfPages();

        unsigned short offset = getRecordLength(recordDescriptor, data);

        // find the page that have free space
        unsigned pageNum = findEmptyPage(fileHandle, offset, number_of_pages);

        // insert the record in the page
        unsigned char * pageData = new unsigned char[PAGE_SIZE];
        fileHandle.readPage(pageNum, pageData); // read page from the file

        unsigned short slotNum;
        unsigned short slotOffset;

        editSlotDirectory(pageData, slotNum, slotOffset, offset);


        // edit the page
        writeAtFreeSpace(pageData, data, slotOffset, offset);
        // save stored page number and the slot number
        fileHandle.writePage(pageNum, pageData); // save the page to the file

        unsigned short * pageSlot = (unsigned short *) pageData;
        unsigned lastSlotInserted = pageSlot[PAGE_SIZE/SHORTSIZE - 4];
        unsigned maxNumOfSlot = pageSlot[PAGE_SIZE/SHORTSIZE - 2];
        unsigned availableStorage = pageSlot[PAGE_SIZE/SHORTSIZE - 1];
        unsigned short endOfData = 4088 - 4 * maxNumOfSlot;
        unsigned short endOfLastSlot = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*lastSlotInserted] + pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*lastSlotInserted + 1];
        unsigned short data_section = endOfLastSlot + availableStorage;

        if (data_section != endOfData) {
            delete[] pageSlot;
            return RC_UPDATE_DATA_ERROR;
        }

        delete[] pageData;

        //save slot number and page number to RID
        rid.pageNum = pageNum;
        rid.slotNum = slotNum;

        return SUCCESS;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        // check for size of data and where the data is located
        unsigned short * pageSlot = new unsigned short[PAGE_SIZE/SHORTSIZE];
        fileHandle.readPage(rid.pageNum, pageSlot);
        unsigned short slotOffset = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*rid.slotNum];
        unsigned short dataSize = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*rid.slotNum + 1];
        delete[] pageSlot;

        if (dataSize == 0) {
            return RC_RFBM_READ_NONEXISTING_DATA;
        } else if (slotOffset >= 32768 && dataSize > 32768) {
            unsigned newPageNum = 65536 - slotOffset;
            unsigned newSlotNum = 65536 - dataSize;
            unsigned short * pageSlot = new unsigned short[PAGE_SIZE/SHORTSIZE];
            fileHandle.readPage(newPageNum, pageSlot);
            slotOffset = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*newSlotNum];
            dataSize = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*newSlotNum + 1];
            delete[] pageSlot;

            // copy data from the file
            unsigned char * page = new unsigned char[PAGE_SIZE];
            fileHandle.readPage(newPageNum,page);
            memcpy(data,page+slotOffset, dataSize);
            delete[] page;
            return SUCCESS;
        } else {
            // copy data from the file
            unsigned char * page = new unsigned char[PAGE_SIZE];
            fileHandle.readPage(rid.pageNum,page);
            memcpy(data,page+slotOffset, dataSize);
            delete[] page;
            return SUCCESS;
        }
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        // check for size of data and where the data is located
        unsigned short * pageSlot = new unsigned short[PAGE_SIZE/SHORTSIZE];
        fileHandle.readPage(rid.pageNum, pageSlot);
        unsigned short slotOffset = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*rid.slotNum];
        unsigned short dataSize = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*rid.slotNum + 1];
        if (slotOffset >= 32768 && dataSize > 32768) {
            unsigned newPageNum = 65536 - slotOffset;
            unsigned newSlotNum = 65536 - dataSize;
            pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*rid.slotNum] = 0;
            pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*rid.slotNum + 1] = 0;
            pageSlot[PAGE_SIZE/SHORTSIZE - 3] = rid.slotNum;
            fileHandle.writePage(rid.pageNum, pageSlot);

            RID newRid;
            newRid.pageNum = newPageNum;
            newRid.slotNum = newSlotNum;


            unsigned lastSlotInserted = pageSlot[PAGE_SIZE/SHORTSIZE - 4];
            unsigned maxNumOfSlot = pageSlot[PAGE_SIZE/SHORTSIZE - 2];
            unsigned availableStorage = pageSlot[PAGE_SIZE/SHORTSIZE - 1];
            unsigned short endOfData = 4088 - 4 * maxNumOfSlot;
            unsigned short endOfLastSlot = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*lastSlotInserted] + pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*lastSlotInserted + 1];
            unsigned short data_section = endOfLastSlot + availableStorage;

            if (data_section != endOfData) {
                delete[] pageSlot;
                return RC_UPDATE_DATA_ERROR;
            }


            delete[] pageSlot;
            return deleteRecord(fileHandle, recordDescriptor, newRid);
        } else {
            // check for size of data and where the data is located
            unsigned lastSlotInserted = pageSlot[PAGE_SIZE/SHORTSIZE - 4];
            unsigned maxNumOfSlot = pageSlot[PAGE_SIZE/SHORTSIZE - MAXNUMSLOT];
            unsigned availableStorage = pageSlot[PAGE_SIZE/SHORTSIZE - 1];
            unsigned short endOfData = 4088 - 4 * maxNumOfSlot;
            unsigned short endOfLastSlot = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*lastSlotInserted] + pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*lastSlotInserted + 1];
            unsigned short data_section = endOfLastSlot + availableStorage;
            if (data_section != endOfData) {
                delete[] pageSlot;
                return RC_UPDATE_DATA_ERROR;
            }

            RC problem = shiftSlot(pageSlot, maxNumOfSlot, slotOffset, dataSize);
            if (problem != SUCCESS) {
                delete[] pageSlot;
                return problem;
            }

            pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*rid.slotNum + 1] = 0;
            pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*rid.slotNum] = 0;
            pageSlot[PAGE_SIZE/SHORTSIZE - 1] = availableStorage + dataSize;
            if(pageSlot[PAGE_SIZE/SHORTSIZE - 3] == 0 ){
                pageSlot[PAGE_SIZE/SHORTSIZE - 3] = rid.slotNum;
            }

            problem = shiftData(pageSlot, slotOffset, dataSize, endOfData);
            if (problem != SUCCESS) {
                delete[] pageSlot;
                return problem;
            }

            lastSlotInserted = 1;
            for ( int i = 2 ; i < maxNumOfSlot + 1 ; i++) {
                unsigned lastSlotOffset = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*lastSlotInserted] + pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*lastSlotInserted + 1];
                unsigned newSlotOffset = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*i] + pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*i+1];
                if ( pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*lastSlotInserted] > 4088) {
                    lastSlotInserted = i;
                }
                if ( lastSlotOffset <= newSlotOffset && pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*i] < endOfLastSlot) {
                    lastSlotInserted = i;
                }
            }
            pageSlot[PAGE_SIZE/SHORTSIZE - 4] = lastSlotInserted;

            //save the page
            fileHandle.writePage(rid.pageNum, pageSlot);

            lastSlotInserted = pageSlot[PAGE_SIZE/SHORTSIZE - 4];
            maxNumOfSlot = pageSlot[PAGE_SIZE/SHORTSIZE - 2];
            availableStorage = pageSlot[PAGE_SIZE/SHORTSIZE - 1];
            endOfData = 4088 - 4 * maxNumOfSlot;
            endOfLastSlot = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*lastSlotInserted] + pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*lastSlotInserted + 1];
            data_section = endOfLastSlot + availableStorage;

            if (data_section != endOfData) {
                delete[] pageSlot;
                return RC_UPDATE_DATA_ERROR;
            }


            delete[] pageSlot;
            return SUCCESS;
        }
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        // calculate how many bytes data is using
        unsigned length = recordDescriptor.size();
        unsigned null_bytes = length / 8 + (length % 8 == 0 ? 0 : 1); // get number of bytes that indicate null

        unsigned char * null_indicator = new unsigned char[null_bytes];
        unsigned offset = 0;
        bool nullBit = false;
        memcpy(null_indicator, (char*)data+offset, null_bytes);
        offset += null_bytes;
        for(int i = 0 ; i < length ; i++ ){
            unsigned char mask = (unsigned)1 << (unsigned) (7 - i % 8);
            nullBit = null_indicator[i/8] & mask;
            out << recordDescriptor[i].name << ": ";
            if (!nullBit){
                switch (recordDescriptor[i].type) {
                    case AttrType::TypeInt:
                        out << *((unsigned *)((char*)data+offset));
                        offset += INTSIZE; break;
                    case AttrType::TypeReal:
                        out << *((float *)((char*)data+offset));
                        offset += FLOATSIZE; break;
                    case AttrType::TypeVarChar:
                        unsigned int number_of_char =  *( (unsigned int *) ((char*)data+offset));
                        offset += INTSIZE;
                        for( int k = 0 ; k < number_of_char ; k++ ){ out << (char) *((char*)data+offset+k);}
                        offset += number_of_char;
                }
            } else {
                out << "NULL";
            }
            if (i != length - 1) {
                out << ", ";
            }
        }
        delete[] null_indicator;
        return SUCCESS;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        unsigned short offset = getRecordLength(recordDescriptor, data);

        // check for size of data and where the data is located
        unsigned short * pageSlot = new unsigned short[PAGE_SIZE/SHORTSIZE];
        fileHandle.readPage(rid.pageNum, pageSlot);
        unsigned short slotOffset = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*rid.slotNum];
        unsigned short dataSize = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*rid.slotNum + 1];
        unsigned lastSlotInserted = pageSlot[PAGE_SIZE/SHORTSIZE - 4];
        unsigned maxNumOfSlot = pageSlot[PAGE_SIZE/SHORTSIZE - 2];
        unsigned availableStorage = pageSlot[PAGE_SIZE/SHORTSIZE - 1];
        unsigned short endOfData = 4088 - 4 * maxNumOfSlot;
        unsigned short endOfLastSlot = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*lastSlotInserted] + pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*lastSlotInserted + 1];
        unsigned short data_section = endOfLastSlot + availableStorage;
        bool inPage = true;

        if (data_section != endOfData) {
            delete[] pageSlot;
            return RC_UPDATE_DATA_ERROR;
        }

        if (slotOffset >= 32768 && dataSize > 32768) {
            unsigned newPageNum = 65536 - slotOffset;
            unsigned newSlotNum = 65536 - dataSize;
            pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*rid.slotNum] = 0;
            pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*rid.slotNum + 1] = 0;
            pageSlot[PAGE_SIZE/SHORTSIZE - 3] = rid.slotNum;
            fileHandle.writePage(rid.pageNum, pageSlot);

            dataSize = 0;
            inPage = false;
            RID newRid;
            newRid.pageNum = newPageNum;
            newRid.slotNum = newSlotNum;
            deleteRecord(fileHandle, recordDescriptor, newRid);
        }
        if (offset <= dataSize) {
            // if the data updating is smaller than saved data
            // update the slot directory
            pageSlot[PAGE_SIZE / SHORTSIZE - 4 - 2 * rid.slotNum + 1] = offset;
            pageSlot[PAGE_SIZE / SHORTSIZE - 1] += dataSize;
            pageSlot[PAGE_SIZE / SHORTSIZE - 1] -= offset;

            RC problem = SUCCESS;
            if (inPage) {
                problem = shiftSlot(pageSlot, maxNumOfSlot, slotOffset, dataSize - offset);
            }
            if (problem != SUCCESS) {
                delete[] pageSlot;
                return problem;
            }

            // save the data from the offset
            unsigned short index = slotOffset;
            writeAtFreeSpace(pageSlot, data, index, offset);
            index += offset;
            //shift Everything to the current.

            if (inPage) {
                problem = shiftData(pageSlot, index, dataSize - offset, endOfData);
            }
            if (problem != SUCCESS) {
                delete[] pageSlot;
                return problem;
            }

            fileHandle.writePage(rid.pageNum, pageSlot);
            delete[] pageSlot;
        } else if (offset <= availableStorage + dataSize) {
            delete[] pageSlot;
            deleteRecord(fileHandle, recordDescriptor, rid);

            pageSlot = new unsigned short[PAGE_SIZE/SHORTSIZE];
            fileHandle.readPage(rid.pageNum, pageSlot);
            lastSlotInserted = pageSlot[PAGE_SIZE/SHORTSIZE - 4];
            pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2 * rid.slotNum] = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2 * lastSlotInserted] + pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2 * lastSlotInserted + 1];
            pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2 * rid.slotNum + 1] = offset;

            for(int i = 0; i < pageSlot[PAGE_SIZE/SHORTSIZE - MAXNUMSLOT]; i++) {
                if(pageSlot[PAGE_SIZE/SHORTSIZE - 6 - 2*i + 1] == 0) {
                    pageSlot[PAGE_SIZE/SHORTSIZE - SLOTTOUSE] = i + 1;
                    break;
                } else {
                    pageSlot[PAGE_SIZE/SHORTSIZE - SLOTTOUSE] = 0;
                }
            }

            pageSlot[PAGE_SIZE/SHORTSIZE - LASTSLOTEDITED] = rid.slotNum;
            pageSlot[PAGE_SIZE/SHORTSIZE - FREESPACE] -= offset;
            writeAtFreeSpace(pageSlot, data, pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2 * rid.slotNum], offset);
            fileHandle.writePage(rid.pageNum, pageSlot);
            delete[] pageSlot;
        } else {
            // find the page that have free space
            // Change slot directory
            delete[] pageSlot;
            RC problem = deleteRecord(fileHandle, recordDescriptor, rid);
            if (problem != SUCCESS) {
                return problem;
            }

            RID newrid;
            insertRecord(fileHandle, recordDescriptor, data, newrid);

            pageSlot = new unsigned short[PAGE_SIZE/SHORTSIZE];
            fileHandle.readPage(rid.pageNum, pageSlot);

            pageSlot[PAGE_SIZE / SHORTSIZE - 4 - 2 * rid.slotNum] = 65535 - newrid.pageNum + 1;
            pageSlot[PAGE_SIZE / SHORTSIZE - 4 - 2 * rid.slotNum + 1] = 65535 - newrid.slotNum + 1;

            for(int i = 0; i < pageSlot[PAGE_SIZE/SHORTSIZE - MAXNUMSLOT]; i++) {
                if(pageSlot[PAGE_SIZE/SHORTSIZE - 6 - 2*i + 1] == 0) {
                    pageSlot[PAGE_SIZE/SHORTSIZE - SLOTTOUSE] = i + 1;
                    break;
                } else {
                    pageSlot[PAGE_SIZE/SHORTSIZE - SLOTTOUSE] = 0;
                }
            }

            fileHandle.writePage(rid.pageNum, pageSlot);
            delete[] pageSlot;
        }

        pageSlot = new unsigned short[PAGE_SIZE/SHORTSIZE];
        fileHandle.readPage(rid.pageNum, pageSlot);
        slotOffset = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*rid.slotNum];
        dataSize = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*rid.slotNum + 1];
        lastSlotInserted = pageSlot[PAGE_SIZE/SHORTSIZE - 4];
        maxNumOfSlot = pageSlot[PAGE_SIZE/SHORTSIZE - 2];
        availableStorage = pageSlot[PAGE_SIZE/SHORTSIZE - 1];
        endOfData = 4088 - 4 * maxNumOfSlot;
        endOfLastSlot = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*lastSlotInserted] + pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*lastSlotInserted + 1];
        data_section = endOfLastSlot + availableStorage;

        if (data_section != endOfData) {
            delete[] pageSlot;
            return RC_UPDATE_DATA_ERROR;
        }

        delete[] pageSlot;
        return SUCCESS;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {

        // calculate how many bytes data is using
        unsigned int length = recordDescriptor.size();
        unsigned int null_bytes = length / 8 + (length % 8 == 0 ? 0 : 1); // get number of bytes that indicate null

        unsigned int maxSize = null_bytes;
        for(int i = 0; i < length; i++) { recordDescriptor[i].type == AttrType::TypeVarChar ? maxSize += recordDescriptor[i].length + 4 : maxSize += recordDescriptor[i].length; }
        unsigned char * record = new unsigned char[maxSize];
        readRecord(fileHandle, recordDescriptor, rid, record);


        unsigned char * null_indicator = new unsigned char[null_bytes];
        unsigned offset = 0;
        bool nullBit = false;
        memcpy(null_indicator, (char*)record+offset, null_bytes);
        offset += null_bytes;
        for(int i = 0 ; i < length ; i++ ){
            unsigned int number_of_char = 0;
            unsigned char * readAttr = (unsigned char*) data;
            unsigned char mask = (unsigned)1 << (unsigned) (7 - i % 8);

            nullBit = null_indicator[i/8] & mask;
            if (!nullBit){
                switch (recordDescriptor[i].type) {
                    case AttrType::TypeInt:
                        if (attributeName == recordDescriptor[i].name) {
                            readAttr[0] = 0;
                            memcpy(readAttr + 1, record + offset, INTSIZE);
                        }
                        offset += INTSIZE; break;
                    case AttrType::TypeReal:
                        if (attributeName == recordDescriptor[i].name) {
                            readAttr[0] = 0;
                            memcpy(readAttr + 1, record + offset, FLOATSIZE);
                        }
                        offset += FLOATSIZE; break;
                    case AttrType::TypeVarChar:
                        unsigned int number_of_char =  *((unsigned int * )(record+offset));
                        if (attributeName == recordDescriptor[i].name) {
                            readAttr[0] = 0;
                            memcpy(readAttr + 1, record + offset, number_of_char + INTSIZE);
                        }
                        offset += INTSIZE;
                        offset += number_of_char;
                }
            } else if (nullBit && attributeName == recordDescriptor[i].name) {
                readAttr[0] = 128;
            }
        }
        delete[] record;
        delete[] null_indicator;
        return SUCCESS;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {

        rbfm_ScanIterator.open(fileHandle, recordDescriptor, attributeNames, conditionAttribute, compOp, value);
        return SUCCESS;
    }

    unsigned RecordBasedFileManager::getRecordLength(const std::vector<Attribute> &recordDescriptor, const void *data) {
        // calculate how many bytes data is using
        unsigned length = recordDescriptor.size();
        unsigned null_bytes = length / 8 + (length % 8 == 0 ? 0 : 1); // get number of bytes that indicate null

        // ceil (length of vector / 8 ) go through the data with the null value
        unsigned char * null_indicator = new unsigned char[null_bytes];
        unsigned offset = 0;
        unsigned char * temp = (unsigned char*) data;
        bool nullBit = false;
        memcpy(null_indicator, temp+offset, null_bytes);
        offset += null_bytes;
        for(int i = 0 ; i < length ; i++ ){
            unsigned char mask = (unsigned)1 << (unsigned) (7 - i % 8);
            nullBit = null_indicator[i/8] & mask;
            if (!nullBit){
                switch (recordDescriptor[i].type) {
                    case AttrType::TypeInt:
                        offset += INTSIZE; break;
                    case AttrType::TypeReal:
                        offset += FLOATSIZE; break;
                    case AttrType::TypeVarChar:
                        unsigned int number_of_char =  *((unsigned int * )(temp+offset));
                        offset += INTSIZE;
                        offset += number_of_char;
                }
            }
        }
        delete[] null_indicator;
        return offset;
    }

    unsigned RecordBasedFileManager::findEmptyPage(FileHandle &fileHandle, unsigned int offset, unsigned number_of_pages) {
        // find the page that have free space
        unsigned pageNum;
        for(pageNum = 0; pageNum < number_of_pages; pageNum++) {
            unsigned short* page = new unsigned short[PAGE_SIZE/SHORTSIZE];
            fileHandle.readPage(pageNum, page);
            if (page[PAGE_SIZE/SHORTSIZE - FREESPACE] >= (offset + 4)){ // added 8 bytes for slot offset and length
                unsigned availableBytes = page[PAGE_SIZE/SHORTSIZE - FREESPACE];
                delete[] page;
                break;
            } else {
                delete[] page;
            }
        }
        if (number_of_pages == pageNum) {
            // if there is no page in the file, create one;
            // or if there is no available space in current pages, append new page;
            unsigned short* page = new unsigned short[PAGE_SIZE/SHORTSIZE];
            for(int i = 0 ; i < PAGE_SIZE/SHORTSIZE - 2; i++){ page[i] = -1; } // set every data with save -1
            page[PAGE_SIZE/SHORTSIZE - 1] = PAGE_SIZE - 4 * SHORTSIZE; // set last 2 byte as bytes of free space
            page[PAGE_SIZE/SHORTSIZE - 2] = 0; // set the 2 bytes before free space as number of slots
            page[PAGE_SIZE/SHORTSIZE - 3] = 0; // if the edit was deleted, place the empty slot.
            page[PAGE_SIZE/SHORTSIZE - 4] = 0; // the end of the slot. That indicate the start point of edit.
            fileHandle.appendPage(page);
            delete[] page;
        }
        return pageNum;
    }

    RC RecordBasedFileManager::editSlotDirectory(void *page, unsigned short &slotNum, unsigned short &slotOffSet, unsigned short &offset) {
        unsigned short * pageSlot = (unsigned short *) page;
        bool add4byte = false;
        slotOffSet = 0;
        unsigned lastSlotInserted = pageSlot[PAGE_SIZE/SHORTSIZE - LASTSLOTEDITED];

        if (lastSlotInserted != 0) {
            slotOffSet = pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*lastSlotInserted] + pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*lastSlotInserted + 1];
        }

        // add slot identifier to the file modify available free space
        if ( pageSlot[PAGE_SIZE/SHORTSIZE - MAXNUMSLOT] == 0 ) {
            slotNum = 1;
            add4byte = true;
        } else if(pageSlot[PAGE_SIZE/SHORTSIZE - SLOTTOUSE] == 0) {
            slotNum = pageSlot[PAGE_SIZE/SHORTSIZE - MAXNUMSLOT] + 1;
            add4byte = true;
        } else {
            slotNum = pageSlot[PAGE_SIZE/SHORTSIZE - SLOTTOUSE];

            for(int i = 0; i < pageSlot[PAGE_SIZE/SHORTSIZE - MAXNUMSLOT]; i++) {
                if(pageSlot[PAGE_SIZE/SHORTSIZE - 6 - 2*i + 1] == 0) {
                    pageSlot[PAGE_SIZE/SHORTSIZE - SLOTTOUSE] = i + 1;
                    break;
                } else {
                    pageSlot[PAGE_SIZE/SHORTSIZE - SLOTTOUSE] = 0;
                }
            }
        }
        pageSlot[PAGE_SIZE/SHORTSIZE - LASTSLOTEDITED] = slotNum;
        pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*slotNum] = slotOffSet;
        pageSlot[PAGE_SIZE/SHORTSIZE - 4 - 2*slotNum + 1] = offset;
        if(add4byte) {
            pageSlot[PAGE_SIZE/SHORTSIZE - MAXNUMSLOT] += 1;
            pageSlot[PAGE_SIZE/SHORTSIZE - FREESPACE] -= offset + 4;
        } else {
            pageSlot[PAGE_SIZE/SHORTSIZE - FREESPACE] -= offset;
        }
    }

    RC RecordBasedFileManager::writeAtFreeSpace(void *page, const void * data, unsigned short slotOffSet, unsigned short &offset) {
        unsigned char * pageData = (unsigned char *) page;
        memcpy(pageData+slotOffSet, data, offset);
    }

    RC RecordBasedFileManager::shiftData(void *page, unsigned short &slotOffSet, unsigned short dataSize, unsigned short endOfData) {
        if (slotOffSet >= 4088 || dataSize >= 4088 || endOfData >= 4088 || endOfData < dataSize) {
            return RC_PAGE_SHIFT_ERROR;
        }
        unsigned char * pageData = (unsigned char * ) page;
        //memmove(pageData + slotOffSet, pageData + slotOffSet + dataSize, endOfData - slotOffSet);
        for(unsigned int i = slotOffSet; i < endOfData - dataSize; i++){
            pageData[i] = pageData[i+dataSize];
        }
        for(unsigned int i = endOfData - dataSize; i < endOfData; i++) {
            pageData[i] = -1;
        }
        return SUCCESS;
    }

    RC RecordBasedFileManager::shiftSlot(void *page, unsigned short maxNumOfSlot, unsigned short slotOffset, unsigned short dataSize) {
        if (slotOffset >= 4088 || dataSize >= 4088 || maxNumOfSlot > 2044) {
            return RC_PAGE_SHIFT_ERROR;
        }
        unsigned short * pageSlot = (unsigned short *) page;
        for (int i = 0 ; i < maxNumOfSlot; i++){
            if(pageSlot[PAGE_SIZE/SHORTSIZE - 6 - 2*i] > slotOffset && pageSlot[PAGE_SIZE/SHORTSIZE - 6 - 2*i + 1] < 4088){
                pageSlot[PAGE_SIZE/SHORTSIZE - 6 - 2*i] -= dataSize;
            }
        }
        return SUCCESS;
    }

    RC RBFM_ScanIterator::open(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const std::vector<std::string> &attributeName, const std::string &conditionAttribute, const CompOp compOp, const void *value) {
        storedFileHandle.savedFileName = fileHandle.savedFileName;
        storedFileHandle.numberOfPages = fileHandle.numberOfPages;
        storedFileHandle.readPageCounter = fileHandle.readPageCounter;
        storedFileHandle.writePageCounter = fileHandle.writePageCounter;
        storedFileHandle.appendPageCounter = fileHandle.appendPageCounter;
        storedFileHandle.filePointer = fileHandle.filePointer;
        pageData = new char[PAGE_SIZE];
        storedFileHandle.readPage(0,pageData);
        currentPageNum = 0;
        currentSlotNum = 0;
        maxSlotNum = ((unsigned short *) pageData)[PAGE_SIZE/SHORTSIZE - MAXNUMSLOT];
        maxPageNum = fileHandle.getNumberOfPages();
        AttributeDescriptor = &recordDescriptor;
        scanAttr = &attributeName;
        conditionAttributeName = &conditionAttribute;
        comparisonOperation = compOp;
        comparisonValue = value;
    }

    RC RBFM_ScanIterator::close() {
        if (pageData != NULL) {
            delete[] (char *) pageData;
        }
        pageData = NULL;
        //storedFileHandle.closeFile();
        return SUCCESS;
    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
        unsigned short * slotDirectory = (unsigned short *)pageData;
        unsigned short slotOffSet = 0;
        unsigned short dataSize = 0;
        bool passTest = false;
        int result = SUCCESS;
        // check data pass comparison test if not run until it pass the test;
        while(!passTest) {
            result = updatePageSlotNum();
            slotOffSet = slotDirectory[PAGE_SIZE/SHORTSIZE - 4 - 2*currentSlotNum];
            dataSize = slotDirectory[PAGE_SIZE/SHORTSIZE - 4 - 2*currentSlotNum + 1];

            while(!correctSlotOffSetDataSize(slotOffSet, dataSize)) {
                result = updatePageSlotNum();
                slotOffSet = slotDirectory[PAGE_SIZE/SHORTSIZE - 4 - 2*currentSlotNum];
                dataSize = slotDirectory[PAGE_SIZE/SHORTSIZE - 4 - 2*currentSlotNum + 1];
                if (result == RBFM_EOF) break;
            }

            if (result == RBFM_EOF) {
                return result;
            } else {
                passTest = checkPassTest(slotOffSet, dataSize);
            }
        }

        char* record = new char[dataSize];
        memcpy(record, (char*)pageData + slotOffSet, dataSize);

        selectAttribute(record, data);

        delete[] record;
        rid.pageNum = currentPageNum;
        rid.slotNum = currentSlotNum;
    };

    bool RBFM_ScanIterator::correctSlotOffSetDataSize(unsigned short slotOffSet, unsigned short dataSize) {
        if (slotOffSet >= 0 && slotOffSet <= 4088 && dataSize <= 4088 && dataSize > 0) {
            return true;
        } else {
            return false;
        }
    }

    RC RBFM_ScanIterator::updatePageSlotNum() {
        unsigned short * slotDirectory = (unsigned short *)pageData;
        if((currentSlotNum == maxSlotNum && currentPageNum + 1 == maxPageNum) || maxPageNum == 0) {
            return RBFM_EOF;
        } else if (currentSlotNum > maxSlotNum) {
            currentSlotNum = 1;
            currentPageNum += 1;
            storedFileHandle.readPage(currentPageNum, pageData);
            maxSlotNum = slotDirectory[PAGE_SIZE/SHORTSIZE - MAXNUMSLOT];
        } else {
            currentSlotNum += 1;
        }
        return SUCCESS;
    }

    bool RBFM_ScanIterator::checkPassTest(unsigned short &slotOffSet, unsigned short dataSize) {
        char * record = new char[dataSize];
        memcpy(record, (char*)pageData + slotOffSet, dataSize);

        bool result = compareAttribute(record, dataSize);
        delete[] record;
        return result;
    }

    RC RBFM_ScanIterator::compareAttribute(void * record, unsigned short dataSize) {
        unsigned short numOfAttribute = AttributeDescriptor->size();
        unsigned short nullBytes = numOfAttribute / 8 + (numOfAttribute % 8 == 0 ? 0 : 1);

        unsigned char * null_indicator = new unsigned char[nullBytes];
        unsigned offset = 0;
        unsigned char * temp = (unsigned char*) record;
        bool nullBit = false;
        bool result = false;
        memcpy(null_indicator, temp+offset, nullBytes);
        offset += nullBytes;
        for(int i = 0 ; i < numOfAttribute ; i++ ){
            unsigned char mask = (unsigned)1 << (unsigned) (7 - i % 8);
            nullBit = null_indicator[i/8] & mask;
            if (!nullBit){
                if (comparisonOperation == NO_OP || AttributeDescriptor->at(i).name == *conditionAttributeName) {
                    unsigned int number_of_char = 0;
                    switch (AttributeDescriptor->at(i).type) {
                        case AttrType::TypeInt: {
                            int integer = *((int *)((char*)record+offset));
                            result = compareInteger(integer);
                            break;
                        }
                        case AttrType::TypeReal: {
                            float realNumber = *((float *)((char*)record+offset));
                            result = compareFloat(realNumber);
                            break;
                        }
                        case AttrType::TypeVarChar: {
                            number_of_char = *((unsigned int * )((char*)record+offset));
                            char* string = new char[number_of_char];
                            memcpy(string, (char*) record + offset + INTSIZE, number_of_char);
                            result = compareString(string, number_of_char);
                            delete[] string;
                            break;
                        }
                    }
                    delete[] null_indicator;
                    return result;
                }
                switch (AttributeDescriptor->at(i).type) {
                    case AttrType::TypeInt: {
                        offset += INTSIZE; break;
                    }
                    case AttrType::TypeReal: {
                        offset += FLOATSIZE; break;
                    }
                    case AttrType::TypeVarChar: {
                        unsigned int number_of_char =  *((unsigned int * )((char*)record+offset));
                        offset += INTSIZE;
                        offset += number_of_char;
                    }
                }
            }
        }
        delete[] null_indicator;
        return result;
    }

    RC RBFM_ScanIterator::compareInteger(int integer){
        switch (comparisonOperation) {
            case CompOp::EQ_OP : {
                return integer == *((int *) comparisonValue);
            }
            case CompOp::GE_OP : {
                return integer >= *((int *) comparisonValue);
            }
            case CompOp::GT_OP : {
                return integer > *((int *) comparisonValue);
            }
            case CompOp::LE_OP : {
                return integer <= *((int *) comparisonValue);
            }
            case CompOp::LT_OP : {
                return integer < *((int *) comparisonValue);
            }
            case CompOp::NE_OP : {
                return integer != *((int *) comparisonValue);
            }
            case CompOp::NO_OP : {
                return true;
            }
        }
    }

    RC RBFM_ScanIterator::compareFloat(float realNumber) {
        switch (comparisonOperation) {
            case CompOp::EQ_OP : {
                return realNumber == *((float *) comparisonValue);
            }
            case CompOp::GE_OP : {
                return realNumber >= *((float *) comparisonValue);
            }
            case CompOp::GT_OP : {
                return realNumber > *((float *) comparisonValue);
            }
            case CompOp::LE_OP : {
                return realNumber <= *((float *) comparisonValue);
            }
            case CompOp::LT_OP : {
                return realNumber < *((float *) comparisonValue);
            }
            case CompOp::NE_OP : {
                return realNumber != *((float *) comparisonValue);
            }
            case CompOp::NO_OP : {
                return true;
            }
        }
    }

    RC RBFM_ScanIterator::compareString(char *string, unsigned number_of_char) {
        int max = 0;
        if (comparisonOperation != CompOp::NO_OP) {
            if (number_of_char > *((int *) comparisonValue)) {
                max = number_of_char;
            } else {
                max = *((int *) comparisonValue);
            }
            switch (comparisonOperation) {
                case CompOp::EQ_OP : {
                    bool result = memcmp(string, (char *) comparisonValue + INTSIZE, max) == 0;
                    return result;
                }
                case CompOp::GE_OP : {
                    bool result = memcmp(string, (char *) comparisonValue + INTSIZE, max) >= 0;
                    return result;
                }
                case CompOp::GT_OP : {
                    bool result = memcmp(string, (char *) comparisonValue + INTSIZE, max) > 0;
                    return result;
                }
                case CompOp::LE_OP : {
                    bool result = memcmp(string, (char *) comparisonValue + INTSIZE, max) <= 0;
                    return result;
                }
                case CompOp::LT_OP : {
                    bool result = memcmp(string, (char *) comparisonValue + INTSIZE, max) < 0;
                    return result;
                }
                case CompOp::NE_OP : {
                    bool result = memcmp(string, (char *) comparisonValue + INTSIZE, max) != 0;
                    return result;
                }
            }
        } else {
            return true;
        }
    }

    RC RBFM_ScanIterator::selectAttribute(void *record, void *data) {
        unsigned short dataAttributeLength = scanAttr->size();
        unsigned short dataNullBytes = dataAttributeLength / 8 + (dataAttributeLength % 8 == 0 ? 0 : 1);

        unsigned short recordAttrubuteLength = AttributeDescriptor->size();
        unsigned short recordNullBytes = recordAttrubuteLength / 8 + (recordAttrubuteLength % 8 == 0 ? 0 : 1);
        unsigned char * recordNullIndicator = new unsigned char[recordNullBytes];
        unsigned char * dataNullIndicator = new unsigned char[dataNullBytes];

        for(int i = 0; i < dataNullBytes; i++) {dataNullIndicator[i] = 0;}

        unsigned recordOffSet = 0;
        unsigned dataOffSet = 0;
        unsigned short scanAttrIndex = 0;
        bool nullBit = false;

        memcpy(recordNullIndicator, (char*)record+recordOffSet, recordNullBytes);

        dataOffSet += dataNullBytes;
        for( ; scanAttrIndex < dataAttributeLength ; scanAttrIndex++) {
            recordOffSet = 0;
            nullBit = false;
            recordOffSet += recordNullBytes;
            for (int i = 0; i < recordAttrubuteLength; i++) {
                unsigned char mask = (unsigned) 1 << (unsigned) (7 - i % 8);
                nullBit = recordNullIndicator[i / 8] & mask;

                if (scanAttr->at(scanAttrIndex) == AttributeDescriptor->at(i).name) {
                    unsigned char *saveAttribute = (unsigned char *) data;
                    if (nullBit) {
                        unsigned char newMask = (unsigned) 1 << (unsigned) (7 - scanAttrIndex % 8);
                        unsigned nullplace = scanAttrIndex / 8;
                        dataNullIndicator[nullplace] += newMask;
                    } else {
                        switch (AttributeDescriptor->at(i).type) {
                            case AttrType::TypeInt: {
                                memcpy((char *) data + dataOffSet, (char *) record + recordOffSet, INTSIZE);
                                dataOffSet += INTSIZE;
                                break;
                            }
                            case AttrType::TypeReal: {
                                memcpy((char *) data + dataOffSet, (char *) record + recordOffSet, FLOATSIZE);
                                dataOffSet += FLOATSIZE;
                                break;
                            }
                            case AttrType::TypeVarChar: {
                                unsigned int number_of_char = *((unsigned int *) ((char *) record + recordOffSet));
                                memcpy((char *) data + dataOffSet, (char *) record + recordOffSet,
                                       number_of_char + INTSIZE);
                                dataOffSet += INTSIZE;
                                dataOffSet += number_of_char;
                            }
                        }
                    }
                    break;
                }

                if (!nullBit) {
                    switch (AttributeDescriptor->at(i).type) {
                        case AttrType::TypeInt: {
                            recordOffSet += INTSIZE;
                            break;
                        }
                        case AttrType::TypeReal: {
                            recordOffSet += FLOATSIZE;
                            break;
                        }
                        case AttrType::TypeVarChar: {
                            unsigned int number_of_char = *((unsigned int *) ((char *) record + recordOffSet));
                            recordOffSet += INTSIZE;
                            recordOffSet += number_of_char;
                        }
                    }
                }
            }
        }

        memcpy(data, dataNullIndicator, dataNullBytes);
        delete[] recordNullIndicator;
        delete[] dataNullIndicator;
    }

} // namespace PeterDB

