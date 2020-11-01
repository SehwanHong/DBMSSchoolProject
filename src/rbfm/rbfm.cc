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
            nullBit = null_indicator[i/8] & ((unsigned)1 << (unsigned) 7 - i % 8);
            if (!nullBit){
                if (recordDescriptor[i].type == AttrType::TypeInt) {
                    offset += sizeof(int);
                } else if (recordDescriptor[i].type == AttrType::TypeReal) {
                    offset += sizeof(float);
                } else if (recordDescriptor[i].type == AttrType::TypeVarChar) {
                    unsigned int number_of_char =  *((unsigned int * )(temp+offset));
                    offset += sizeof(int);
                    offset += number_of_char;
                }
            }
        }
        delete[] null_indicator;

        // find the page that have free space
        unsigned pageNum;
        for(pageNum = 0; pageNum < number_of_pages; pageNum++) {
            unsigned short* page = new unsigned short[PAGE_SIZE/sizeof(unsigned short)];
            for(int i = 0 ; i < PAGE_SIZE/sizeof(unsigned short) - 2; i++){ page[i] = 0; } // set every data with 0
            fileHandle.readPage(pageNum, page);
            if (page[PAGE_SIZE/sizeof(unsigned short)-1] >= offset + 4){ // added 4 bytes for slot offset and length
                delete[] page;
                break;
            } else {
                delete[] page;
            }
        }
        if (number_of_pages == pageNum) {
            // if there is no page in the file, create one;
            // or if there is no available space in current pages, append new page;
            unsigned short* page = new unsigned short[PAGE_SIZE/sizeof(unsigned short)];
            for(int i = 0 ; i < PAGE_SIZE/sizeof(unsigned short) - 2; i++){ page[i] = -1; } // set every data with save -1
            page[PAGE_SIZE/sizeof(unsigned short)-1] = PAGE_SIZE - 2 * sizeof(unsigned short); // set last 2 byte as bytes of free space
            page[PAGE_SIZE/sizeof(unsigned short)-2] = 0; // set the 2 bytes before free space as number of slots
            fileHandle.appendPage(page);
            delete[] page;
        }

        // insert the record in the page
        unsigned short* pageSlot = new unsigned short[PAGE_SIZE/sizeof(unsigned short)];
        fileHandle.readPage(pageNum, pageSlot); // read page from the file

        // check which slot number is the data stored
        unsigned short slotNum = pageSlot[PAGE_SIZE/sizeof(unsigned short) - 2];
        unsigned slotOffset = 0;
        int i;
        for(i = 0 ; i < slotNum ; i++) {
            slotOffset += pageSlot[PAGE_SIZE/sizeof(unsigned short) - 4 - 2*i +1];
        }
        slotNum++;
        // add slot identifier to the file modify available free space
        pageSlot[PAGE_SIZE/sizeof(unsigned short) - 2] = slotNum;
        pageSlot[PAGE_SIZE/sizeof(unsigned short) - 2 - 2*slotNum] = slotOffset;
        pageSlot[PAGE_SIZE/sizeof(unsigned short) - 2 - 2*slotNum + 1] = offset;
        pageSlot[PAGE_SIZE/sizeof(unsigned short) - 1] -= offset + 4;
        fileHandle.writePage(pageNum, pageSlot);
        delete[] pageSlot;

        // edit the page
        unsigned char * page = new unsigned char[PAGE_SIZE];
        fileHandle.readPage(pageNum, page);
        memcpy(page+slotOffset, data, offset);
        // save stored page number and the slot number
        fileHandle.writePage(pageNum, page); // save the page to the file
        delete[] page;

        //save slot number and page number to RID
        rid.pageNum = pageNum;
        rid.slotNum = slotNum;

        return SUCCESS;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        // check for size of data and where the data is located
        unsigned short * pageSlot = new unsigned short[PAGE_SIZE/sizeof(unsigned short)];
        fileHandle.readPage(rid.pageNum, pageSlot);
        unsigned slotOffset = pageSlot[PAGE_SIZE/sizeof(unsigned short) - 2 - 2*rid.slotNum];
        unsigned dataSize = pageSlot[PAGE_SIZE/sizeof(unsigned short) - 2 - 2*rid.slotNum + 1];
        delete[] pageSlot;

        // copy data from the file
        unsigned char * page = new unsigned char[PAGE_SIZE];
        fileHandle.readPage(rid.pageNum,page);
        memcpy(data,page+slotOffset, dataSize);
        delete[] page;
        return SUCCESS;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
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
                if (recordDescriptor[i].type == AttrType::TypeInt) {
                    out << *((unsigned *)((char*)data+offset));
                    offset += sizeof(int);
                } else if (recordDescriptor[i].type == AttrType::TypeReal) {
                    out << *((float *)((char*)data+offset));
                    offset += sizeof(float);
                } else if (recordDescriptor[i].type == AttrType::TypeVarChar) {
                    unsigned number_of_char = (unsigned) *((char*)data+offset);
                    offset += sizeof(int);
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
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

} // namespace PeterDB

