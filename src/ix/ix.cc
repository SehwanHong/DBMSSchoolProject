#include "src/include/ix.h"

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    RC IndexManager::createFile(const std::string &fileName) {
        PagedFileManager &pfm = PagedFileManager::instance();
        return pfm.createFile(fileName);
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        PagedFileManager &pfm = PagedFileManager::instance();
        return pfm.destroyFile(fileName);
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        if (access(fileName.c_str(), F_OK) == 0) { //check if file name exists
            return ixFileHandle.openFile(fileName);
            // save file name to the fileHandl
            // so fileHandle could open file by itself
        } else {
            return RC_FILE_NAME_NOT_EXIST;
        }
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        return ixFileHandle.closeFile();
    }

    RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute,
                                 const void *key, const RID &rid) {
        unsigned numberOfPage = ixFileHandle.getNumberOfPages();
        char * rootNode = new char[PAGE_SIZE];
        unsigned rootPage = 0;
        if (numberOfPage == 0) {
            rootPage = 1;
            ixFileHandle.setRootNode(rootPage);
            ixFileHandle.configureHeader(rootNode);
            ixFileHandle.appendPage(rootNode);
            createLeafNode(rootNode);
            ixFileHandle.appendPage(rootNode);
        } else {
            ixFileHandle.readPage(0, rootNode);
            rootPage = ((int*)rootNode)[4];
        }
        ixFileHandle.readPage(rootPage, rootNode);
        bool leafNode = ((unsigned short *) rootNode)[PAGE_SIZE/SHORTSIZE - LEAFNODE] == 0;
        unsigned short numberOfRecord = ((unsigned short *) rootNode)[PAGE_SIZE/SHORTSIZE - NUMRECORD];

        if (numberOfRecord == 0 && leafNode){
            unsigned lenOfKey = getLengthOfKey(attribute, key);
            unsigned totalSize = INTSIZE+lenOfKey+INTSIZE+SHORTSIZE;
            char * modifiedKey = new char[totalSize];
            getModifiedKey(key,modifiedKey,rid,lenOfKey);
            insertFirstEntry(rootNode, modifiedKey, totalSize, leafNode);
            ixFileHandle.writePage(1, rootNode);
            delete[] rootNode;
            delete[] modifiedKey;
            return SUCCESS;
        } else {
            std::vector<int> pageRead;
            std::vector<int> slotUsed;
            int numberOfSelectedPages = 0;
            int height = ixFileHandle.getHeight();
            char ** selectedPages = new char* [height];
            getLeafNode(ixFileHandle, attribute, pageRead, slotUsed, rootNode,
                        key, rid, selectedPages, numberOfSelectedPages, rootPage);
            if (height != numberOfSelectedPages) {
                return RC_UPDATE_DATA_ERROR;
            }
            insertLeafEntry(selectedPages, ixFileHandle, attribute, key, rid,
                            pageRead, slotUsed, height);
            for(int i = 0 ; i < numberOfSelectedPages ; i++) {
                delete[] selectedPages[i];
            }
            delete[] selectedPages;
            return SUCCESS;
        }
    }

    RC IndexManager::insertFirstEntry(void *data, void * modifiedKey, unsigned & totalSize, bool LeafNode) {
        char * rootNode = (char*) data;
        memcpy(rootNode, modifiedKey, totalSize);
        unsigned next = 0;
        memcpy(rootNode + totalSize, &next, INTSIZE);
        unsigned short * directory = (unsigned short *) rootNode;
        directory[PAGE_SIZE/SHORTSIZE - FREESPACE] -= (totalSize+12);
        directory[PAGE_SIZE/SHORTSIZE - NUMRECORD] += 1;
        if(LeafNode) {
            directory[PAGE_SIZE/SHORTSIZE - LEAFNODE] = 0;
        } else {
            directory[PAGE_SIZE/SHORTSIZE - LEAFNODE] = -1;
        }
        directory[PAGE_SIZE/SHORTSIZE - 3 - 2 * 1] = 0;
        directory[PAGE_SIZE/SHORTSIZE - 3 - 2 * 1 + 1] = totalSize;
        directory[PAGE_SIZE/SHORTSIZE - 3 - 2 * 2] = totalSize;
        directory[PAGE_SIZE/SHORTSIZE - 3 - 2 * 2 + 1] = -1;
    }

    RC IndexManager::insertLeafEntry(char** selectedPages, IXFileHandle &ixFileHandle,
                                     const Attribute &attribute, const void * key, const RID &rid,
                                     std::vector<int> &pageRead, std::vector<int> &slotUsed,
                                     int &numberOfSelectedPages) {
        bool breakNode = false;
        unsigned lenOfKey = getLengthOfKey(attribute, key);
        unsigned totalSize = INTSIZE+lenOfKey+INTSIZE+SHORTSIZE;
        char * modifiedKey = new char[totalSize];
        getModifiedKey(key,modifiedKey,rid,lenOfKey);
        char * goingUp = NULL;
        int goingSize;
        unsigned newLeafPageNum = ixFileHandle.getNumberOfPages();

        for(int i = numberOfSelectedPages - 1; i > -1; i--) {

            char * rootNode = selectedPages[i];
            unsigned offset = 0;
            unsigned short * directory = (unsigned short*) rootNode;
            unsigned freeSpace = directory[PAGE_SIZE/SHORTSIZE - FREESPACE];
            unsigned numberOfRecord = directory[PAGE_SIZE/SHORTSIZE - NUMRECORD];

            offset = directory[PAGE_SIZE/SHORTSIZE - 5 - 2 * slotUsed[i]];
            bool leafNode = directory[PAGE_SIZE/SHORTSIZE - LEAFNODE] == 0;

            if ( leafNode && freeSpace < totalSize + 4) {
                char * newLeafPage = new char[PAGE_SIZE];
                createLeafNode(newLeafPage);
                //break and connect the leaf nodes.
                unsigned currentSlotInput = slotUsed[i];
                if (newLeafPageNum == 228) {
                    int error = 0;
                }
                splitHalf(selectedPages[i], newLeafPage, pageRead[i], newLeafPageNum);
                unsigned short * newDirectory = (unsigned short*) newLeafPage;
                if (currentSlotInput < numberOfRecord/2) {
                    selectedPages[i];
                    shift(selectedPages[i], offset, currentSlotInput, modifiedKey, totalSize);
                } else {
                    currentSlotInput = currentSlotInput - numberOfRecord/2;
                    offset = newDirectory[PAGE_SIZE/SHORTSIZE - 5 - 2 * currentSlotInput];
                    shift(newLeafPage, offset, currentSlotInput, modifiedKey, totalSize);
                }
                newDirectory[PAGE_SIZE/SHORTSIZE - LEAFNODE] = 0;
                ixFileHandle.writePage(pageRead[i],selectedPages[i]);
                ixFileHandle.appendPage(newLeafPage);
                if (i == 0) {
                    unsigned newRootPage = ixFileHandle.getNumberOfPages();
                    ixFileHandle.setRootNode(newRootPage);
                    ixFileHandle.saveHeader();
                    char * newRootNode = new char[PAGE_SIZE];
                    createRootNode(newRootNode);
                    unsigned short * newDirectory = (unsigned short*) newLeafPage;
                    unsigned firstEntryLength = newDirectory[PAGE_SIZE/SHORTSIZE - 4];
                    char * firstEntry = new char[firstEntryLength];
                    memcpy(firstEntry,newLeafPage,firstEntryLength);
                    ((unsigned*)firstEntry)[0] = pageRead[i];
                    insertFirstEntry(newRootNode, firstEntry, firstEntryLength,false);
                    memcpy(newRootNode + firstEntryLength, &newLeafPageNum, INTSIZE);
                    ixFileHandle.appendPage(newRootNode);
                    delete[] firstEntry;
                    delete[] newRootNode;
                } else {
                    unsigned short * newDirectory = (unsigned short*) newLeafPage;
                    goingSize = newDirectory[PAGE_SIZE / SHORTSIZE - 3 - 2 * 1 + 1];
                    goingUp = new char[goingSize];
                    memcpy(goingUp, newLeafPage, goingSize);
                    memcpy(goingUp, &newLeafPageNum, INTSIZE);
                }
                delete[] newLeafPage;
                breakNode = true;
                if (breakNode) {
                    int NeedToBreakNode = 0;
                }
            } else if (leafNode) {
                if (freeSpace < 40) {
                    int check_for_split = 0;
                }
                //add node on the slot
                shift(selectedPages[i], offset, slotUsed[i], modifiedKey, totalSize);
                ixFileHandle.writePage(pageRead[i],selectedPages[i]);
                break;
            } else if (breakNode && freeSpace < totalSize + 4) {
                char * newLeafPage = new char[PAGE_SIZE];
                createRootNode(newLeafPage);
                //break and connect the leaf nodes.
                unsigned currentSlotInput = slotUsed[i];
                unsigned short * newDirectory = (unsigned short*) newLeafPage;
                newLeafPageNum = ixFileHandle.getNumberOfPages();
                unsigned middleSize = directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * numberOfRecord/2 + 1];
                char * middleUp = new char[middleSize];
                memcpy(middleUp, &newLeafPageNum, INTSIZE);


                if (currentSlotInput <= numberOfRecord/2) {
                    splitHalfMiddleLeft(selectedPages[i], newLeafPage, middleUp, middleSize);
                    selectedPages[i];
                    shiftMiddleNode(selectedPages[i], offset, currentSlotInput, goingUp, goingSize);
                } else {
                    splitHalfMiddleRight(selectedPages[i], newLeafPage, middleUp, middleSize);
                    currentSlotInput = currentSlotInput - numberOfRecord/2 - 1;
                    offset = newDirectory[PAGE_SIZE/SHORTSIZE - 5 - 2 * currentSlotInput];
                    shiftMiddleNode(newLeafPage, offset, currentSlotInput, goingUp, goingSize);
                }
                ixFileHandle.writePage(pageRead[i],selectedPages[i]);
                ixFileHandle.appendPage(newLeafPage);

                if (i == 0) {
                    unsigned newRootPage = ixFileHandle.getNumberOfPages();
                    ixFileHandle.setRootNode(newRootPage);
                    ixFileHandle.saveHeader();
                    char * newRootNode = new char[PAGE_SIZE];
                    createRootNode(newRootNode);
                    unsigned short * newDirectory = (unsigned short*) newLeafPage;
                    char * firstEntry = new char[middleSize];
                    memcpy(firstEntry + 4 , middleUp + 4, middleSize - 4);
                    ((unsigned*)firstEntry)[0] = pageRead[i];
                    insertFirstEntry(newRootNode, firstEntry, middleSize,false);
                    memcpy(newRootNode + middleSize, middleUp, INTSIZE);
                    ixFileHandle.appendPage(newRootNode);
                    delete[] firstEntry;
                    delete[] newRootNode;
                } else {
                    delete[] goingUp;
                    goingUp = new char[middleSize];
                    memcpy(goingUp, middleUp, middleSize);
                }
                breakNode = true;
                if (breakNode) {
                    int NeedToBreakNode = 0;
                }
                delete[] middleUp;
                delete[] newLeafPage;
                //break do not need to connect
            } else {
                if (pageRead[i] == 230) {
                    int wait = 0;
                }
                //add node on the slot
                shiftMiddleNode(rootNode, offset, slotUsed[i], goingUp, goingSize);
                ixFileHandle.writePage(pageRead[i],selectedPages[i]);
                break;
            }
        }
        if( goingUp != NULL) {
            delete[] goingUp;
        }
        delete[] modifiedKey;
        return SUCCESS;
    }

    RC IndexManager::splitHalf(void * originalData, void * newData, unsigned originalPage, unsigned newPage) {
         unsigned short * directory = (unsigned short*) originalData;
        unsigned freeSpace = directory[PAGE_SIZE/SHORTSIZE - FREESPACE];
        unsigned numberOfRecord = directory[PAGE_SIZE/SHORTSIZE - NUMRECORD];

        unsigned divisionPoint = numberOfRecord/2 + 1;
        unsigned offset = directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * divisionPoint];
        unsigned short * newDirectory = (unsigned short*) newData;
        int j = 0;
        char * originalNode = (char*) originalData;
        char * newNode = (char*) newData;

        for(int i = offset ; i < 4096 - 6 - 4*( numberOfRecord + 1 ); i++) {
            newNode[j] = originalNode[i];
            originalNode[i] = -1;
            j++;
        }

        memcpy(originalNode + offset, &newPage, INTSIZE);
        memcpy(newNode, &originalPage, INTSIZE);


        directory[PAGE_SIZE/SHORTSIZE - NUMRECORD] = divisionPoint - 1;
        directory[PAGE_SIZE/SHORTSIZE - FREESPACE] += divisionPoint * 4;
        j = 0;
        for (int i = divisionPoint ; i < numberOfRecord + 1 ; i++ ) {
            unsigned int movedPosition = directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * i] - offset;
            newDirectory[PAGE_SIZE/SHORTSIZE - 5  - 2 * j] = movedPosition;
            newDirectory[PAGE_SIZE/SHORTSIZE - 5  - 2 * j + 1] = directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * i + 1];
            directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * i] = -1;
            directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * i + 1] = -1;
            j++;
        }
        unsigned endOfPoint = directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * (divisionPoint - 1)] + directory[PAGE_SIZE/SHORTSIZE - 3 - 2 * (divisionPoint - 1) + 1];
        directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * (divisionPoint)] = endOfPoint;
        directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * (divisionPoint) + 1] = -1;
        directory[PAGE_SIZE/SHORTSIZE - FREESPACE] = 4096 - directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * (divisionPoint)] - 6 - 4 * (divisionPoint + 1);
        for (int i = endOfPoint + 4; i < 4096 - directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * (divisionPoint)] - 6; i++){
            ((char*)originalData)[i] = -1;
        }
        unsigned tempOffset = newDirectory[PAGE_SIZE/SHORTSIZE - 5 - 2 * (j -1) ] + newDirectory[PAGE_SIZE/SHORTSIZE - 5 - 2 * (j-1) + 1];
        newDirectory[PAGE_SIZE/SHORTSIZE - 5  - 2 * (j)] = tempOffset;
        //newDirectory[PAGE_SIZE/SHORTSIZE - 5  - 2 * (j) + 1] = -1;
        if( j != numberOfRecord - divisionPoint) {
            int wait = 0;
        }
        newDirectory[PAGE_SIZE/SHORTSIZE - NUMRECORD] = j;
        newDirectory[PAGE_SIZE/SHORTSIZE - FREESPACE] = 4096 - newDirectory[PAGE_SIZE/SHORTSIZE - 5 - 2 * j ] - 6 - 4 * (j + 2);

    }

    RC IndexManager::splitHalfMiddleRight(void * originalData, void * newData,
                                     void * middleUp, unsigned &middleSize) {
        unsigned short * directory = (unsigned short*) originalData;
        unsigned freeSpace = directory[PAGE_SIZE/SHORTSIZE - FREESPACE];
        unsigned numberOfRecord = directory[PAGE_SIZE/SHORTSIZE - NUMRECORD];

        unsigned divisionPoint = numberOfRecord/2 + 1;
        unsigned offset = directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * divisionPoint];
        unsigned short * newDirectory = (unsigned short*) newData;
        int j = 0;
        char * originalNode = (char*) originalData;
        char * newNode = (char*) newData;
        unsigned middleOffset = directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * (divisionPoint + 1)];
        middleSize = middleOffset - offset;
        memcpy((char*)middleUp + INTSIZE, originalNode + offset + INTSIZE, middleSize - INTSIZE);
        int key = *(int*)((char*)middleUp + 4);

        for(int i = middleOffset; i < 4096 - 6 - 4*( numberOfRecord + 1 ); i++) {
            newNode[j] = originalNode[i];
            originalNode[i] = -1;
            j++;
        }

        for(int i = offset + 4; i < middleOffset; i++) {
            originalNode[i] = -1;
        }

        directory[PAGE_SIZE/SHORTSIZE - NUMRECORD] = divisionPoint - 1;
        directory[PAGE_SIZE/SHORTSIZE - FREESPACE] += divisionPoint * 4;
        j = 0;
        for (int i = divisionPoint + 1 ; i < numberOfRecord + 1 ; i++ ) {
            unsigned short PrevOffset = directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * i] - middleOffset;
            newDirectory[PAGE_SIZE/SHORTSIZE - 5  - 2 * j] = PrevOffset;
            newDirectory[PAGE_SIZE/SHORTSIZE - 5  - 2 * j + 1] = directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * i + 1];
            directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * i] = -1;
            directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * i + 1] = -1;
            j++;
        }
        unsigned endOfPoint = directory[PAGE_SIZE/SHORTSIZE - 5  - 2 * (divisionPoint)] + directory[PAGE_SIZE/SHORTSIZE - 5 - 2 * (divisionPoint) + 1];
        directory[PAGE_SIZE/SHORTSIZE - 5  - 2 * (divisionPoint + 1)] = endOfPoint;
        directory[PAGE_SIZE/SHORTSIZE - 5  - 2 * (divisionPoint + 1) + 1] = -1;
        directory[PAGE_SIZE/SHORTSIZE - FREESPACE] = 4096 - directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * (divisionPoint)] - 6 - 4 * (divisionPoint + 1);
        if (endOfPoint != offset) {
            int error = 0;
        }
        for (int i = endOfPoint + 4; i < 4096 - directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * (divisionPoint)] - 6; i++){
            ((char*)originalData)[i] = -1;
        }

        if(j != numberOfRecord - divisionPoint) {
            int wait = 0;
        }
        newDirectory[PAGE_SIZE/SHORTSIZE - 5 - 2 * j] = newDirectory[PAGE_SIZE/SHORTSIZE - 5 - 2 * (j - 1) ] + newDirectory[PAGE_SIZE/SHORTSIZE - 5 - 2 * (j - 1) + 1 ];
        newDirectory[PAGE_SIZE/SHORTSIZE - 5 - 2 * (numberOfRecord - divisionPoint) + 1] = -1;
        //newDirectory[PAGE_SIZE/SHORTSIZE - 7 - 2 * j] = newDirectory[PAGE_SIZE/SHORTSIZE - 7 - 2 * (j - 1) ] + newDirectory[PAGE_SIZE/SHORTSIZE - 7 - 2 * (j - 1) + 1 ];
        newDirectory[PAGE_SIZE/SHORTSIZE - NUMRECORD] = numberOfRecord - divisionPoint;
        newDirectory[PAGE_SIZE/SHORTSIZE - FREESPACE] = 4096 - newDirectory[PAGE_SIZE/SHORTSIZE - 5 - 2 * j ] - 6 - 4 * (j + 2);

    }

    RC IndexManager::splitHalfMiddleLeft(void * originalData, void * newData,
                                          void * middleUp, unsigned &middleSize) {
        unsigned short * directory = (unsigned short*) originalData;
        unsigned freeSpace = directory[PAGE_SIZE/SHORTSIZE - FREESPACE];
        unsigned numberOfRecord = directory[PAGE_SIZE/SHORTSIZE - NUMRECORD];

        unsigned divisionPoint = numberOfRecord/2;
        unsigned offset = directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * divisionPoint];
        unsigned short * newDirectory = (unsigned short*) newData;
        int j = 0;
        char * originalNode = (char*) originalData;
        char * newNode = (char*) newData;
        unsigned middleOffset = directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * (divisionPoint + 1)];
        middleSize = middleOffset - offset;
        memcpy((char*)middleUp + INTSIZE, originalNode + offset + INTSIZE, middleSize - INTSIZE);
        int key = *(int*)((char*)middleUp + 4);

        for(int i = middleOffset; i < 4096 - 6 - 4*( numberOfRecord + 1 ); i++) {
            newNode[j] = originalNode[i];
            originalNode[i] = -1;
            j++;
        }

        for(int i = offset + 4; i < middleOffset; i++) {
            originalNode[i] = -1;
        }

        directory[PAGE_SIZE/SHORTSIZE - NUMRECORD] = divisionPoint - 1;
        directory[PAGE_SIZE/SHORTSIZE - FREESPACE] += divisionPoint * 4;
        j = 0;
        for (int i = divisionPoint + 1 ; i < numberOfRecord + 1 ; i++ ) {
            unsigned short PrevOffset = directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * i] - middleOffset;
            newDirectory[PAGE_SIZE/SHORTSIZE - 5  - 2 * j] = PrevOffset;
            newDirectory[PAGE_SIZE/SHORTSIZE - 5  - 2 * j + 1] = directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * i + 1];
            directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * i] = -1;
            directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * i + 1] = -1;
            j++;
        }
        unsigned endOfPoint = directory[PAGE_SIZE/SHORTSIZE - 5  - 2 * (divisionPoint)] + directory[PAGE_SIZE/SHORTSIZE - 5 - 2 * (divisionPoint) + 1];
        directory[PAGE_SIZE/SHORTSIZE - 5  - 2 * (divisionPoint+1)] = endOfPoint;
        directory[PAGE_SIZE/SHORTSIZE - 5  - 2 * (divisionPoint+1) + 1] = -1;
        directory[PAGE_SIZE/SHORTSIZE - FREESPACE] = 4096 - directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * (divisionPoint)] - 6 - 4 * (divisionPoint + 1);
        if (endOfPoint != offset) {
            int error = 0;
        }
        for (int i = endOfPoint + 4; i < 4096 - directory[PAGE_SIZE/SHORTSIZE - 3  - 2 * (divisionPoint)] - 6; i++){
            ((char*)originalData)[i] = -1;
        }
        newDirectory[PAGE_SIZE/SHORTSIZE - 5 - 2 * j] = newDirectory[PAGE_SIZE/SHORTSIZE - 5 - 2 * (j - 1) ] + newDirectory[PAGE_SIZE/SHORTSIZE - 5 - 2 * (j - 1) + 1 ];
        newDirectory[PAGE_SIZE/SHORTSIZE - 5 - 2 * (numberOfRecord - divisionPoint) + 1] = -1;
        //newDirectory[PAGE_SIZE/SHORTSIZE - 7 - 2 * j] = newDirectory[PAGE_SIZE/SHORTSIZE - 7 - 2 * (j - 1) ] + newDirectory[PAGE_SIZE/SHORTSIZE - 7 - 2 * (j - 1) + 1 ];
        newDirectory[PAGE_SIZE/SHORTSIZE - NUMRECORD] = numberOfRecord - divisionPoint;
        newDirectory[PAGE_SIZE/SHORTSIZE - FREESPACE] = 4096 - newDirectory[PAGE_SIZE/SHORTSIZE - 5 - 2 * j ] - 6 - 4 * (j + 2);

    }

    RC IndexManager::shift(void * data, int offset, int slotToUse, const void *modifiedKey, int totalSize) {
        unsigned short * directory = (unsigned short *) data;
        unsigned numberOfRecord = directory[PAGE_SIZE/SHORTSIZE - NUMRECORD];
        unsigned freeSpace = directory[PAGE_SIZE/SHORTSIZE - FREESPACE];
        directory[PAGE_SIZE/SHORTSIZE - FREESPACE] -= totalSize;
        directory[PAGE_SIZE/SHORTSIZE - FREESPACE] -= 4;
        directory[PAGE_SIZE/SHORTSIZE - NUMRECORD] += 1;
        unsigned tempOffset, tempTotalSize;
        for (int i = numberOfRecord + 1; i > slotToUse; i--) {
            tempOffset = directory[PAGE_SIZE/SHORTSIZE - 3 - 2 * i];
            tempTotalSize = directory[PAGE_SIZE/SHORTSIZE - 3 - 2 * i + 1];
            unsigned short result = tempOffset + totalSize;
            directory[PAGE_SIZE/SHORTSIZE - 3 - 2 * (i+1) ] = result;
            directory[PAGE_SIZE/SHORTSIZE - 3 - 2 * (i+1) + 1] = tempTotalSize;
        }
        directory[PAGE_SIZE/SHORTSIZE - 3 - 2 * (slotToUse + 1) + 1] = totalSize;
        unsigned char * indexData = (unsigned char * ) data;
        //memmove(pageData + slotOffSet, pageData + slotOffSet + dataSize, endOfData - slotOffSet);
        for(int i = 4096 - 6 - 4 * ( numberOfRecord + 2) - totalSize - 1; i > offset - 1 ; i--){
            if (i == offset) {
                int check_site = 0;
            }
            indexData[i+totalSize] = indexData[i];
        }
        memcpy(indexData+offset, modifiedKey, totalSize);
    }

    RC IndexManager::shiftMiddleNode(void * data, int offset, int slotToUse, const void *modifiedKey, int totalSize) {
        unsigned short * directory = (unsigned short *) data;
        unsigned numberOfRecord = directory[PAGE_SIZE/SHORTSIZE - NUMRECORD];
        unsigned freeSpace = directory[PAGE_SIZE/SHORTSIZE - FREESPACE];
        directory[PAGE_SIZE/SHORTSIZE - FREESPACE] -= totalSize;
        directory[PAGE_SIZE/SHORTSIZE - FREESPACE] -= 4;
        directory[PAGE_SIZE/SHORTSIZE - NUMRECORD] += 1;
        unsigned tempOffset, tempTotalSize;
        for (int i = numberOfRecord + 1; i > slotToUse ; i--) {
            tempOffset = directory[PAGE_SIZE/SHORTSIZE - 3 - 2 * i];
            tempTotalSize = directory[PAGE_SIZE/SHORTSIZE - 3 - 2 * i + 1];
            unsigned short result = tempOffset + totalSize;
            directory[PAGE_SIZE/SHORTSIZE - 3 - 2 * (i+1) ] = result;
            directory[PAGE_SIZE/SHORTSIZE - 3 - 2 * (i+1) + 1] = tempTotalSize;
        }
        directory[PAGE_SIZE/SHORTSIZE - 3 - 2 * (slotToUse + 1)] = offset;
        directory[PAGE_SIZE/SHORTSIZE - 3 - 2 * (slotToUse + 1) + 1] = totalSize;
        unsigned char * indexData = (unsigned char * ) data;
        //memmove(pageData + slotOffSet, pageData + slotOffSet + dataSize, endOfData - slotOffSet);
        for(unsigned int i = 4096 - 6 - 4 * ( numberOfRecord + 2) - totalSize - 1; i > offset + 3 ; i--){
            if (i == offset) {
                int check_site = 0;
            }
            indexData[i+totalSize] = indexData[i];
        }
        memcpy(indexData + offset + 4, (char*)modifiedKey + 4, totalSize - 4);
        memcpy(indexData + offset + totalSize, modifiedKey, INTSIZE);
    }

    RC IndexManager::getLeafNode(IXFileHandle &ixFileHandle, const Attribute &attribute,
                                 std::vector<int> &pageRead, std::vector<int> &slotUsed,
                                 void * data, const void * key, const RID & rid,
                                 char** selectedPages, int &numberOfSelectedPage, unsigned& rootPage) {
        unsigned short * directory = (unsigned short*) data;
        unsigned numberOfRecord = directory[PAGE_SIZE/SHORTSIZE - NUMRECORD];
        bool findPlace = false;
        int lower = 0;
        int upper = numberOfRecord;
        unsigned current = (lower + upper)/2;
        unsigned offset = 0;
        bool leafNode = directory[PAGE_SIZE/SHORTSIZE - LEAFNODE] == 0;
        unsigned page = rootPage;
        unsigned problem = SUCCESS;

        while(!findPlace || !leafNode) {
            if (findPlace) {
                offset = directory[PAGE_SIZE/SHORTSIZE - 5 - 2 * current];
                selectedPages[numberOfSelectedPage] = (char*) data;
                numberOfSelectedPage++;
                pageRead.push_back(page);
                slotUsed.push_back(current);
                page = *(int*)((char*)data + offset);
                data = new char[PAGE_SIZE];
                ixFileHandle.readPage(page, data);
                directory = (unsigned short*) data;


                numberOfRecord = directory[PAGE_SIZE/SHORTSIZE - NUMRECORD];
                findPlace = false;
                lower = 0;
                upper = numberOfRecord;
                current = (lower + upper)/2;
                offset = 0;
                leafNode = directory[PAGE_SIZE/SHORTSIZE - LEAFNODE] == 0;
            }
            offset = directory[PAGE_SIZE/SHORTSIZE - 5 - 2 * current];
            int shift = 0;
            switch(attribute.type) {
                case AttrType::TypeInt:
                    shift = compareInt(data, offset, key, rid);
                    break;
                case AttrType::TypeReal:
                    shift = compareReal(data, offset, key, rid);
                    break;
                case AttrType::TypeVarChar:
                    shift = compareVarChar(data, offset, key, rid);
                    break;
            }
            if (shift == 100 && leafNode) {
                break;
            } else if (shift == 1) {
                lower = current + 1;
            } else if (shift == 100) {
                lower = current + 1;
            } else {
                upper = current - 1;
            }
            if (upper - lower < 0) {
                findPlace = true;
                if (leafNode && shift != 100) {
                    problem += 110;
                }
                //current = (lower + upper)/2;
                if( lower > numberOfRecord) {
                    current = numberOfRecord;
                } else {
                    current = lower;
                }
            } else {
                current = (lower + upper)/2;
                findPlace = false;
            }
        }
        selectedPages[numberOfSelectedPage] = (char*) data;
        numberOfSelectedPage++;
        pageRead.push_back(page);
        slotUsed.push_back(current);
        return problem;
    }

    int IndexManager::getLengthOfKey(const Attribute & attribute, const void * key) {
        int lengthOfKey = 0;
        switch(attribute.type) {
            case AttrType::TypeInt:
                lengthOfKey += INTSIZE;
                break;
            case AttrType::TypeReal:
                lengthOfKey += FLOATSIZE;
                break;
            case AttrType::TypeVarChar:
                lengthOfKey += INTSIZE;
                int length_of_string = *(int*)((char*)key);
                lengthOfKey += length_of_string;
        }
        return lengthOfKey;
    }

    void IndexManager::getModifiedKey(const void * key, void *modifiedData, const RID &rid, int lenOfKey) {
        unsigned pagenum = 0;
        memcpy((char*)modifiedData, &pagenum, INTSIZE);
        memcpy((char*)modifiedData+INTSIZE, key, lenOfKey);
        int pageNum = rid.pageNum;
        unsigned short slotNum = rid.slotNum;
        memcpy((char*)modifiedData+INTSIZE+lenOfKey, &pageNum, INTSIZE);
        memcpy((char*)modifiedData+INTSIZE+lenOfKey+INTSIZE, &slotNum, SHORTSIZE);
    }

    int IndexManager::compareInt(void * data, unsigned offset, const void * key, const RID &rid) {
        int keyint = *((int*)key);
        int dataint = *((int*)((char*)data + offset + INTSIZE));
        if (keyint == dataint) {
            RID temp;
            temp.pageNum = *((int*)((char*)data + offset + INTSIZE + INTSIZE));
            temp.slotNum = *((short*)((char*)data + offset + INTSIZE + INTSIZE + INTSIZE));
            if (temp.pageNum == rid.pageNum) {
                if(rid.slotNum < temp.slotNum) {
                    return 0;
                } else if (rid.slotNum > temp.slotNum) {
                    return 1;
                } else {
                    return RC_UPDATE_DATA_ERROR;
                }
            } else if (rid.pageNum < temp.pageNum ) {
                return 0;
            } else {
                return 1;
            }
        } else if (keyint < dataint) {
            return 0;
        } else {
            return 1;
        }
    }

    int IndexManager::compareReal(void * data, unsigned offset, const void * key, const RID &rid) {
        float keyfloat = *((float*)key);
        float datafloat = *((float*)((char*)data + offset + INTSIZE));
        if (keyfloat == datafloat) {
            RID temp;
            temp.pageNum = *((int*)((char*)data + offset + INTSIZE + FLOATSIZE));
            temp.slotNum = *((short*)((char*)data + offset + INTSIZE + FLOATSIZE + INTSIZE));
            if (temp.pageNum == rid.pageNum) {
                if(rid.slotNum < temp.slotNum) {
                    return 0;
                } else if (rid.slotNum > temp.slotNum) {
                    return 1;
                } else {
                    return RC_UPDATE_DATA_ERROR;
                }
            } else if (rid.pageNum < temp.pageNum ) {
                return 0;
            } else {
                return 1;
            }
        } else if (keyfloat < datafloat) {
            return 0;
        } else {
            return 1;
        }
    }

    int IndexManager::compareVarChar(void * data, unsigned offset, const void * key, const RID &rid) {
        int comp = memcmp((char*)key+INTSIZE, (char*)data+offset+INTSIZE+INTSIZE, *((int*)key));
        int length = *((int*)key);
        if (comp == 0) {
            RID temp;
            temp.pageNum = *((int*)((char*)data + offset + INTSIZE + INTSIZE + length));
            temp.slotNum = *((short*)((char*)data + offset + INTSIZE + INTSIZE + length + INTSIZE));
            if (temp.pageNum == rid.pageNum) {
                if(rid.slotNum < temp.slotNum) {
                    return 0;
                } else if (rid.slotNum > temp.slotNum) {
                    return 1;
                } else {
                    return RC_UPDATE_DATA_ERROR;
                }
            } else if (rid.pageNum < temp.pageNum ) {
                return 0;
            } else {
                return 1;
            }
        } else if (comp < 0) {
            return 0;
        } else {
            return 1;
        }
    }


    RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute,
                                 const void *key, const RID &rid) {
        unsigned numberOfPage = ixFileHandle.getNumberOfPages();
        char * rootNode = new char[PAGE_SIZE];
        unsigned rootPage = 0;
        if (numberOfPage == 0) {
            return RC_DELETE_NON_EXISTING_ENTRY;
        } else {
            ixFileHandle.readPage(0, rootNode);
            rootPage = ((int*)rootNode)[4];
        }
        ixFileHandle.readPage(rootPage, rootNode);
        bool leafNode = ((unsigned short *) rootNode)[PAGE_SIZE/SHORTSIZE - LEAFNODE] == 0;
        unsigned short numberOfRecord = ((unsigned short *) rootNode)[PAGE_SIZE/SHORTSIZE - NUMRECORD];
        int problem = SUCCESS;

        std::vector<int> pageRead;
        std::vector<int> slotUsed;
        int numberOfSelectedPages = 0;
        int height = ixFileHandle.getHeight();
        char ** selectedPages = new char* [height];
        problem += getLeafNode(ixFileHandle, attribute, pageRead, slotUsed, rootNode,
                    key, rid, selectedPages, numberOfSelectedPages, rootPage);
        problem += deleteLeafEntry(selectedPages, ixFileHandle, attribute, key, rid,
                        pageRead, slotUsed, numberOfSelectedPages);
        for (int i = 0 ; i < numberOfSelectedPages ; i++) {
            delete[] selectedPages[i];
        }
        delete[] selectedPages;
        return problem;
    }

    RC IndexManager::deleteLeafEntry(char **selectedPages, IXFileHandle &ixFileHandle, const Attribute &attribute,
                                     const void *key, const RID &rid, std::vector<int> &pageRead,
                                     std::vector<int> &slotUsed, int &numberOfSelectedPages) {
        bool breakNode = false;
        unsigned lenOfKey = getLengthOfKey(attribute, key);
        unsigned totalSize = INTSIZE+lenOfKey+INTSIZE+SHORTSIZE;
        char * modifiedKey = new char[totalSize];
        getModifiedKey(key,modifiedKey,rid,lenOfKey);
        int problem = SUCCESS;

        for(int i = numberOfSelectedPages - 1; i > -1; i--) {

            char *rootNode = selectedPages[i];
            unsigned offset = 0;
            unsigned short *directory = (unsigned short *) rootNode;
            unsigned freeSpace = directory[PAGE_SIZE / SHORTSIZE - FREESPACE];
            unsigned numberOfRecord = directory[PAGE_SIZE / SHORTSIZE - NUMRECORD];

            offset = directory[PAGE_SIZE / SHORTSIZE - 5 - 2 * slotUsed[i]];
            bool leafNode = directory[PAGE_SIZE / SHORTSIZE - LEAFNODE] == 0;
            if (leafNode) {
                problem += deleteLeafEntryOnly(rootNode, offset, slotUsed[i], modifiedKey, totalSize);
                problem += ixFileHandle.writePage(pageRead[i],rootNode);
            }
        }
        delete[] modifiedKey;
        return problem;
    }

    RC IndexManager::deleteLeafEntryOnly(void *data, int offset, int slotToUse, const void *modifiedKey,
                                         int totalSize) {
        unsigned short * directory = (unsigned short *) data;
        unsigned numberOfRecord = directory[PAGE_SIZE/SHORTSIZE - NUMRECORD];
        unsigned endOfData = directory[PAGE_SIZE/SHORTSIZE - 5 - 2 * numberOfRecord] + 4;
        unsigned freeSpace = directory[PAGE_SIZE/SHORTSIZE - FREESPACE];
        if(numberOfRecord == 0) {
            return RC_DELETE_NON_EXISTING_ENTRY;
        }
        directory[PAGE_SIZE/SHORTSIZE - FREESPACE] += totalSize;
        directory[PAGE_SIZE/SHORTSIZE - FREESPACE] += 4;
        directory[PAGE_SIZE/SHORTSIZE - NUMRECORD] -= 1;
        unsigned tempOffset, tempTotalSize;
        for (int i = slotToUse; i < numberOfRecord; i++) {
            tempOffset = directory[PAGE_SIZE/SHORTSIZE - 5 - 2 * (i+1)];
            tempTotalSize = directory[PAGE_SIZE/SHORTSIZE - 5 - 2 * (i+1) + 1];
            unsigned short result = tempOffset - totalSize;
            directory[PAGE_SIZE/SHORTSIZE - 5 - 2 * i ] = result;
            directory[PAGE_SIZE/SHORTSIZE - 5 - 2 * i + 1] = tempTotalSize;
        }
        directory[PAGE_SIZE/SHORTSIZE - 5 - 2 * (numberOfRecord)] = directory[PAGE_SIZE/SHORTSIZE - 5 - 2 * (numberOfRecord - 1) + 1] + directory[PAGE_SIZE/SHORTSIZE - 5 - 2 * (numberOfRecord - 1)];
        directory[PAGE_SIZE/SHORTSIZE - 5 - 2 * (numberOfRecord) + 1] = -1;
        unsigned char * indexData = (unsigned char * ) data;
        //memmove(pageData + slotOffSet, pageData + slotOffSet + dataSize, endOfData - slotOffSet);
        for(int i = offset; i < endOfData; i++){
            indexData[i] = indexData[i + totalSize];
        }
        for (int i = endOfData; i < endOfData+freeSpace; i++) {
            indexData[i] = -1;
        }
        return SUCCESS;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        if (access(ixFileHandle.savedFileName, F_OK) != 0) {
            return RC_FILE_NAME_NOT_EXIST;
        }
        ix_ScanIterator.open(ixFileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
        return SUCCESS;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        unsigned * rootNode = new unsigned[PAGE_SIZE];
        ixFileHandle.readPage(0, rootNode);
        unsigned rootPage = rootNode[4];
        ixFileHandle.readPage(rootPage, rootNode);
        bool leafNode = ((unsigned short*)rootNode)[PAGE_SIZE/SHORTSIZE - LEAFNODE] == 0;
        if (leafNode) {
            printBTreeLeaf(rootNode, attribute, out);
        } else {
            printBTreeRoot(rootNode, ixFileHandle, attribute, out);
        }
        delete[] rootNode;
        return SUCCESS;
    }

    RC IndexManager::printBTreeRoot(void *rootNode, IXFileHandle &ixFileHandle, const Attribute &attribute,
                                    std::ostream &out) const {
        out << "{ \"keys\" : [" ;
        char * key = new char[attribute.length];
        char* data = (char*) rootNode;
        bool skip = false;
        std::vector<int> pageNumbers;
        unsigned numberOfRecord = ((unsigned short*)rootNode)[PAGE_SIZE/SHORTSIZE - NUMRECORD];
        for ( int i = 0 ; i < numberOfRecord + 1 ; i++) {
            unsigned offset = ((unsigned short*)rootNode)[PAGE_SIZE/SHORTSIZE - 5 - 2 * i];
            unsigned totalsize = ((unsigned short*)rootNode)[PAGE_SIZE/SHORTSIZE - 5 - 2 * i + 1];
            pageNumbers.push_back(*(int*)(data+offset));
            offset += INTSIZE;
            if (pageNumbers.size() > numberOfRecord){
                break;
            }
            unsigned length = 0;
            switch(attribute.type) {
                case AttrType::TypeInt:
                    length += INTSIZE;
                    break;
                case AttrType::TypeReal:
                    length += FLOATSIZE;
                    break;
                case AttrType::TypeVarChar:
                    length += *((int *) (data + offset));
                    offset += INTSIZE;
                    break;
            }
            switch(attribute.type) {
                case AttrType::TypeInt:
                    out << "\"" << *((int *) (data + offset)) << "\"";
                    memcpy(key, data + offset, length);
                    break;
                case AttrType::TypeReal:
                    out << "\"" << *((float *) (data + offset)) << "\"";
                    memcpy(key, data + offset, length);
                    break;
                case AttrType::TypeVarChar:
                    memcpy(key, data + offset, length);
                    key[length] = '\0';
                    out << "\"" << key << "\"";
                    break;
            }
            if (i != numberOfRecord-1) {
                out <<",";
            }
        }
        delete[] key;
        out << "] , ";
        out << " \"children\" : [";
        for (int i = 0; i<pageNumbers.size() ; i++) {
            ixFileHandle.readPage(pageNumbers.at(i), rootNode);
            bool leafNode = ((unsigned short*)rootNode)[PAGE_SIZE/SHORTSIZE - LEAFNODE] == 0;
            if (leafNode) {
                printBTreeLeaf(rootNode, attribute, out);
            } else {
                printBTreeRoot(rootNode, ixFileHandle, attribute, out);
            }
            if (i != pageNumbers.size() - 1) {
                out << ",";
            }
        }
        out << "]}";
        return SUCCESS;
    }

    RC IndexManager::printBTreeLeaf(void * leafNode, const Attribute &attribute, std::ostream &out) const {
        out << "{ \"keys\" : [" ;
        char * key = new char[attribute.length];
        char* data = (char*) leafNode;
        bool skip = false;
        unsigned numberOfRecord = ((unsigned short*)leafNode)[PAGE_SIZE/SHORTSIZE - NUMRECORD];
        for ( int i = 0 ; i < numberOfRecord ; i++) {
            unsigned offset = ((unsigned short*)leafNode)[PAGE_SIZE/SHORTSIZE - 5 - 2 * i];
            offset += INTSIZE;
            unsigned length = 0;
            switch(attribute.type) {
                case AttrType::TypeInt:
                    length += INTSIZE;
                    break;
                case AttrType::TypeReal:
                    length += FLOATSIZE;
                    break;
                case AttrType::TypeVarChar:
                    length += *((int *) (data + offset));
                    break;
            }
            if (attribute.type != AttrType::TypeVarChar) {
                if (memcmp(key,data+offset, length) == 0) {
                    skip = true;
                } else {
                    skip = false;
                }
            } else {
                if (memcmp(key,data + offset + INTSIZE, length) == 0) {
                    skip = true;
                } else {
                    skip = false;
                }
            }
            if (!skip) {
                if (i != 0 ){
                    out << "]\",";
                }
                switch(attribute.type) {
                    case AttrType::TypeInt:
                        out << "\"" << *((int*)(data+offset)) << ":[";
                        memcpy(key, data+offset, length);
                        break;
                    case AttrType::TypeReal:
                        out << "\"" << *((float*)(data+offset)) << ":[";
                        memcpy(key, data+offset, length);
                        break;
                    case AttrType::TypeVarChar:
                        memcpy(key, data+offset+INTSIZE, length);
                        key[length] = '\0';
                        out << "\"" << key << ":[";
                        break;
                }
            } else {
                out << ",";
            }
            switch(attribute.type) {
                case AttrType::TypeInt:
                    offset += INTSIZE;
                    break;
                case AttrType::TypeReal:
                    offset += FLOATSIZE;
                    break;
                case AttrType::TypeVarChar:
                    length = *((int *) (data + offset));
                    offset += INTSIZE;
                    offset += length;
                    break;
            }
            out << "(" << *((unsigned int*)(data+offset)) << "," << *((unsigned short*)(data+offset+INTSIZE)) << ")";
            if (i == numberOfRecord - 1) {
                out << "]\"";
            }
        }
        out << "]}";
        delete[] key;
        return SUCCESS;
    }

    RC IndexManager::createRootNode(void * data){
        unsigned short* rootNode = (unsigned short*) data;
        for(int i = 0 ; i < PAGE_SIZE/SHORTSIZE; i++) rootNode[i] = -1;
        rootNode[PAGE_SIZE/SHORTSIZE - FREESPACE] = 4096 - 2*3;
        rootNode[PAGE_SIZE/SHORTSIZE - NUMRECORD] = 0;
        rootNode[PAGE_SIZE/SHORTSIZE - LEAFNODE] = -1;
    }

    RC IndexManager::createLeafNode(void * data){
        unsigned short* rootNode = (unsigned short*) data;
        for(int i = 0 ; i < PAGE_SIZE/SHORTSIZE; i++) rootNode[i] = -1;
        rootNode[PAGE_SIZE/SHORTSIZE - FREESPACE] = 4096 - 2*3;
        rootNode[PAGE_SIZE/SHORTSIZE - NUMRECORD] = 0;
        rootNode[PAGE_SIZE/SHORTSIZE - LEAFNODE] = 0;
    }

    IX_ScanIterator::IX_ScanIterator() {
        low = NULL;
        high = NULL;
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        unsigned short * directory = (unsigned short*) data;
        unsigned numberOfRecord = directory[PAGE_SIZE/SHORTSIZE - NUMRECORD];
        unsigned offset = 0;
        if (prev_numberOfRecord > numberOfRecord) {
            currentSlot -= prev_numberOfRecord - numberOfRecord;
            prev_numberOfRecord = numberOfRecord;
        }
        while (currentSlot == numberOfRecord) {
            offset = directory[PAGE_SIZE/SHORTSIZE - 5 - 2 * currentSlot];
            currentPage = *(int*)(data + offset);;
            if (currentPage == 0) {
                return IX_EOF;
            }
            offset = 0;
            storedIXFileHandle->readPage(currentPage, data);
            directory = (unsigned short*) data;
            numberOfRecord = directory[PAGE_SIZE/SHORTSIZE - NUMRECORD];
            currentSlot = 0;
            prev_numberOfRecord = numberOfRecord;
            numPageUsed++;
        }
        if (high == NULL) {
            offset = directory[PAGE_SIZE/SHORTSIZE - 5 - 2 * currentSlot];
            offset += INTSIZE;
            switch(storedAttribute.type) {
                case AttrType::TypeInt:
                    memcpy((char*)key, data+offset, INTSIZE);
                    offset += INTSIZE;
                    break;
                case AttrType::TypeReal:
                    memcpy((char*)key, data+offset, FLOATSIZE);
                    offset += FLOATSIZE;
                    break;
                case AttrType::TypeVarChar:
                    unsigned length = *(int*)(data+offset);
                    memcpy((char*)key, data+offset, INTSIZE);
                    offset += INTSIZE;
                    memcpy((char*)key+INTSIZE, data+offset, length);
                    offset += length;
                    break;
            }
            rid.pageNum = *(int*)(data+offset);
            rid.slotNum = *(short*)(data+offset+INTSIZE);
            currentSlot += 1;
            return SUCCESS;
        } else {
            int shift = 0;
            offset = directory[PAGE_SIZE/SHORTSIZE - 5 - 2 * currentSlot];
            switch(storedAttribute.type) {
                case AttrType::TypeInt:
                    shift = compareIntHigh(offset);
                    break;
                case AttrType::TypeReal:
                    shift = compareRealHigh(offset);
                    break;
                case AttrType::TypeVarChar:
                    shift = compareVarCharHigh(offset);
                    break;
            }
            if (shift == 1) {
                return IX_EOF;
            } else {
                offset += INTSIZE;
                switch(storedAttribute.type) {
                    case AttrType::TypeInt:
                        memcpy((char*)key, data+offset, INTSIZE);
                        offset += INTSIZE;
                        break;
                    case AttrType::TypeReal:
                        memcpy((char*)key, data+offset, FLOATSIZE);
                        offset += FLOATSIZE;
                        break;
                    case AttrType::TypeVarChar:
                        unsigned length = *(int*)(data+offset);
                        memcpy((char*)key, data+offset, INTSIZE);
                        offset += INTSIZE;
                        memcpy((char*)key+INTSIZE, data+offset, length);
                        offset += length;
                        break;
                }
                rid.pageNum = *(int*)(data+offset);
                rid.slotNum = *(short*)(data+offset+INTSIZE);
                currentSlot += 1;
                return SUCCESS;
            }
        }
    }

    RC IX_ScanIterator::open(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *lowKey, const void *highKey,
                             bool lowKeyInclusive, bool highKeyInclusive) {
        storedIXFileHandle = &ixFileHandle;
        storedAttribute = attribute;
        if (lowKey != NULL) {
            low = new char[storedAttribute.length];
            memcpy(low, lowKey, storedAttribute.length);
        }
        if (highKey != NULL) {
            high = new char[storedAttribute.length];
            memcpy(high, highKey, storedAttribute.length);
        }
        lowIn = lowKeyInclusive;
        highIn = highKeyInclusive;
        data = new char[PAGE_SIZE];
        currentPage = 0;
        currentSlot = 0;
        storedIXFileHandle->readPage(currentPage, data);
        currentPage = ((unsigned*)data)[4];
        storedIXFileHandle->readPage(currentPage, data);
        prev_numberOfRecord = 0;
        numPageUsed = 1;
        bool leafNode = ((unsigned short *) data)[PAGE_SIZE/SHORTSIZE - LEAFNODE] == 0;
        if (!leafNode) {
            getStartPage();
        }
        return SUCCESS;
    }

    RC IX_ScanIterator::getStartPage() {
        unsigned short * directory = (unsigned short*) data;
        unsigned numberOfRecord = directory[PAGE_SIZE/SHORTSIZE - NUMRECORD];
        bool findPlace = false;
        int lower = 0;
        int upper = numberOfRecord;
        currentSlot = (lower + upper)/2;
        unsigned offset = 0;
        bool leafNode = directory[PAGE_SIZE/SHORTSIZE - LEAFNODE] == 0;
        unsigned page = currentPage;

        while(!findPlace || !leafNode) {
            if (findPlace) {
                offset = directory[PAGE_SIZE/SHORTSIZE - 5 - 2 * currentSlot];
                currentPage = *(int*)(data + offset);;
                storedIXFileHandle->readPage(currentPage, data);
                directory = (unsigned short *) data;

                leafNode = directory[PAGE_SIZE/SHORTSIZE - LEAFNODE] == 0;
                numberOfRecord = directory[PAGE_SIZE/SHORTSIZE - NUMRECORD];
                findPlace = false;
                lower = 0;
                upper = numberOfRecord;
                prev_numberOfRecord = numberOfRecord;
                currentSlot = (lower + upper)/2;
                offset = 0;
            }
            offset = directory[PAGE_SIZE/SHORTSIZE - 5 - 2 * currentSlot];
            int shift = 0;
            switch(storedAttribute.type) {
                case AttrType::TypeInt:
                    shift = compareInt(offset);
                    break;
                case AttrType::TypeReal:
                    shift = compareReal(offset);
                    break;
                case AttrType::TypeVarChar:
                    shift = compareVarChar(offset);
                    break;
            }
            if (shift == 1) {
                lower = currentSlot +1;
            } else {
                upper = currentSlot - 1;
            }
            if (upper - lower < 0) {
                findPlace = true;
                if ( lower > numberOfRecord) {
                    currentSlot = numberOfRecord;
                } else {
                    currentSlot = lower;
                }
            } else {
                currentSlot = (lower+upper)/2;
                findPlace = false;
            }
        }
    }

    int IX_ScanIterator::compareInt(int offset) {
        if (low == NULL) {
            return 0;
        } else {
            int lowint = *((int*)low);
            int dataint = *((int*)((char*)data + offset + INTSIZE));
            if (lowint < dataint) {
                return 0;
            } else if (lowint > dataint) {
                return 1;
            } else if (lowint == dataint && lowIn) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    int IX_ScanIterator::compareReal(int offset) {
        if (low == NULL) {
            return 0;
        } else {
            float lowfloat = *((float*)low);
            float datafloat = *((float*)((char*)data + offset + INTSIZE));
            if (lowfloat < datafloat) {
                return 0;
            } else if (lowfloat > datafloat) {
                return 1;
            } else if (lowfloat == datafloat && lowIn) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    int IX_ScanIterator::compareVarChar(int offset) {
        if (low == NULL) {
            return 0;
        } else {
            int comp = memcmp(low+INTSIZE, data+offset+INTSIZE, *((int*)low));
            if (comp < 0) {
                return 0;
            } else if (comp > 0) {
                return 1;
            } else if (comp == 0 && lowIn) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    int IX_ScanIterator::compareIntHigh(int offset) {
        if (high == NULL) {
            return 0;
        } else {
            int highint = *((int*)high);
            int dataint = *((int*)((char*)data + offset + INTSIZE));
            if (highint > dataint) {
                return 0;
            } else if (highint < dataint) {
                return 1;
            } else if (highint == dataint && highIn) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    int IX_ScanIterator::compareRealHigh(int offset) {
        if (high == NULL) {
            return 0;
        } else {
            float highfloat = *((float*)high);
            float datafloat = *((float*)((char*)data + offset + INTSIZE));
            if (highfloat > datafloat) {
                return 0;
            } else if (highfloat < datafloat) {
                return 1;
            } else if (highfloat == datafloat && highIn) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    int IX_ScanIterator::compareVarCharHigh(int offset) {
        if (high == NULL) {
            return 0;
        } else {
            int comp = memcmp(high+INTSIZE, data+offset+INTSIZE, *((int*)high));
            if (comp > 0) {
                return 0;
            } else if (comp < 0) {
                return 1;
            } else if (comp == 0 && highIn) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    RC IX_ScanIterator::close() {
        if (low != NULL) {
            delete[] low;
        }
        if (high != NULL) {
            delete[] high;
        }
        delete[] data;
        return SUCCESS;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
        savedFileName = NULL;
        numberOfPages = 0;
        filesize = 0;
        rootNode = 0;
        height = 0;
    }

    RC IXFileHandle::openFile(const std::string &fileName) { // get string of file name and save as pointer to filename
        if (savedFileName != NULL) {
            return RC_OPEN_SAME_INDEX_FILE;
        }
        int len = fileName.size();
        savedFileName  = new char[len+1];
        std::copy(fileName.begin(), fileName.end(), savedFileName);
        savedFileName[len] = '\0';
        filePointer = std::fopen(savedFileName, "rb+");
        getHeader();
        return SUCCESS;
    }

    void IXFileHandle::getHeader(){
        filesizecheck();
        if(filesize > PAGE_SIZE) {
            unsigned * header = new unsigned[PAGE_SIZE/UNSIGNEDSIZE];

            std::fseek(filePointer, 0L, SEEK_SET);
            std::fread(header, UNSIGNEDSIZE, PAGE_SIZE/UNSIGNEDSIZE, filePointer);

            ixReadPageCounter = header[0];
            ixWritePageCounter = header[1];
            ixAppendPageCounter = header[2];
            numberOfPages = header[3];
            rootNode = header[4];
            height = header[5];
            delete[] header;
            std::fseek(filePointer, 0L, SEEK_SET);
        }
    }

    void IXFileHandle::configureHeader(void * header){  // store read write append page count to header
        unsigned * tempPointer = (unsigned *) header;
        tempPointer[0] = ixReadPageCounter;
        tempPointer[1] = ixWritePageCounter;
        tempPointer[2] = ixAppendPageCounter;
        tempPointer[3] = numberOfPages;
        tempPointer[4] = rootNode;
        tempPointer[5] = height;
        for(int i = 5; i < PAGE_SIZE/UNSIGNEDSIZE; i++){
            tempPointer[i] = -1;
        }
    }

    unsigned IXFileHandle::setRootNode(unsigned rootPage) {
        rootNode = rootPage;
        height++;
    }

    RC IXFileHandle::saveHeader(){ // save the header to the file
        ixWritePageCounter++;
        unsigned * header = new unsigned[PAGE_SIZE/UNSIGNEDSIZE];
        configureHeader(header); // save the header with the current Counters

        std::fseek(filePointer, 0L, SEEK_SET);
        std::fwrite(header, UNSIGNEDSIZE, PAGE_SIZE/UNSIGNEDSIZE, filePointer);
        delete[] header;

        filesizecheck();
        unsigned temp = filesize;
        if (temp != numberOfPages*PAGE_SIZE){  // check if the file size have changed
            return RC_FILE_SIZE_ERROR;
        }
        return SUCCESS;
    }

    unsigned IXFileHandle::getNumberOfPages() {
        return numberOfPages;
    }

    unsigned IXFileHandle::getHeight() {
        return height;
    }

    RC IXFileHandle::closeFile() {
        saveHeader();
        std::fclose(filePointer);
        delete[] savedFileName;
        savedFileName = NULL;
        return SUCCESS;
    }

    unsigned IXFileHandle::filesizecheck() { // check the size of the file
        std::fseek(filePointer, 0L, SEEK_END);
        filesize = std::ftell(filePointer);
        std::fseek(filePointer, 0L, SEEK_SET);
    }

    RC IXFileHandle::readPage(PageNum pageNum, void *data) {
        if (pageNum > numberOfPages){ // check if the page number is invalid
            return RC_READ_NONEXISTENT_PAGE;
        } else if (pageNum < 0) {
            return RC_READ_NONEXISTENT_PAGE;
        }
        ixReadPageCounter++;

        filesizecheck();
        unsigned temp = filesize;
        if (temp != numberOfPages*PAGE_SIZE){ // check if the file size is correct
            return RC_FILE_SIZE_ERROR;
        }


        std::fseek(filePointer, PAGE_SIZE*pageNum, SEEK_SET); // move file pointer to the start of page
        std::fread(data, 1, PAGE_SIZE, filePointer); // read the data from start of page to next page
        std::fseek(filePointer, 0L, SEEK_SET);

        filesizecheck();
        if(temp != filesize) { // check if filesize have changed with the write move
            return RC_FILE_SIZE_ERROR;
        }
        if( (ixReadPageCounter + ixAppendPageCounter + ixWritePageCounter) % 50 == 0 ) {
            saveHeader();
        }
        return SUCCESS;
    }

    RC IXFileHandle::writePage(PageNum pageNum, const void *data) {
        if (pageNum > numberOfPages){ // check if the page Number is correct
            return RC_WRITE_NONEXISTENT_PAGE;
        } else if (pageNum < 0) {
            return RC_WRITE_NONEXISTENT_PAGE;
        }
        ixWritePageCounter++;
        filesizecheck();
        unsigned temp = filesize;
        if (temp != numberOfPages*PAGE_SIZE){
            return RC_FILE_SIZE_ERROR;
        }


        std::fseek(filePointer, PAGE_SIZE*pageNum, SEEK_SET); // move file pointer to the start of page
        std::fwrite(data, 1, PAGE_SIZE, filePointer); //overwrite data
        std::fseek(filePointer, 0L, SEEK_SET);

        filesizecheck();
        if(temp != filesize) {
            return RC_FILE_SIZE_ERROR;
        }
        if( (ixReadPageCounter + ixAppendPageCounter + ixWritePageCounter) % 50 == 0 ) {
            saveHeader();
        }
        return SUCCESS;
    }

    RC IXFileHandle::appendPage(const void *data) {
        ixAppendPageCounter++;
        filesizecheck();
        unsigned temp = filesize;
        unsigned realsize = 0;
        if(numberOfPages == 0) {
            realsize = 4096;
        } else {
            realsize = numberOfPages*PAGE_SIZE;
        }
        if (temp != realsize){  // check file size before appending a page
            return RC_FILE_SIZE_ERROR;
        }

        if(numberOfPages == 0){
            std::fseek(filePointer, 0L, SEEK_SET);
            std::fwrite(data, 1, PAGE_SIZE, filePointer); // write a new data
        } else {
            std::fseek(filePointer, 0L, SEEK_END);
            std::fwrite(data, 1, PAGE_SIZE, filePointer); // write a new data
        }

        numberOfPages++;

        std::fseek(filePointer, 0L, SEEK_SET);

        filesizecheck();
        if(temp+PAGE_SIZE != filesize) { // check if file size have increased with factor of PAGE_SIZE
            return RC_FILE_SIZE_ERROR;
        }
        if( (ixReadPageCounter + ixAppendPageCounter + ixWritePageCounter) % 50 == 0 ) {
            saveHeader();
        }
        return SUCCESS;
    }

    IXFileHandle::~IXFileHandle() {
    }

    RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = ixReadPageCounter;
        writePageCount = ixWritePageCounter;
        appendPageCount = ixAppendPageCounter;
        return SUCCESS;
    }

} // namespace PeterDB