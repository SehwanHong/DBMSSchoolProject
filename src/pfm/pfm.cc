#include "src/include/pfm.h"

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    char* PagedFileManager::String_to_char_point(const std::string & str) {
        int len = str.size();
        char * pchar = new char(len+1);
        std::copy(str.begin(), str.end(), pchar);
        pchar[len] = '\0';
        return pchar;
    }

    RC PagedFileManager::createFile(const std::string &fileName) {
        char * name = String_to_char_point(fileName);
        if (access(name, F_OK) == 0) {
            delete[] name;
            return RC_FILE_NAME_EXIST;
        } else {
            FILE * pFile = std::fopen(name, "brw");
            std::fclose(pFile);
            delete[] name;
            return SUCCESS;
        };
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        char * name = String_to_char_point(fileName);
        if (access(name, F_OK) == 0) {
            std::remove(name);
            delete[] name;
            return SUCCESS;
        } else {
            delete[] name;
            return RC_FILE_NAME_NOT_EXIST;
        };
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        char * name = String_to_char_point(fileName);
        if (access(name, F_OK) == 0) {
            FILE * pFile = std::fopen(name, "brw");
            fileHandle.storeFilePointer(pFile);
            delete[] name;
            return SUCCESS;
        } else {
            delete[] name;
            return RC_FILE_NAME_NOT_EXIST;
        };
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        std::fclose(fileHandle.filePointer);
        fileHandle.filePointer = NULL;
        return SUCCESS;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        numberOfPages = 0;
    }

    FileHandle::~FileHandle() = default;

    void FileHandle::storeFilePointer(FILE *pfile) {
        filePointer = pfile;
        if (std::feof(pfile)) {
            unsigned * header = new unsigned[PAGE_SIZE/8];
            configureHeader(header);
            std::fwrite(header, sizeof(unsigned),PAGE_SIZE/8, pfile);
            std::rewind(filePointer);
            delete [] header;
        } else {
            getHeader();
        }
    }

    void FileHandle::configureHeader(unsigned &header){
        unsigned * tempPointer = header;
        tempPointer[0] = readPageCounter;
        tempPointer[1] = writePageCounter;
        tempPointer[2] = appendPageCounter;
        tempPointer[3] = numberOfPages;
        for(int i = 4; i < PAGE_SIZE/8; i++){
            tempPointer[i] = 0;
        }
    }

    void FileHandle::getHeader(){
        fscanf(filePointer,"%u", &readPageCounter);
        fscanf(filePointer,"%u", &writePageCounter);
        fscanf(filePointer,"%u", &appendPageCounter);
        fscanf(filePointer,"%u", &numberOfPages);
        rewind(filePointer);
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        return -1;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        return -1;
    }

    RC FileHandle::appendPage(const void *data) {
        return -1;
    }

    unsigned FileHandle::getNumberOfPages() {
        return -1;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        return -1;
    }

} // namespace PeterDB