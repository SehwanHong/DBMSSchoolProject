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
        char * pointer = new char[len+1];
        std::copy(str.begin(), str.end(), pointer); // need to modify this valgrind error
        pointer[len] = '\0';
        return pointer;
    }

    RC PagedFileManager::createFile(const std::string &fileName) {
        char * name = String_to_char_point(fileName);
        if (access(name, F_OK) == 0) { // check if the file name exists
            delete[] name;
            return RC_FILE_NAME_EXIST;
        } else {
            FILE * pFile = fopen(name, "wb"); // create a binary file
            unsigned* header = new unsigned[PAGE_SIZE/sizeof(unsigned)];
            for(int i = 0 ; i < PAGE_SIZE/sizeof(unsigned); i++){ header[i] = 0; }
            std::fwrite(header, sizeof(unsigned), PAGE_SIZE/sizeof(unsigned), pFile);
            std::fwrite(header, sizeof(unsigned), PAGE_SIZE/sizeof(unsigned), pFile);
            std::fflush(pFile);
            delete[] header;
            fclose(pFile);
            delete[] name;
            return SUCCESS;
        }
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        char * name = String_to_char_point(fileName);
        if (access(name, F_OK) == 0) { // check if file name exists
            remove(name); // delete file
            delete[] name;
            return SUCCESS;
        } else {
            delete[] name;
            return RC_FILE_NAME_NOT_EXIST;
        }
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        char * name = String_to_char_point(fileName);
        if (access(name, F_OK) == 0) { //check if file name exists
            FILE * pFile = std::fopen(name, "wb+"); //open file as binary read update
            if (!pFile) {
                return RC_FILE_OPEN_FAIL;
            }
            std::fclose(pFile);
            fileHandle.generateHeader(name); // save file name to the fileHandle so fileHandle could open file by itself
            return SUCCESS;
        } else {
            delete [] name;
            return RC_FILE_NAME_NOT_EXIST;
        }
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        return fileHandle.closeFile();
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        numberOfPages = 0;
        savedFileName = NULL;
    }

    FileHandle::~FileHandle() = default;

    void FileHandle::generateHeader(char* fileName) {
        savedFileName = fileName;
        getHeader(); // get the header from the existing file
    }

    void FileHandle::configureHeader(unsigned * header){
        unsigned * tempPointer = header;
        tempPointer[0] = readPageCounter;
        tempPointer[1] = writePageCounter;
        tempPointer[2] = appendPageCounter;
        tempPointer[3] = numberOfPages;
        for(int i = 4; i < PAGE_SIZE/sizeof(unsigned); i++){
            tempPointer[i] = 0;
        }
    }

    void FileHandle::getHeader(){
        FILE* filePointer = std::fopen(savedFileName,"rb");
        std::fscanf(filePointer,"%u", &readPageCounter);
        std::fscanf(filePointer,"%u", &writePageCounter);
        std::fscanf(filePointer,"%u", &appendPageCounter);
        std::fscanf(filePointer,"%u", &numberOfPages);
        std::fclose(filePointer);
    }

    void FileHandle::saveHeader(){
        unsigned * header = new unsigned[PAGE_SIZE/sizeof(unsigned)];
        configureHeader(header); // save the header with the current Counters
        FILE* filePointer = std::fopen(savedFileName,"rb");
        std::fwrite(header, sizeof(unsigned), PAGE_SIZE, filePointer);
        std::fclose(filePointer);
        delete[] header;
    }

    RC FileHandle::closeFile() {
        saveHeader();
        delete[] savedFileName;
        savedFileName = NULL;
        return SUCCESS;
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        readPageCounter++;
        if (pageNum > numberOfPages){ // check if the page number is invalid
            return RC_READ_NONEXISTENT_PAGE;
        } else if (pageNum < 0) {
            return RC_READ_NONEXISTENT_PAGE;
        }

        FILE * filePointer = std::fopen(savedFileName, "rb");
        std::fseek(filePointer, PAGE_SIZE*(pageNum+1), SEEK_SET); // move file pointer to the start of page
        std::fread(data, sizeof(char), PAGE_SIZE, filePointer); // read the data from start of page to next page
        std::fclose(filePointer);
        saveHeader();
        return SUCCESS;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if (pageNum > numberOfPages){
            return RC_WRITE_NONEXISTENT_PAGE;
        } else if (pageNum < 0) {
            return RC_WRITE_NONEXISTENT_PAGE;
        }
        writePageCounter++;
        FILE * filePointer = std::fopen(savedFileName, "wb");
        std::fseek(filePointer, PAGE_SIZE*(pageNum+1), SEEK_SET); // move file pointer to the start of page
        std::fwrite(data, 1, PAGE_SIZE, filePointer); //overwrite data
        std::fclose(filePointer);
        saveHeader();
        return SUCCESS;
    }

    RC FileHandle::appendPage(const void *data) {
        appendPageCounter++;
        FILE * filePointer = std::fopen(savedFileName, "wb"); // append at the end of the file.
        std::fseek(filePointer, PAGE_SIZE*(numberOfPages+1), SEEK_SET);
        numberOfPages++;
        std::fwrite(data, 1, PAGE_SIZE, filePointer); // write a new data
        std::fclose(filePointer);
        saveHeader();
        return SUCCESS;
    }

    unsigned FileHandle::getNumberOfPages() {
        return numberOfPages;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        getHeader();
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        saveHeader();
        return SUCCESS;
    }

} // namespace PeterDB