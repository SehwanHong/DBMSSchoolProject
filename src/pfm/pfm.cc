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
        std::copy(str.begin(), str.end(), pointer);
        pointer[len] = '\0';
        return pointer;
    }

    RC PagedFileManager::createFileHelper(char *fileName) {
        FILE * pFile = std::fopen(fileName, "wb"); // create a binary file
        unsigned* header = new unsigned[PAGE_SIZE/UNSIGNEDSIZE];
        for(int i = 0 ; i < 4; i++){ header[i] = 0; } // set first four values to zero read write append pages count
        for(int i = 4 ; i < PAGE_SIZE/UNSIGNEDSIZE; i++){ header[i] = -1; } // else save -1 as hidden
        std::fwrite(header, UNSIGNEDSIZE, PAGE_SIZE/UNSIGNEDSIZE, pFile);
        delete[] header;
        std::fclose(pFile);

        pFile = std::fopen(fileName, "rb"); // check if file is save correctly with correct Page_size
        std::fseek(pFile, 0, SEEK_END);
        unsigned length = std::ftell(pFile);
        if (length != PAGE_SIZE) {
            return RC_FILE_SIZE_ERROR;
        }
        std::fclose(pFile);
    }

    RC PagedFileManager::createFile(const std::string &fileName) {
        char * name = String_to_char_point(fileName);
        if (access(name, F_OK) == 0) { // check if the file name exists
            delete[] name;
            return RC_FILE_NAME_EXIST;
        } else {
            createFileHelper(name);
            delete[] name;
            return SUCCESS;
        }
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        char * name = String_to_char_point(fileName);
        if (access(name, F_OK) == 0) { // check if file name exists

            FILE* pFile = std::fopen(name, "rb");
            std::fseek(pFile, 0, SEEK_END);  // check if file is save correctly with correct Page_size
            unsigned length = std::ftell(pFile);
            if (length < PAGE_SIZE) {
                return RC_FILE_SIZE_ERROR;
            }
            std::fclose(pFile);
            std::remove(name); // delete file

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
            delete [] name;
            return fileHandle.generateHeader(fileName); // save file name to the fileHandle so fileHandle could open file by itself
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
        filesize = 0;
        savedFileName = NULL;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::generateHeader(const std::string &fileName) { // get string of file name and save as pointer to filename
        int len = fileName.size();
        savedFileName  = new char[len+1];
        std::copy(fileName.begin(), fileName.end(), savedFileName);
        savedFileName[len] = '\0';
        filePointer = std::fopen(savedFileName, "rb+");
        getHeader();
        filesizecheck();
        unsigned temp = filesize;
        if (temp != (numberOfPages+1)*PAGE_SIZE){
            return RC_FILE_SIZE_ERROR;
        } // get the header from the existing file
        return SUCCESS;
    }

    void FileHandle::configureHeader(unsigned * header){  // store read write append page count to header
        unsigned * tempPointer = header;
        tempPointer[0] = readPageCounter;
        tempPointer[1] = writePageCounter;
        tempPointer[2] = appendPageCounter;
        tempPointer[3] = numberOfPages;
        for(int i = 4; i < PAGE_SIZE/UNSIGNEDSIZE; i++){
            tempPointer[i] = -1;
        }
    }

    void FileHandle::getHeader(){   // get header from the file's hidden page
        unsigned * header = new unsigned[PAGE_SIZE/UNSIGNEDSIZE];

        std::fseek(filePointer, 0L, SEEK_SET);
        std::fread(header, UNSIGNEDSIZE,PAGE_SIZE/UNSIGNEDSIZE,filePointer);

        std::fseek(filePointer, 0L, SEEK_SET);
        readPageCounter = header[0]+1;
        writePageCounter = header[1];
        appendPageCounter = header[2];
        numberOfPages = header[3];
        delete[] header;
    }

    RC FileHandle::saveHeader(){ // save the header to the file
        writePageCounter++;
        unsigned * header = new unsigned[PAGE_SIZE/UNSIGNEDSIZE];
        configureHeader(header); // save the header with the current Counters

        std::fseek(filePointer, 0L, SEEK_SET);
        std::fwrite(header, UNSIGNEDSIZE, PAGE_SIZE/UNSIGNEDSIZE, filePointer);
        delete[] header;

        filesizecheck();
        unsigned temp = filesize;
        if (temp != (numberOfPages+1)*PAGE_SIZE){  // check if the file size have changed
            return RC_FILE_SIZE_ERROR;
        }
        return SUCCESS;
    }

    RC FileHandle::closeFile() {
        saveHeader();
        std::fclose(filePointer);
        delete[] savedFileName;
        savedFileName = NULL;
        return SUCCESS;
    }

    unsigned FileHandle::filesizecheck() { // check the size of the file
        std::fseek(filePointer, 0L, SEEK_END);
        filesize = std::ftell(filePointer);
        std::fseek(filePointer, 0L, SEEK_SET);
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if (pageNum > numberOfPages){ // check if the page number is invalid
            return RC_READ_NONEXISTENT_PAGE;
        } else if (pageNum < 0) {
            return RC_READ_NONEXISTENT_PAGE;
        }
        readPageCounter++;

        filesizecheck();
        unsigned temp = filesize;
        if (temp != (numberOfPages+1)*PAGE_SIZE){ // check if the file size is correct
            return RC_FILE_SIZE_ERROR;
        }


        std::fseek(filePointer, PAGE_SIZE*(pageNum+1), SEEK_SET); // move file pointer to the start of page
        std::fread(data, 1, PAGE_SIZE, filePointer); // read the data from start of page to next page
        std::fseek(filePointer, 0L, SEEK_SET);

        filesizecheck();
        if(temp != filesize) { // check if filesize have changed with the write move
            return RC_FILE_SIZE_ERROR;
        }
        if( (readPageCounter + appendPageCounter + writePageCounter) % 50 == 0 ) {
            saveHeader();
        }
        return SUCCESS;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if (pageNum > numberOfPages){ // check if the page Number is correct
            return RC_WRITE_NONEXISTENT_PAGE;
        } else if (pageNum < 0) {
            return RC_WRITE_NONEXISTENT_PAGE;
        }
        writePageCounter++;
        filesizecheck();
        unsigned temp = filesize;
        if (temp != (numberOfPages+1)*PAGE_SIZE){
            return RC_FILE_SIZE_ERROR;
        }


        std::fseek(filePointer, PAGE_SIZE*(pageNum+1), SEEK_SET); // move file pointer to the start of page
        std::fwrite(data, 1, PAGE_SIZE, filePointer); //overwrite data
        std::fseek(filePointer, 0L, SEEK_SET);

        filesizecheck();
        if(temp != filesize) {
            return RC_FILE_SIZE_ERROR;
        }
        if( (readPageCounter + appendPageCounter + writePageCounter) % 50 == 0 ) {
            saveHeader();
        }
        return SUCCESS;
    }

    RC FileHandle::appendPage(const void *data) {
        appendPageCounter++;
        filesizecheck();
        unsigned temp = filesize;
        if (temp != (numberOfPages+1)*PAGE_SIZE){  // check file size before appending a page
            return RC_FILE_SIZE_ERROR;
        }
        numberOfPages++;

        std::fseek(filePointer, 0L, SEEK_END);
        std::fwrite(data, 1, PAGE_SIZE, filePointer); // write a new data
        std::fseek(filePointer, 0L, SEEK_SET);

        filesizecheck();
        if(temp+PAGE_SIZE != filesize) { // check if file size have increased with factor of PAGE_SIZE
            return RC_FILE_SIZE_ERROR;
        }
        if( (readPageCounter + appendPageCounter + writePageCounter) % 50 == 0 ) {
            saveHeader();
        }
        return SUCCESS;
    }

    unsigned FileHandle::getNumberOfPages() {
        return numberOfPages;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return SUCCESS;
    }

} // namespace PeterDB