#ifndef _pfm_h_
#define _pfm_h_

#define PAGE_SIZE 4096

#define SUCCESS 0
#define RC_FILE_NAME_EXIST 1
#define RC_FILE_NAME_NOT_EXIST 2
#define RC_FILE_SIZE_ERROR 3
#define RC_READ_NONEXISTENT_PAGE 4
#define RC_WRITE_NONEXISTENT_PAGE 5

#define UNSIGNEDSIZE sizeof(unsigned)

#include <string>
#include <unistd.h>
#include <stdio.h>

namespace PeterDB {

    typedef unsigned PageNum;
    typedef int RC;

    class FileHandle;

    class PagedFileManager {
    public:
        static PagedFileManager &instance();                                // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new file
        RC destroyFile(const std::string &fileName);                        // Destroy a file
        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file
        RC closeFile(FileHandle &fileHandle);                               // Close a file
        RC createFileHelper(char * fileName);

    protected:
        PagedFileManager();                                                 // Prevent construction
        ~PagedFileManager();                                                // Prevent unwanted destruction
        PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
        PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment

        char* String_to_char_point(const std::string & str);                // change String to char * for different functions

    };

    class FileHandle {
    public:
        // variables to keep the counter for each operation
        unsigned readPageCounter;
        unsigned writePageCounter;
        unsigned appendPageCounter;
        unsigned numberOfPages;

        FileHandle();                                                       // Default constructor
        ~FileHandle();                                                      // Destructor

        RC generateHeader(const std::string &fileName);                     // Function that store Pointer to the file
        void configureHeader(unsigned * header);                            // configure Header to save to file
        void getHeader();                                                   // get Header from existing file
        RC saveHeader();                                                    // Save current number of pages, read write append count
        RC closeFile();                                                     // free saved file name and store current values

        unsigned filesize;
        unsigned filesizecheck();

        RC readPage(PageNum pageNum, void *data);                           // Get a specific page
        RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
        RC appendPage(const void *data);                                    // Append a specific page
        unsigned getNumberOfPages();                                        // Get the number of pages in the file
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                                unsigned &appendPageCount);                 // Put current counter values into variables

        char * savedFileName;                                               // Pointer to the file
        FILE * filePointer;
    };

} // namespace PeterDB

#endif // _pfm_h_