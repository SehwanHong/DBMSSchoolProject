#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan
#define RC_OPEN_SAME_INDEX_FILE 50
#define RC_IX_LEAF_ERROR 70;
#define RC_DELETE_NON_EXISTING_ENTRY 80
#define NUMRECORD 2
#define LEAFNODE 3

namespace PeterDB {
    class IX_ScanIterator;

    class IXFileHandle;

    class IndexManager {

    public:
        static IndexManager &instance();

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        RC insertFirstEntry(void * data, void * modifiedKey, unsigned & totalSize, bool LeafNode);

        RC insertLeafEntry(char** selectedPages, IXFileHandle &ixFileHandle,
                           const Attribute &attribute, const void * key, const RID &rid,
                           std::vector<int> &pageRead, std::vector<int> &slotUsed,
                           int &numberOfSelectedPages);

        RC getLeafNode(IXFileHandle &ixFileHandle, const Attribute &attribute,
                       std::vector<int> &pageRead, std::vector<int> &slotUsed,
                       void * data, const void * key, const RID &rid,
                       char** selectedPages, int &numberOfSelectedPage, unsigned &rootPage);

        int compareInt(void * data, unsigned offset, const void * key, const RID &rid);
        int compareReal(void * data, unsigned offset, const void * key, const RID &rid);
        int compareVarChar(void * data, unsigned offset, const void * key, const RID &rid);

        int getLengthOfKey(const Attribute & attribute, const void *key);
        void getModifiedKey(const void * key, void* modifiedData, const RID & rid, int lenOfKey);

        RC splitHalf(void * originalData, void * newData, unsigned originalPage, unsigned newPage);

        RC shift(void * data, int offset, int slotToUse, const void * modifiedKey, int totalSize);

        RC shiftMiddleNode(void * data, int offset, int slotToUse, const void * modifiedKey, int totalSize);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        RC deleteLeafEntry(char** selectedPages, IXFileHandle &ixFileHandle,
                          const Attribute &attribute, const void * key, const RID &rid,
                          std::vector<int> &pageRead, std::vector<int> &slotUsed,
                          int &numberOfSelectedPages);

        RC deleteLeafEntryOnly(void * data, int offset, int slotToUse, const void * modifiedKey, int totalSize);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;

        RC createRootNode(void *data);
        RC createLeafNode(void *data);

        RC printBTreeLeaf(void * leafNode, const Attribute &attribute, std::ostream &out) const;
        RC printBTreeRoot(void * rootNode, IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;

    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

    };

    class IX_ScanIterator {
    public:

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        RC open(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *lowKey, const void * highKey, bool lowKeyInclusive, bool highKeyInclusive);

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();

        RC getStartPage();
        int compareInt(int offset);
        int compareReal(int offset);
        int compareVarChar(int offset);

        int compareIntHigh(int offset);
        int compareRealHigh(int offset);
        int compareVarCharHigh(int offset);

        char * low;
        char * high;
        bool lowIn;
        bool highIn;
        IXFileHandle* storedIXFileHandle;
        Attribute storedAttribute;
        char * data;
        unsigned currentPage;
        unsigned currentSlot;
        unsigned prev_numberOfRecord;
    };

    class IXFileHandle {
    public:

        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);


        RC openFile(const std::string &fileName);
        void configureHeader(void * header);                                // configure Header to save to file
        RC saveHeader();                                                    // Save current number of pages, read write append count
        RC closeFile();                                                     // free saved file name and store current values
        unsigned getNumberOfPages();                                        // Get the number of pages in the file
        unsigned setRootNode(unsigned rootPage);
        unsigned getHeight();


        unsigned numberOfPages;
        unsigned filesize;
        unsigned filesizecheck();

        void getHeader();

        RC readPage(PageNum pageNum, void *data);                           // Get a specific page
        RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
        RC appendPage(const void *data);                                    // Append a specific page


        char * savedFileName;                                               // Pointer to the file
        FILE * filePointer;
        unsigned rootNode;
        unsigned height;

    };
}// namespace PeterDB
#endif // _ix_h_
