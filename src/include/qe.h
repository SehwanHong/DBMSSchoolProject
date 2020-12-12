#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <string>
#include <unordered_map>
#include <cfloat>

#include <sstream>

#include "rm.h"
#include "ix.h"


namespace PeterDB {

#define QE_EOF (-1)  // end of the index scan
    typedef enum AggregateOp {
        MIN = 0, MAX, COUNT, SUM, AVG
    } AggregateOp;

    // The following functions use the following
    // format for the passed data.
    //    For INT and REAL: use 4 bytes
    //    For VARCHAR: use 4 bytes for the length followed by the characters

    typedef struct Value {
        AttrType type;          // type of value
        void *data;             // value
    } Value;

    typedef struct Condition {
        std::string lhsAttr;        // left-hand side attribute
        CompOp op;                  // comparison operator
        bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
        std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
        Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
    } Condition;

    class Iterator {
        // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;

        virtual RC getAttributes(std::vector<Attribute> &attrs) const = 0;

        virtual ~Iterator() = default;
    };

    class TableScan : public Iterator {
        // A wrapper inheriting Iterator over RM_ScanIterator
    private:
        RelationManager &rm;
        RM_ScanIterator iter;
        std::string tableName;
        std::vector<Attribute> attrs;
        std::vector<std::string> attrNames;
        RID rid;
    public:
        TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL) : rm(rm) {
            //Set members
            this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            for (const Attribute &attr : attrs) {
                // convert to char *
                attrNames.push_back(attr.name);
            }

            // Call RM scan to get an iterator
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator() {
            iter.close();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);
        };

        RC getNextTuple(void *data) override {
            return iter.getNextTuple(rid, data);
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
        };

        ~TableScan() override {
            iter.close();
        };
    };

    class IndexScan : public Iterator {
        // A wrapper inheriting Iterator over IX_IndexScan
    private:
        RelationManager &rm;
        RM_IndexScanIterator iter;
        std::string tableName;
        std::string attrName;
        std::vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;
    public:
        IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName,
                  const char *alias = NULL) : rm(rm) {
            // Set members
            this->tableName = tableName;
            this->attrName = attrName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
            iter.close();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, iter);
        };

        RC getNextTuple(void *data) override {
            RC rc = iter.getNextEntry(rid, key);
            if (rc == 0) {
                rc = rm.readTuple(tableName, rid, data);
            }
            return rc;
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;


            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
        };

        ~IndexScan() override {
            iter.close();
        };
    };

    class Filter : public Iterator {
        Iterator * iter;
        Condition filterCondition;
        std::vector<Attribute> Attributes;

        // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );

        ~Filter() override;

        RC getNextTuple(void *data) override;

        bool checkPassTest(void * data);

        bool compareInteger(int integer);
        bool compareFloat(float realNumber);
        bool compareString(char *string, unsigned number_of_char);

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class Project : public Iterator {
        Iterator * iter;
        std::vector<std::string> projectAttribute;
        std::vector<Attribute> Attributes;
        // Projection operator
    public:
        Project(Iterator *input,                                // Iterator of input R
                const std::vector<std::string> &attrNames);     // std::vector containing attribute names
        ~Project() override;

        RC getNextTuple(void *data) override;

        RC selectAttribute(void * tuple, void *data);

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class BNLJoin : public Iterator {
        Iterator * left;
        TableScan * right;
        Condition joinCondition;
        unsigned numberOfPages;
        char * pageBlock;
        char * rightTuple;
        char * leftTuple;
        std::vector<Attribute> leftAttribute;
        std::vector<Attribute> rightAttribute;
        std::vector<Attribute> joinAttribute;
        int leftEndOfFile;
        std::unordered_map<std::string, std::vector<Value>> blockHash;
        AttrType attrType;
        int leftIndex;
        int rightIndex;
        std::vector<Value> leftValues;
        // Block nested-loop join operator
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
                TableScan *rightIn,           // TableScan Iterator of input S
                const Condition &condition,   // Join condition
                const unsigned numPages       // # of pages that can be loaded into memory,
                //   i.e., memory block size (decided by the optimizer)
        );

        ~BNLJoin() override;

        RC getNextTuple(void *data) override;

        RC updatePageBlock();
        int getMaxLength();
        int getDataSize();
        RC getLeftKey(std::string & Key);
        RC getRightKey(std::string & Key);

        RC setLeftValues();
        bool compareInt(int leftInteger, int rightInteger);
        bool compareFloat(float leftFloat, float rightFloat);
        bool compareVarChar(char * leftString, char* rightString, int leftNumChar, int rightNumChar);

        bool passJoinTest(Value &leftValue);
        RC joinTuple(void * leftValue, void * data);
        RC getAttrType();

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class INLJoin : public Iterator {
        Iterator * left;
        IndexScan * right;
        Condition joinCondition;
        char * rightTuple;
        char * leftTuple;
        std::vector<Attribute> leftAttribute;
        std::vector<Attribute> rightAttribute;
        std::vector<Attribute> joinAttribute;
        int leftEndOfFile;
        int rightEndOfFile;

        // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
                IndexScan *rightIn,          // IndexScan Iterator of input S
                const Condition &condition   // Join condition
        );

        ~INLJoin() override;

        RC getNextTuple(void *data) override;

        bool passJoinTest();
        RC joinTuple(void * data);
        int getLeftKey();
        int getRightKey();

        bool compareInt(int leftInteger, int rightInteger);
        bool compareFloat(float leftFloat, float rightFloat);
        bool compareVarChar(char * leftString, char* rightString, int leftNumChar, int rightNumChar);

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    // 10 extra-credit points
    class GHJoin : public Iterator {
        std::vector<std::string> leftPartition;
        std::vector<std::string> rightPartition;
        std::vector<Attribute> joinAttribute;
        std::vector<Attribute> leftAttribute;
        std::vector<Attribute> rightAttribute;
        Condition joinCondition;
        int numberPartition;

        TableScan * leftIter;
        TableScan * rightIter;
        BNLJoin * bnlJoin;

        int count;

        bool created;
        int bnlEndOfFile;
        int callCount = -1;
        // Grace hash join operator
    public:

        RelationManager &relationManager = RelationManager::instance();

        GHJoin(Iterator *leftIn,               // Iterator of input R
               Iterator *rightIn,               // Iterator of input S
               const Condition &condition,      // Join condition (CompOp is always EQ)
               const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
        );

        ~GHJoin() override;

        RC getNextTuple(void *data) override;

        RC buildPartition(Iterator * iter, bool isLeft, std::vector<std::string> &fileName);

        int getValue(void * record, std::vector<Attribute> & recordDescriptor, std::string & attributeName);

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class Aggregate : public Iterator {
        bool isGroupBy;
        AggregateOp aggregateOp;
        Iterator * iter;
        Attribute aggregateAttribute;
        Attribute groupAttribute;
        std::vector<Attribute> aggregateAttributes;
        char * tuple;
        bool endOfFile;
        std::vector<Attribute> iterAttribute;
        std::vector<std::string> keys;
        std::unordered_map<std::string, float> aggregatedValueMap;
        // Aggregation operator
    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
                  const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );

        ~Aggregate() override;

        RC getNextTuple(void *data) override;

        RC getValue(void * tuple, float & value);

        RC getKey(void * tuple, std::string &Key);

        RC aggregateNoGroup();

        RC aggregateGroup();

        float noGroupAvg();
        float noGroupCount();
        float noGroupMax();
        float noGroupMin();
        float noGroupSum();

        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrName = "MAX(rel.attr)"
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };
} // namespace PeterDB

#endif // _qe_h_
