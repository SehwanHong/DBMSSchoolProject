#include "src/include/qe.h"

namespace PeterDB {
    Filter::Filter(Iterator *input, const Condition &condition) {
        iter = input;
        filterCondition = condition;
        iter->getAttributes(Attributes);
    }

    Filter::~Filter() {
    }

    RC Filter::getNextTuple(void *data) {
        int problem = SUCCESS;
        while( iter->getNextTuple(data) != RM_EOF ) {
            bool isPass = checkPassTest(data);
            if (isPass) {
                return SUCCESS;
            }
        }
        return QE_EOF;
    }

    bool Filter::checkPassTest(void *data) {
        unsigned short numOfAttribute = Attributes.size();
        unsigned short nullBytes = numOfAttribute / 8 + (numOfAttribute % 8 == 0 ? 0 : 1);

        unsigned char * null_indicator = new unsigned char[nullBytes];
        unsigned offset = 0;
        unsigned char * temp = (unsigned char*) data;
        bool nullBit = false;
        bool result = false;
        memcpy(null_indicator, temp+offset, nullBytes);
        offset += nullBytes;
        for(int i = 0 ; i < numOfAttribute ; i++ ){
            unsigned char mask = (unsigned)1 << (unsigned) (7 - i % 8);
            nullBit = null_indicator[i/8] & mask;
            if (!nullBit){
                if (filterCondition.op == NO_OP || Attributes.at(i).name == filterCondition.lhsAttr) {
                    unsigned int number_of_char = 0;
                    switch (Attributes.at(i).type) {
                        case AttrType::TypeInt: {
                            int integer = *((int *)((char*)data+offset));
                            result = compareInteger(integer);
                            break;
                        }
                        case AttrType::TypeReal: {
                            float realNumber = *((float *)((char*)data+offset));
                            result = compareFloat(realNumber);
                            break;
                        }
                        case AttrType::TypeVarChar: {
                            number_of_char = *((unsigned int * )((char*)data+offset));
                            char* string = new char[number_of_char];
                            memcpy(string, (char*) data + offset + INTSIZE, number_of_char);
                            result = compareString(string, number_of_char);
                            delete[] string;
                            break;
                        }
                    }
                    delete[] null_indicator;
                    return result;
                }
                switch (Attributes.at(i).type) {
                    case AttrType::TypeInt: {
                        offset += INTSIZE; break;
                    }
                    case AttrType::TypeReal: {
                        offset += FLOATSIZE; break;
                    }
                    case AttrType::TypeVarChar: {
                        unsigned int number_of_char =  *((unsigned int * )((char*)data+offset));
                        offset += INTSIZE;
                        offset += number_of_char;
                    }
                }
            }
        }
        delete[] null_indicator;
        return result;
    }

    bool Filter::compareInteger(int integer){
        int compVal =  *((int *) filterCondition.rhsValue.data);
        switch (filterCondition.op) {
            case CompOp::EQ_OP : {
                return integer == compVal;
            }
            case CompOp::GE_OP : {
                return integer >= compVal;
            }
            case CompOp::GT_OP : {
                return integer > compVal;
            }
            case CompOp::LE_OP : {
                return integer <= compVal;
            }
            case CompOp::LT_OP : {
                return integer < compVal;
            }
            case CompOp::NE_OP : {
                return integer != compVal;
            }
            case CompOp::NO_OP : {
                return true;
            }
        }
    }

    bool Filter::compareFloat(float realNumber) {
        float compVal = *((float *) filterCondition.rhsValue.data);
        switch (filterCondition.op) {
            case CompOp::EQ_OP : {
                return realNumber == compVal;
            }
            case CompOp::GE_OP : {
                return realNumber >= compVal;
            }
            case CompOp::GT_OP : {
                return realNumber > compVal;
            }
            case CompOp::LE_OP : {
                return realNumber <= compVal;
            }
            case CompOp::LT_OP : {
                return realNumber < compVal;
            }
            case CompOp::NE_OP : {
                return realNumber != compVal;
            }
            case CompOp::NO_OP : {
                return true;
            }
        }
    }

    bool Filter::compareString(char *string, unsigned number_of_char) {
        int max = 0;
        if(number_of_char > *((int*)filterCondition.rhsValue.data)) {
            max = number_of_char;
        } else {
            max = *((int*)filterCondition.rhsValue.data);
        }
        switch (filterCondition.op) {
            case CompOp::EQ_OP : {
                bool result = memcmp(string, (char*)filterCondition.rhsValue.data + INTSIZE, max) == 0;
                return result;
            }
            case CompOp::GE_OP : {
                bool result = memcmp(string, (char*)filterCondition.rhsValue.data + INTSIZE, max) >= 0;
                return result;
            }
            case CompOp::GT_OP : {
                bool result = memcmp(string, (char*)filterCondition.rhsValue.data + INTSIZE, max) > 0;
                return result;
            }
            case CompOp::LE_OP : {
                bool result = memcmp(string, (char*)filterCondition.rhsValue.data + INTSIZE, max) <= 0;
                return result;
            }
            case CompOp::LT_OP : {
                bool result = memcmp(string, (char*)filterCondition.rhsValue.data + INTSIZE, max) < 0;
                return result;
            }
            case CompOp::NE_OP : {
                bool result = memcmp(string, (char*)filterCondition.rhsValue.data + INTSIZE, max) != 0;
                return result;
            }
            case CompOp::NO_OP : {
                return true;
            }
        }
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        int problem = SUCCESS;
        problem += iter->getAttributes(attrs);
        return SUCCESS;
    }


    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
        iter = input;
        projectAttribute.insert(projectAttribute.begin(), attrNames.begin(), attrNames.end());
        iter->getAttributes(Attributes);
    }

    Project::~Project() {
    }

    RC Project::getNextTuple(void *data) {
        int problem = SUCCESS;
        char * tuple = new char[PAGE_SIZE];
        while(iter->getNextTuple(tuple) != RM_EOF) {
            problem += selectAttribute(tuple, data);
            delete[] tuple;
            return SUCCESS;
        }
        delete[] tuple;
        return QE_EOF;
    }

    RC Project::selectAttribute(void * tuple, void *data) {
        unsigned short dataAttributeLength = projectAttribute.size();
        unsigned short dataNullBytes = dataAttributeLength / 8 + (dataAttributeLength % 8 == 0 ? 0 : 1);

        unsigned short recordAttrubuteLength = Attributes.size();
        unsigned short recordNullBytes = recordAttrubuteLength / 8 + (recordAttrubuteLength % 8 == 0 ? 0 : 1);
        unsigned char * recordNullIndicator = new unsigned char[recordNullBytes];
        unsigned char * dataNullIndicator = new unsigned char[dataNullBytes];

        for(int i = 0; i < dataNullBytes; i++) {dataNullIndicator[i] = 0;}

        unsigned recordOffSet = 0;
        unsigned dataOffSet = 0;
        unsigned short scanAttrIndex = 0;
        bool nullBit = false;

        memcpy(recordNullIndicator, (char*)tuple+recordOffSet, recordNullBytes);

        dataOffSet += dataNullBytes;
        for( ; scanAttrIndex < dataAttributeLength ; scanAttrIndex++) {
            recordOffSet = 0;
            nullBit = false;
            recordOffSet += recordNullBytes;
            for (int i = 0; i < recordAttrubuteLength; i++) {
                unsigned char mask = (unsigned) 1 << (unsigned) (7 - i % 8);
                nullBit = recordNullIndicator[i / 8] & mask;

                if (projectAttribute.at(scanAttrIndex) == Attributes.at(i).name) {
                    unsigned char *saveAttribute = (unsigned char *) data;
                    if (nullBit) {
                        unsigned char newMask = (unsigned) 1 << (unsigned) (7 - scanAttrIndex % 8);
                        unsigned nullplace = scanAttrIndex / 8;
                        dataNullIndicator[nullplace] += newMask;
                    } else {
                        switch (Attributes.at(i).type) {
                            case AttrType::TypeInt: {
                                memcpy((char *) data + dataOffSet, (char *) tuple + recordOffSet, INTSIZE);
                                dataOffSet += INTSIZE;
                                break;
                            }
                            case AttrType::TypeReal: {
                                memcpy((char *) data + dataOffSet, (char *) tuple + recordOffSet, FLOATSIZE);
                                dataOffSet += FLOATSIZE;
                                break;
                            }
                            case AttrType::TypeVarChar: {
                                unsigned int number_of_char = *((unsigned int *) ((char *) tuple + recordOffSet));
                                memcpy((char *) data + dataOffSet, (char *) tuple + recordOffSet,
                                       number_of_char + INTSIZE);
                                dataOffSet += INTSIZE;
                                dataOffSet += number_of_char;
                            }
                        }
                    }
                    break;
                }

                if (!nullBit) {
                    switch (Attributes.at(i).type) {
                        case AttrType::TypeInt: {
                            recordOffSet += INTSIZE;
                            break;
                        }
                        case AttrType::TypeReal: {
                            recordOffSet += FLOATSIZE;
                            break;
                        }
                        case AttrType::TypeVarChar: {
                            unsigned int number_of_char = *((unsigned int *) ((char *) tuple + recordOffSet));
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

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.erase(attrs.begin(), attrs.end());
        for( int index = 0; index < projectAttribute.size(); index++) {
            for (int i = 0; i < Attributes.size(); i++) {
                if (Attributes.at(i).name == projectAttribute.at(index)) {
                    attrs.push_back(Attributes.at(i));
                    break;
                }
            }
        }
        return SUCCESS;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {
        left = leftIn;
        right = rightIn;
        joinCondition = condition;
        numberOfPages = numPages;
        pageBlock = new char[PAGE_SIZE*numPages];
        rightTuple = new char[PAGE_SIZE];
        leftTuple = new char[PAGE_SIZE];
        left->getAttributes(leftAttribute);
        right->getAttributes(rightAttribute);
        joinAttribute.insert(joinAttribute.end(),leftAttribute.begin(),leftAttribute.end());
        joinAttribute.insert(joinAttribute.end(),rightAttribute.begin(),rightAttribute.end());
        leftEndOfFile = 0;
        leftIndex = 0;
        rightIndex = 0;
        getAttrType();
        updatePageBlock();
    }

    BNLJoin::~BNLJoin() {
        delete[] leftTuple;
        delete[] rightTuple;
        delete[] pageBlock;
    }

    RC BNLJoin::getNextTuple(void *data) {
        int rightEndOfFile = 0;
        while (true) {
            int reason = SUCCESS;
            if (leftValues.empty()) {
                memset(rightTuple, 0, PAGE_SIZE);
                rightEndOfFile = right->getNextTuple(rightTuple);
                if (rightEndOfFile == RM_EOF && leftEndOfFile != RM_EOF) {
                    right->setIterator();
                    rightEndOfFile = right->getNextTuple(rightTuple);
                    memset(pageBlock, 0, PAGE_SIZE*numberOfPages);
                    updatePageBlock();
                } else if (rightEndOfFile == RM_EOF && leftEndOfFile == RM_EOF) {
                    return QE_EOF;
                }
                reason = setLeftValues();
            }
            Value leftValue;
            if (reason == SUCCESS) {
                bool pass = passJoinTest(leftValue);
                if (pass) {
                    return joinTuple(leftValue.data, data);
                }
            }
        }
    }

    RC BNLJoin::joinTuple(void * leftValue, void * data){
        unsigned length = joinAttribute.size();
        unsigned null_bytes = length / 8 + (length % 8 == 0 ? 0 : 1); // get number of bytes that indicate null

        char * null_indicator = new char[null_bytes];
        unsigned offset = 0;
        int index = 0;
        offset += null_bytes;

        for (int i = 0 ; i < null_bytes ; i++) {null_indicator[i] = 0;}

        int leftOffset = 0;
        int leftLength = leftAttribute.size();
        unsigned leftNullBytes = leftLength / 8 + (leftLength % 8 == 0 ? 0 : 1);
        char * leftNull = new char[leftNullBytes];
        memcpy(leftNull, leftValue, leftNullBytes);
        leftOffset += leftNullBytes;
        for(int i = 0; i < leftLength ; i++){
            unsigned char mask = (unsigned) 1 << (unsigned) (7 - i % 8);
            bool nullBit = leftNull[i / 8] & mask;

            if (nullBit) {
                unsigned char newMask = (unsigned) 1 << (unsigned) (7 - index % 8);
                unsigned nullplace = index / 8;
                null_indicator[nullplace] += newMask;
            } else {
                switch (leftAttribute.at(i).type) {
                    case AttrType::TypeInt: {
                        memcpy((char *) data + offset, (char *) leftValue + leftOffset, INTSIZE);
                        offset += INTSIZE;
                        leftOffset += INTSIZE;
                        break;
                    }
                    case AttrType::TypeReal: {
                        memcpy((char *) data + offset, (char *) leftValue + leftOffset, FLOATSIZE);
                        offset += FLOATSIZE;
                        leftOffset += FLOATSIZE;
                        break;
                    }
                    case AttrType::TypeVarChar: {
                        unsigned int number_of_char = *((unsigned int *) ((char *) leftValue + leftOffset));
                        memcpy((char *) data + offset, (char *) leftValue + leftOffset,
                               number_of_char + INTSIZE);
                        offset += INTSIZE;
                        offset += number_of_char;
                        leftOffset += INTSIZE;
                        leftOffset += number_of_char;
                    }
                }
            }
            index++;
        }

        int rightOffset = 0;
        int rightLength = rightAttribute.size();
        unsigned rightNullBytes = rightLength / 8 + (rightLength % 8 == 0 ? 0 : 1);
        char * rightNull = new char[rightNullBytes];
        memcpy(rightNull, rightTuple, rightNullBytes);
        rightOffset += rightNullBytes;
        for(int i = 0; i < rightLength ; i++){
            unsigned char mask = (unsigned) 1 << (unsigned) (7 - i % 8);
            bool nullBit = rightNull[i / 8] & mask;

            if (nullBit) {
                unsigned char newMask = (unsigned) 1 << (unsigned) (7 - index % 8);
                unsigned nullplace = index / 8;
                null_indicator[nullplace] += newMask;
            } else {
                switch (rightAttribute.at(i).type) {
                    case AttrType::TypeInt: {
                        memcpy((char *) data + offset, (char *) rightTuple + rightOffset, INTSIZE);
                        offset += INTSIZE;
                        rightOffset += INTSIZE;
                        break;
                    }
                    case AttrType::TypeReal: {
                        memcpy((char *) data + offset, (char *) rightTuple + rightOffset, FLOATSIZE);
                        offset += FLOATSIZE;
                        rightOffset += FLOATSIZE;
                        break;
                    }
                    case AttrType::TypeVarChar: {
                        unsigned int number_of_char = *((unsigned int *) ((char *) rightTuple + rightOffset));
                        memcpy((char *) data + offset, (char *) rightTuple + rightOffset,
                               number_of_char + INTSIZE);
                        offset += INTSIZE;
                        offset += number_of_char;
                        rightOffset += INTSIZE;
                        rightOffset += number_of_char;
                    }
                }
            }
            index++;
        }

        if( index == length ) {
            memcpy(data, null_indicator, null_bytes);
            delete[] null_indicator;
            delete[] leftNull;
            delete[] rightNull;
            return SUCCESS;
        } else {
            delete[] null_indicator;
            delete[] leftNull;
            delete[] rightNull;
            return 5000;
        }
    }

    bool BNLJoin::passJoinTest(Value &leftValue) {
        leftValue.type = leftValues.at(0).type;
        leftValue.data = leftValues.at(0).data;
        leftValues.erase(leftValues.begin());

        int leftOffset = leftAttribute.size() / 8 + (leftAttribute.size() % 8 == 0 ? 0 : 1);
        for (int i = 0; i < leftAttribute.size(); i++) {
            if (i == leftIndex) {
                break;
            }
            switch(leftAttribute.at(i).type) {
                case AttrType::TypeInt:
                    leftOffset += INTSIZE;
                    break;
                case AttrType::TypeReal:
                    leftOffset += FLOATSIZE;
                    break;
                case AttrType::TypeVarChar:
                    unsigned numChar = *(int*)((char*) leftValue.data + leftOffset);
                    leftOffset += INTSIZE;
                    leftOffset += numChar;
            }
        }

        int rightOffset = rightAttribute.size() / 8 + (rightAttribute.size() % 8 == 0 ? 0 : 1);
        for (int i = 0; i < rightAttribute.size(); i++) {
            if (i == rightIndex) {
                break;
            }
            switch(rightAttribute.at(i).type) {
                case AttrType::TypeInt:
                    rightOffset += INTSIZE;
                    break;
                case AttrType::TypeReal:
                    rightOffset += FLOATSIZE;
                    break;
                case AttrType::TypeVarChar:
                    unsigned numChar = *(int*)(rightTuple + rightOffset);
                    rightOffset += INTSIZE;
                    rightOffset += numChar;
            }
        }

        bool result = false;
        switch(attrType) {
            case AttrType::TypeInt: {
                int leftInt = *(int *) ((char *) leftValue.data + leftOffset);
                int rightInt = *(int *) (rightTuple + rightOffset);
                result = compareInt(leftInt, rightInt);
                break;
            }
            case AttrType::TypeReal: {
                float leftFloat = *(float *) ((char *) leftValue.data + leftOffset);
                float rightFloat = *(float *) (rightTuple + rightOffset);
                result = compareFloat(leftFloat, rightFloat);
                break;
            }
            case AttrType::TypeVarChar: {
                int leftNumChar = *(int *) ((char *) leftValue.data + leftOffset);
                int rightNumChar = *(int *) (rightTuple + rightOffset);
                result = compareVarChar((char *) leftValue.data + leftOffset + INTSIZE,
                                        rightTuple + rightOffset + INTSIZE, leftNumChar, rightNumChar);
                break;
            }
        }
        return result;
    }

    RC BNLJoin::setLeftValues() {
        std::string rightKey;
        getRightKey(rightKey);
        try {
            leftValues = blockHash.at(rightKey);
            return SUCCESS;
        } catch (const std::out_of_range& oor) {
            return -1;
        }

    }

    RC BNLJoin::getAttrType() {
        for(int i = 0 ; i < leftAttribute.size(); i++) {
            if (leftAttribute.at(i).name == joinCondition.lhsAttr) {
                attrType = leftAttribute.at(i).type;
                leftIndex = i;
                break;
            }
        }
        for(int i = 0 ; i < rightAttribute.size(); i++) {
            if (rightAttribute.at(i).name == joinCondition.rhsAttr) {
                rightIndex = i;
                break;
            }
        }
    }

    RC BNLJoin::getDataSize() {
        // calculate how many bytes data is using
        unsigned length = leftAttribute.size();
        unsigned null_bytes = length / 8 + (length % 8 == 0 ? 0 : 1); // get number of bytes that indicate null

        // ceil (length of vector / 8 ) go through the data with the null value
        unsigned char * null_indicator = new unsigned char[null_bytes];
        unsigned offset = 0;
        bool nullBit = false;
        memcpy(null_indicator, leftTuple+offset, null_bytes);
        offset += null_bytes;
        for(int i = 0 ; i < length ; i++ ){
            unsigned char mask = (unsigned)1 << (unsigned) (7 - i % 8);
            nullBit = null_indicator[i/8] & mask;
            if (!nullBit){
                switch (leftAttribute[i].type) {
                    case AttrType::TypeInt:
                        offset += INTSIZE; break;
                    case AttrType::TypeReal:
                        offset += FLOATSIZE; break;
                    case AttrType::TypeVarChar:
                        unsigned int number_of_char =  *((unsigned int * )(leftTuple+offset));
                        offset += INTSIZE;
                        offset += number_of_char;
                }
            }
        }
        delete[] null_indicator;
        return offset;
    }

    RC BNLJoin::getLeftKey(std::string &Key) {
        // calculate how many bytes data is using
        unsigned length = leftAttribute.size();
        unsigned null_bytes = length / 8 + (length % 8 == 0 ? 0 : 1); // get number of bytes that indicate null

        // ceil (length of vector / 8 ) go through the data with the null value
        unsigned char * null_indicator = new unsigned char[null_bytes];
        unsigned offset = 0;
        unsigned char * temp = (unsigned char*) leftTuple;
        bool nullBit = false;
        memcpy(null_indicator, temp+offset, null_bytes);
        offset += null_bytes;
        for(int i = 0 ; i < length ; i++ ){
            unsigned char mask = (unsigned)1 << (unsigned) (7 - i % 8);
            nullBit = null_indicator[i/8] & mask;
            if (i == leftIndex) {
                switch (leftAttribute[i].type) {
                    case AttrType::TypeInt: {
                        int integer = *(int*)(temp + offset);
                        Key = std::to_string(integer);
                        break;
                    }
                    case AttrType::TypeReal: {
                        float realNumber = *(float *)( temp + offset);
                        Key = std::to_string(realNumber);
                        break;
                    }
                    case AttrType::TypeVarChar: {
                        unsigned int number_of_char = *((unsigned int *) (temp + offset));
                        char *attributeString = new char[number_of_char + 1];
                        memcpy(attributeString, temp + offset + INTSIZE, number_of_char);
                        attributeString[number_of_char] = '\0';
                        Key = attributeString;
                        delete[] attributeString;
                        break;
                    }
                }
                break;
            }
            if (!nullBit){
                switch (leftAttribute[i].type) {
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

    RC BNLJoin::getRightKey(std::string &Key) {
        // calculate how many bytes data is using
        unsigned length = leftAttribute.size();
        unsigned null_bytes = length / 8 + (length % 8 == 0 ? 0 : 1); // get number of bytes that indicate null

        // ceil (length of vector / 8 ) go through the data with the null value
        unsigned char * null_indicator = new unsigned char[null_bytes];
        unsigned offset = 0;
        unsigned char * temp = (unsigned char*) rightTuple;
        bool nullBit = false;
        memcpy(null_indicator, temp+offset, null_bytes);
        offset += null_bytes;
        for(int i = 0 ; i < length ; i++ ){
            unsigned char mask = (unsigned)1 << (unsigned) (7 - i % 8);
            nullBit = null_indicator[i/8] & mask;
            if (i == rightIndex) {
                switch (leftAttribute[i].type) {
                    case AttrType::TypeInt:
                        Key = std::to_string(*(int*)( temp + offset));
                        break;
                    case AttrType::TypeReal:
                        Key = std::to_string(*(float*)( temp + offset));
                        break;
                    case AttrType::TypeVarChar:
                        unsigned int number_of_char =  *((unsigned int * )(temp+offset));
                        char * attributeString = new char[number_of_char + 1];
                        memcpy(attributeString, temp+offset + INTSIZE, number_of_char);
                        attributeString[number_of_char] = '\0';
                        Key = attributeString;
                        delete[] attributeString;
                }
                break;
            }
            if (!nullBit){
                switch (rightAttribute[i].type) {
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

    RC BNLJoin::updatePageBlock() {
        int maxLength = getMaxLength();
        int freeSpace = PAGE_SIZE*numberOfPages;
        int offset = 0;

        RelationManager &relationManager = RelationManager::instance();

        while(freeSpace - offset > maxLength) {
            memset(leftTuple, 0, PAGE_SIZE);
            leftEndOfFile = left->getNextTuple(leftTuple);
            if(leftEndOfFile == RM_EOF) {
                break;
            }
            int dataSize = getDataSize();
            std::stringstream ss;
            relationManager.printTuple(leftAttribute, leftTuple, ss);
            std::string temp = ss.str();
            std::string key;
            getLeftKey(key);
            Value value;
            value.type = attrType;
            memcpy(pageBlock + offset, leftTuple, dataSize);
            value.data = pageBlock + offset;
            offset += dataSize;
            blockHash[key].push_back(value);
        }

        return SUCCESS;
    }

    int BNLJoin::getMaxLength() {
        int leftLength = leftAttribute.size();
        int length = leftLength / 8 + (leftLength % 8 == 0 ? 0 : 1);;
        for(int i = 0; i < leftLength; i++) {
            if (leftAttribute.at(i).type != AttrType::TypeVarChar) {
                length += leftAttribute.at(i).length;
            } else {
                length += leftAttribute.at(i).length;
                length += INTSIZE;
            }
        }
        return length;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.erase(attrs.begin(),attrs.end());
        attrs.insert(attrs.begin(), joinAttribute.begin(), joinAttribute.end());
        return SUCCESS;
    }

    bool BNLJoin::compareInt(int leftInteger, int rightInteger) {
        switch(joinCondition.op) {
            case CompOp::EQ_OP : {
                return leftInteger == rightInteger;
            }
            case CompOp::GE_OP : {
                return leftInteger >= rightInteger;
            }
            case CompOp::GT_OP : {
                return leftInteger > rightInteger;
            }
            case CompOp::LE_OP : {
                return leftInteger <= rightInteger;
            }
            case CompOp::LT_OP : {
                return leftInteger < rightInteger;
            }
            case CompOp::NE_OP : {
                return leftInteger != rightInteger;
            }
            case CompOp::NO_OP : {
                return true;
            }
        }
    }

    bool BNLJoin::compareFloat(float leftFloat, float rightFloat) {
        switch(joinCondition.op) {
            case CompOp::EQ_OP : {
                return leftFloat == rightFloat;
            }
            case CompOp::GE_OP : {
                return leftFloat >= rightFloat;
            }
            case CompOp::GT_OP : {
                return leftFloat > rightFloat;
            }
            case CompOp::LE_OP : {
                return leftFloat <= rightFloat;
            }
            case CompOp::LT_OP : {
                return leftFloat < rightFloat;
            }
            case CompOp::NE_OP : {
                return leftFloat != rightFloat;
            }
            case CompOp::NO_OP : {
                return true;
            }
        }
    }

    bool BNLJoin::compareVarChar(char *leftString, char *rightString, int leftNumChar, int rightNumChar) {
        int max = 0;
        if(leftNumChar > rightNumChar) {
            max = leftNumChar;
        } else {
            max = rightNumChar;
        }
        switch (joinCondition.op) {
            case CompOp::EQ_OP : {
                bool result = memcmp(leftString, rightString, max) == 0;
                return result;
            }
            case CompOp::GE_OP : {
                bool result = memcmp(leftString, rightString, max) >= 0;
                return result;
            }
            case CompOp::GT_OP : {
                bool result = memcmp(leftString, rightString, max) > 0;
                return result;
            }
            case CompOp::LE_OP : {
                bool result = memcmp(leftString, rightString, max) <= 0;
                return result;
            }
            case CompOp::LT_OP : {
                bool result = memcmp(leftString, rightString, max) < 0;
                return result;
            }
            case CompOp::NE_OP : {
                bool result = memcmp(leftString, rightString, max) != 0;
                return result;
            }
            case CompOp::NO_OP : {
                return true;
            }
        }
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
        left = leftIn;
        right = rightIn;
        joinCondition = condition;
        left->getAttributes(leftAttribute);
        right->getAttributes(rightAttribute);
        joinAttribute.insert(joinAttribute.end(), leftAttribute.begin(), leftAttribute.end());
        joinAttribute.insert(joinAttribute.end(), rightAttribute.begin(), rightAttribute.end());
        leftTuple = new char[PAGE_SIZE];
        rightTuple = new char[PAGE_SIZE];
        leftEndOfFile = 0;
        rightEndOfFile = RM_EOF;
    }

    INLJoin::~INLJoin() {
        delete[] leftTuple;
        delete[] rightTuple;
    }

    RC INLJoin::getNextTuple(void *data) {
        while (leftEndOfFile != RM_EOF) {
            if (rightEndOfFile == RM_EOF) {
                memset(leftTuple, 0, PAGE_SIZE);
                leftEndOfFile = left->getNextTuple(leftTuple);
                int keyOffset = getLeftKey();
                right->setIterator(leftTuple + keyOffset, leftTuple + keyOffset, true, true);
                rightEndOfFile = right->getNextTuple(rightTuple);
            } else {
                bool pass = passJoinTest();
                if (pass) {
                    joinTuple(data);
                    rightEndOfFile = right->getNextTuple(rightTuple);
                    return SUCCESS;
                } else {
                    rightEndOfFile = right->getNextTuple(rightTuple);
                }
            }
        }
        return QE_EOF;
    }

    int INLJoin::getLeftKey() {
        // calculate how many bytes data is using
        unsigned length = leftAttribute.size();
        unsigned null_bytes = length / 8 + (length % 8 == 0 ? 0 : 1); // get number of bytes that indicate null

        // ceil (length of vector / 8 ) go through the data with the null value
        unsigned char * null_indicator = new unsigned char[null_bytes];
        unsigned offset = 0;
        unsigned char * temp = (unsigned char*) leftTuple;
        bool nullBit = false;
        memcpy(null_indicator, temp+offset, null_bytes);
        offset += null_bytes;
        for(int i = 0 ; i < length ; i++ ){
            unsigned char mask = (unsigned)1 << (unsigned) (7 - i % 8);
            nullBit = null_indicator[i/8] & mask;
            if (leftAttribute[i].name == joinCondition.lhsAttr) {
                break;
            }
            if (!nullBit){
                switch (leftAttribute[i].type) {
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

    int INLJoin::getRightKey() {
        // calculate how many bytes data is using
        unsigned length = rightAttribute.size();
        unsigned null_bytes = length / 8 + (length % 8 == 0 ? 0 : 1); // get number of bytes that indicate null

        // ceil (length of vector / 8 ) go through the data with the null value
        unsigned char * null_indicator = new unsigned char[null_bytes];
        unsigned offset = 0;
        unsigned char * temp = (unsigned char*) rightTuple;
        bool nullBit = false;
        memcpy(null_indicator, temp+offset, null_bytes);
        offset += null_bytes;
        for(int i = 0 ; i < length ; i++ ){
            unsigned char mask = (unsigned)1 << (unsigned) (7 - i % 8);
            nullBit = null_indicator[i/8] & mask;
            if (rightAttribute[i].name == joinCondition.rhsAttr) {
                break;
            }
            if (!nullBit){
                switch (leftAttribute[i].type) {
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

    bool INLJoin::passJoinTest() {
        int rightOffset = getRightKey();
        int leftOffset = getLeftKey();
        AttrType type = joinCondition.rhsValue.type;
        bool result = false;
        switch(type) {
            case AttrType::TypeInt: {
                int leftInt = *(int *) ((char *)leftTuple + leftOffset);
                int rightInt = *(int *) (rightTuple + rightOffset);
                result = compareInt(leftInt, rightInt);
                break;
            }
            case AttrType::TypeReal: {
                float leftFloat = *(float *) ((char *) leftTuple + leftOffset);
                float rightFloat = *(float *) (rightTuple + rightOffset);
                result = compareFloat(leftFloat, rightFloat);
                break;
            }
            case AttrType::TypeVarChar: {
                int leftNumChar = *(int *) ((char *) leftTuple + leftOffset);
                int rightNumChar = *(int *) (rightTuple + rightOffset);
                result = compareVarChar((char *) leftTuple + leftOffset + INTSIZE,
                                        rightTuple + rightOffset + INTSIZE, leftNumChar, rightNumChar);
                break;
            }
        }
        return result;
    }

    RC INLJoin::joinTuple(void *data) {

        unsigned length = joinAttribute.size();
        unsigned null_bytes = length / 8 + (length % 8 == 0 ? 0 : 1); // get number of bytes that indicate null

        char * null_indicator = new char[null_bytes];
        unsigned offset = 0;
        int index = 0;
        offset += null_bytes;

        for (int i = 0 ; i < null_bytes ; i++) {null_indicator[i] = 0;}

        int leftOffset = 0;
        int leftLength = leftAttribute.size();
        unsigned leftNullBytes = leftLength / 8 + (leftLength % 8 == 0 ? 0 : 1);
        char * leftNull = new char[leftNullBytes];
        memcpy(leftNull, leftTuple, leftNullBytes);
        leftOffset += leftNullBytes;
        for(int i = 0; i < leftLength ; i++){
            unsigned char mask = (unsigned) 1 << (unsigned) (7 - i % 8);
            bool nullBit = leftNull[i / 8] & mask;

            if (nullBit) {
                unsigned char newMask = (unsigned) 1 << (unsigned) (7 - index % 8);
                unsigned nullplace = index / 8;
                null_indicator[nullplace] += newMask;
            } else {
                switch (leftAttribute.at(i).type) {
                    case AttrType::TypeInt: {
                        memcpy((char *) data + offset, (char *) leftTuple + leftOffset, INTSIZE);
                        offset += INTSIZE;
                        leftOffset += INTSIZE;
                        break;
                    }
                    case AttrType::TypeReal: {
                        memcpy((char *) data + offset, (char *) leftTuple + leftOffset, FLOATSIZE);
                        offset += FLOATSIZE;
                        leftOffset += FLOATSIZE;
                        break;
                    }
                    case AttrType::TypeVarChar: {
                        unsigned int number_of_char = *((unsigned int *) ((char *) leftTuple + leftOffset));
                        memcpy((char *) data + offset, (char *) leftTuple + leftOffset,
                               number_of_char + INTSIZE);
                        offset += INTSIZE;
                        offset += number_of_char;
                        leftOffset += INTSIZE;
                        leftOffset += number_of_char;
                    }
                }
            }
            index++;
        }

        int rightOffset = 0;
        int rightLength = rightAttribute.size();
        unsigned rightNullBytes = rightLength / 8 + (rightLength % 8 == 0 ? 0 : 1);
        char * rightNull = new char[rightNullBytes];
        memcpy(rightNull, rightTuple, rightNullBytes);
        rightOffset += rightNullBytes;
        for(int i = 0; i < rightLength ; i++){
            unsigned char mask = (unsigned) 1 << (unsigned) (7 - i % 8);
            bool nullBit = rightNull[i / 8] & mask;

            if (nullBit) {
                unsigned char newMask = (unsigned) 1 << (unsigned) (7 - index % 8);
                unsigned nullplace = index / 8;
                null_indicator[nullplace] += newMask;
            } else {
                switch (rightAttribute.at(i).type) {
                    case AttrType::TypeInt: {
                        memcpy((char *) data + offset, (char *) rightTuple + rightOffset, INTSIZE);
                        offset += INTSIZE;
                        rightOffset += INTSIZE;
                        break;
                    }
                    case AttrType::TypeReal: {
                        memcpy((char *) data + offset, (char *) rightTuple + rightOffset, FLOATSIZE);
                        offset += FLOATSIZE;
                        rightOffset += FLOATSIZE;
                        break;
                    }
                    case AttrType::TypeVarChar: {
                        unsigned int number_of_char = *((unsigned int *) ((char *) rightTuple + rightOffset));
                        memcpy((char *) data + offset, (char *) rightTuple + rightOffset,
                               number_of_char + INTSIZE);
                        offset += INTSIZE;
                        offset += number_of_char;
                        rightOffset += INTSIZE;
                        rightOffset += number_of_char;
                    }
                }
            }
            index++;
        }

        if( index == length ) {
            memcpy(data, null_indicator, null_bytes);
            delete[] null_indicator;
            delete[] leftNull;
            delete[] rightNull;
            return SUCCESS;
        } else {
            delete[] null_indicator;
            delete[] leftNull;
            delete[] rightNull;
            return 5000;
        }
    }

    bool INLJoin::compareInt(int leftInteger, int rightInteger) {
        switch(joinCondition.op) {
            case CompOp::EQ_OP : {
                return leftInteger == rightInteger;
            }
            case CompOp::GE_OP : {
                return leftInteger >= rightInteger;
            }
            case CompOp::GT_OP : {
                return leftInteger > rightInteger;
            }
            case CompOp::LE_OP : {
                return leftInteger <= rightInteger;
            }
            case CompOp::LT_OP : {
                return leftInteger < rightInteger;
            }
            case CompOp::NE_OP : {
                return leftInteger != rightInteger;
            }
            case CompOp::NO_OP : {
                return true;
            }
        }
    }

    bool INLJoin::compareFloat(float leftFloat, float rightFloat) {
        switch(joinCondition.op) {
            case CompOp::EQ_OP : {
                return leftFloat == rightFloat;
            }
            case CompOp::GE_OP : {
                return leftFloat >= rightFloat;
            }
            case CompOp::GT_OP : {
                return leftFloat > rightFloat;
            }
            case CompOp::LE_OP : {
                return leftFloat <= rightFloat;
            }
            case CompOp::LT_OP : {
                return leftFloat < rightFloat;
            }
            case CompOp::NE_OP : {
                return leftFloat != rightFloat;
            }
            case CompOp::NO_OP : {
                return true;
            }
        }
    }

    bool INLJoin::compareVarChar(char *leftString, char *rightString, int leftNumChar, int rightNumChar) {
        int max = 0;
        if(leftNumChar > rightNumChar) {
            max = leftNumChar;
        } else {
            max = rightNumChar;
        }
        switch (joinCondition.op) {
            case CompOp::EQ_OP : {
                bool result = memcmp(leftString, rightString, max) == 0;
                return result;
            }
            case CompOp::GE_OP : {
                bool result = memcmp(leftString, rightString, max) >= 0;
                return result;
            }
            case CompOp::GT_OP : {
                bool result = memcmp(leftString, rightString, max) > 0;
                return result;
            }
            case CompOp::LE_OP : {
                bool result = memcmp(leftString, rightString, max) <= 0;
                return result;
            }
            case CompOp::LT_OP : {
                bool result = memcmp(leftString, rightString, max) < 0;
                return result;
            }
            case CompOp::NE_OP : {
                bool result = memcmp(leftString, rightString, max) != 0;
                return result;
            }
            case CompOp::NO_OP : {
                return true;
            }
        }
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs.insert(attrs.begin(), joinAttribute.begin(), joinAttribute.end());
        return SUCCESS;
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {
        numberPartition = numPartitions;
        joinCondition = condition;
        leftIn->getAttributes(leftAttribute);
        buildPartition(leftIn, true, leftPartition);
        rightIn->getAttributes(rightAttribute);
        buildPartition(rightIn, false, rightPartition);
        joinAttribute.clear();
        joinAttribute.insert(joinAttribute.begin(),leftAttribute.begin(),leftAttribute.end());
        joinAttribute.insert(joinAttribute.end(),rightAttribute.begin(),rightAttribute.end());
        bnlEndOfFile = QE_EOF;
        callCount = 0;
        count = 0;
    }

    RC GHJoin::buildPartition(Iterator *iter, bool isLeft,  std::vector<std::string> &fileName) {
        std::string name;
        if (isLeft) {
            name = "left";
        } else {
            name = "right";
        }
        char * tuple = new char[PAGE_SIZE];
        memset(tuple, 0, PAGE_SIZE);
        for(int i = 0 ; i < numberPartition; i++) {
            fileName.push_back(name + "Join" + std::to_string(i));
            if (isLeft) {
                relationManager.createTable(fileName.at(i), leftAttribute);
            } else {
                relationManager.createTable(fileName.at(i), rightAttribute);
            }
        }
        std::string tableName;
        if (isLeft) {
            tableName = leftAttribute[0].name;
        } else {
            tableName = rightAttribute[0].name;
        }
        for ( int i = tableName.size() - 1; i > 0 ; i--) {
            if ('.' != tableName[i]) {
                tableName.pop_back();
            } else {
                tableName.pop_back();
                break;
            }
        }
        std::vector<int> valueCount = {0,0,0,0,0,0,0,0,0,0};
        while( iter->getNextTuple(tuple) != QE_EOF ) {
            int value = -1;
            RID rid;
            std::stringstream ss;
            if (isLeft) {
                value = getValue(tuple, leftAttribute, joinCondition.lhsAttr);
                relationManager.insertTuple(fileName[value], tuple, rid);
                relationManager.printTuple(leftAttribute, tuple, ss);
            } else {
                value = getValue(tuple, rightAttribute, joinCondition.rhsAttr);
                relationManager.insertTuple(fileName[value],  tuple, rid);
                relationManager.printTuple(rightAttribute, tuple, ss);
            }
            std::string temp = ss.str();
            memset(tuple, 0, PAGE_SIZE);
            valueCount[value] += 1;
            relationManager.storeCurrentSystem(tableName);
        }
        delete[] tuple;
        return SUCCESS;
    }

    int GHJoin::getValue(void *record, std::vector<Attribute> &recordDescriptor, std::string &attributeName) {
        // calculate how many bytes data is using
        unsigned length = recordDescriptor.size();
        unsigned null_bytes = length / 8 + (length % 8 == 0 ? 0 : 1); // get number of bytes that indicate null
        int value;

        // ceil (length of vector / 8 ) go through the data with the null value
        unsigned char * null_indicator = new unsigned char[null_bytes];
        unsigned offset = 0;
        unsigned char * temp = (unsigned char*) record;
        bool nullBit = false;
        memcpy(null_indicator, temp+offset, null_bytes);
        offset += null_bytes;
        for(int i = 0 ; i < length ; i++ ){
            unsigned char mask = (unsigned)1 << (unsigned) (7 - i % 8);
            nullBit = null_indicator[i/8] & mask;
            if (recordDescriptor[i].name == attributeName) {
                switch (recordDescriptor[i].type) {
                    case AttrType::TypeInt:
                    case AttrType::TypeReal:
                        delete[] null_indicator;
                        value = *(int*)((char*)temp + offset);
                        return value % numberPartition;
                }
                break;
            }
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
        return -1;
    }

    GHJoin::~GHJoin() {
        RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
        for(int i = 0; i < numberPartition; i++) {
            recordBasedFileManager.destroyFile(leftPartition[i]);
            recordBasedFileManager.destroyFile(rightPartition[i]);
        }
    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
        while (true) {
            if (callCount <= numberPartition) {
                if (bnlEndOfFile == QE_EOF) {
                    if (callCount > 0 ) {
                        delete leftIter;
                        delete rightIter;
                        delete bnlJoin;
                    }
                    RecordBasedFileManager &recordBasedFileManager = RecordBasedFileManager::instance();
                    int pageNum = 0;
                    FileHandle fileHandle;
                    recordBasedFileManager.openFile(leftPartition[callCount], fileHandle);
                    pageNum = fileHandle.numberOfPages;
                    leftIter = new TableScan(relationManager, leftPartition[callCount]);
                    rightIter = new TableScan(relationManager, rightPartition[callCount]);
                    Condition newCondition = joinCondition;
                    newCondition.rhsAttr = rightPartition[callCount] + "." + newCondition.rhsAttr;
                    newCondition.lhsAttr = leftPartition[callCount] + "." + newCondition.lhsAttr;
                    bnlJoin = new BNLJoin(leftIter, rightIter, newCondition, pageNum);
                    callCount += 1;
                }
                bnlEndOfFile = bnlJoin->getNextTuple(data);
                if (bnlEndOfFile == QE_EOF) {
                    continue;
                }
                std::stringstream ss;
                relationManager.printTuple(joinAttribute, data, ss);
                std::string temp = ss.str();
                int value = getValue(data, rightAttribute, joinCondition.rhsAttr);
                count += 1;
                return bnlEndOfFile;
            } else {
                delete leftIter;
                delete rightIter;
                delete bnlJoin;
                return QE_EOF;
            }
        }
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs.insert(attrs.begin(), joinAttribute.begin(), joinAttribute.end());
        return SUCCESS;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {
        isGroupBy = false;
        aggregateAttribute = aggAttr;
        iter = input;
        tuple = new char[PAGE_SIZE];
        aggregateOp = op;
        endOfFile = false;
        iter->getAttributes(iterAttribute);
        aggregateAttributes.push_back(aggAttr);
        switch(aggregateOp) {
            case AggregateOp::AVG:
                aggregateAttributes[0].name = "AVG(" + aggAttr.name + ")";
                break;
            case AggregateOp::COUNT:
                aggregateAttributes[0].name = "COUNT(" + aggAttr.name + ")";
                break;
            case AggregateOp::MAX:
                aggregateAttributes[0].name = "MAX(" + aggAttr.name + ")";
                break;
            case AggregateOp::MIN:
                aggregateAttributes[0].name = "MIN(" + aggAttr.name + ")";
                break;
            case AggregateOp::SUM:
                aggregateAttributes[0].name = "SUM(" + aggAttr.name + ")";
                break;
        }
        aggregateAttributes[0].type = AttrType::TypeReal;
        aggregateNoGroup();
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {
        isGroupBy = true;
        groupAttribute = groupAttr;
        aggregateAttribute = aggAttr;
        iter = input;
        tuple = new char[PAGE_SIZE];
        aggregateOp = op;
        endOfFile = false;
        iter->getAttributes(iterAttribute);
        aggregateAttributes.push_back(groupAttr);
        aggregateAttributes.push_back(aggAttr);
        switch(aggregateOp) {
            case AggregateOp::AVG:
                aggregateAttributes[1].name = "AVG(" + aggAttr.name + ")";
                break;
            case AggregateOp::COUNT:
                aggregateAttributes[1].name = "COUNT(" + aggAttr.name + ")";
                break;
            case AggregateOp::MAX:
                aggregateAttributes[1].name = "MAX(" + aggAttr.name + ")";
                break;
            case AggregateOp::MIN:
                aggregateAttributes[1].name = "MIN(" + aggAttr.name + ")";
                break;
            case AggregateOp::SUM:
                aggregateAttributes[1].name = "SUM(" + aggAttr.name + ")";
                break;
        }
        aggregateAttributes[1].type = AttrType::TypeReal;
        aggregateGroup();
    }

    Aggregate::~Aggregate() {
        delete[] tuple;
    }

    RC Aggregate::getNextTuple(void *data) {
        if (isGroupBy) {
            if(keys.empty()) {
                return QE_EOF;
            }
            std::string key;
            int offset = 0;
            offset += 1;
            char null = '\0';
            memcpy((char*)data, &null, 1);
            float value;
            key = keys.at(0);
            if (aggregateOp == AggregateOp::AVG) {
                float sum = aggregatedValueMap[key+"SUM"];
                float count = aggregatedValueMap[key+"COUNT"];
                value = sum / count;
            } else {
                value = aggregatedValueMap[key];
            }
            switch (groupAttribute.type) {
                case AttrType::TypeInt: {
                    int integer = std::stoi(key);
                    memcpy((char*)data+offset, &integer, INTSIZE);
                    offset += INTSIZE;
                    break;
                }
                case AttrType::TypeReal: {
                    float realNumber = std::stof(key);
                    memcpy((char*)data+offset, &realNumber, FLOATSIZE);
                    offset += FLOATSIZE;
                    break;
                }
                case AttrType::TypeVarChar: {
                    int size = key.size();
                    memcpy((char*) data + offset, &size, INTSIZE);
                    offset += INTSIZE;
                    memcpy((char*) data + offset, key.c_str(), size);
                    offset += size;
                    break;
                }
            }
            memcpy((char*) data + offset, & value, FLOATSIZE);
            keys.erase(keys.begin());
            return SUCCESS;
        } else {
            if(aggregatedValueMap.size() > 1) {
                return 40004;
            } else if (aggregatedValueMap.empty()) {
                return QE_EOF;
            }
            char null = '\0';
            float value = aggregatedValueMap["all"];
            aggregatedValueMap.erase("all");
            memcpy(data, &null, 1);
            memcpy((char*)data + 1, &value, FLOATSIZE);
            return SUCCESS;
        }
    }

    RC Aggregate::aggregateNoGroup() {
        float aggValue;
        switch(aggregateOp) {
            case AggregateOp::AVG:
                aggValue = noGroupAvg();
                break;
            case AggregateOp::COUNT:
                aggValue = noGroupCount();
                break;
            case AggregateOp::MAX:
                aggValue = noGroupMax();
                break;
            case AggregateOp::MIN:
                aggValue = noGroupMin();
                break;
            case AggregateOp::SUM:
                aggValue = noGroupSum();
                break;
        }
        std::string all = "all";
        aggregatedValueMap[all] = aggValue;
        return SUCCESS;
    }

    RC Aggregate::aggregateGroup() {
        while (iter->getNextTuple(tuple) != RM_EOF) {
            float value;
            std::string Key;
            getValue(tuple, value);
            getKey(tuple, Key);
            memset(tuple, 0, PAGE_SIZE);
            switch(aggregateOp) {
                case AggregateOp::AVG:
                    try {
                        aggregatedValueMap.at(Key+"Sum");
                        aggregatedValueMap.at(Key+"Count");
                    } catch (std::out_of_range) {
                        aggregatedValueMap[Key+"Count"] = 0;
                        aggregatedValueMap[Key+"Sum"] = 0;
                        keys.push_back(Key);
                    }
                    break;
                case AggregateOp::COUNT:
                    try {
                        aggregatedValueMap.at(Key);
                    } catch (std::out_of_range) {
                        aggregatedValueMap[Key] = 0;
                        keys.push_back(Key);
                    }
                    aggregatedValueMap[Key] += 1;
                    break;
                case AggregateOp::MAX:
                    try {
                        aggregatedValueMap.at(Key);
                    } catch (std::out_of_range) {
                        aggregatedValueMap[Key] = FLT_MIN;
                        keys.push_back(Key);
                    }
                    if (aggregatedValueMap[Key] < value) {
                        aggregatedValueMap[Key] = value;
                    }
                    break;
                case AggregateOp::MIN:
                    try {
                        aggregatedValueMap.at(Key);
                    } catch (std::out_of_range) {
                        aggregatedValueMap[Key] = FLT_MAX;
                        keys.push_back(Key);
                    }
                    if (aggregatedValueMap[Key] > value) {
                        aggregatedValueMap[Key] = value;
                    }
                    break;
                case AggregateOp::SUM:
                    try {
                        aggregatedValueMap.at(Key);
                    } catch (std::out_of_range) {
                        aggregatedValueMap[Key] = 0;
                        keys.push_back(Key);
                    }
                    aggregatedValueMap[Key] += value;
                    break;
            }
        }
    }

    RC Aggregate::getKey(void * data, std::string & Key) {
        // calculate how many bytes data is using
        unsigned length = iterAttribute.size();
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
            if (iterAttribute[i].name == groupAttribute.name) {
                switch (iterAttribute[i].type) {
                    case AttrType::TypeInt: {
                        int integer = *(int*)(temp + offset);
                        Key = std::to_string(integer);
                        break;
                    }
                    case AttrType::TypeReal: {
                        float realNumber = *(float *)( temp + offset);
                        Key = std::to_string(realNumber);
                        break;
                    }
                    case AttrType::TypeVarChar: {
                        unsigned int number_of_char = *((unsigned int *) (temp + offset));
                        char *attributeString = new char[number_of_char + 1];
                        memcpy(attributeString, temp + offset + INTSIZE, number_of_char);
                        attributeString[number_of_char] = '\0';
                        Key = attributeString;
                        delete[] attributeString;
                        break;
                    }
                }
                break;
            }
            if (!nullBit){
                switch (iterAttribute[i].type) {
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

    RC Aggregate::getValue(void * record, float & value) {
        // calculate how many bytes data is using
        unsigned length = iterAttribute.size();
        unsigned null_bytes = length / 8 + (length % 8 == 0 ? 0 : 1); // get number of bytes that indicate null

        // ceil (length of vector / 8 ) go through the data with the null value
        unsigned char * null_indicator = new unsigned char[null_bytes];
        unsigned offset = 0;
        unsigned char * temp = (unsigned char*) record;
        bool nullBit = false;
        memcpy(null_indicator, temp+offset, null_bytes);
        offset += null_bytes;
        for(int i = 0 ; i < length ; i++ ){
            unsigned char mask = (unsigned)1 << (unsigned) (7 - i % 8);
            nullBit = null_indicator[i/8] & mask;
            if (iterAttribute[i].name == aggregateAttribute.name) {
                switch (iterAttribute[i].type) {
                    case AttrType::TypeInt:
                        delete[] null_indicator;
                        value = float(*(int*)((char*)temp + offset));
                        return SUCCESS;
                    case AttrType::TypeReal:
                        delete[] null_indicator;
                        value = *(float*)((char*)temp + offset);
                        return SUCCESS;
                }
                break;
            }
            if (!nullBit){
                switch (iterAttribute[i].type) {
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
        return -1;
    }

    float Aggregate::noGroupAvg() {
        int count = 0;
        float sum = 0;
        while (iter->getNextTuple(tuple) != RM_EOF) {
            float value;
            if( getValue(tuple, value) != SUCCESS) {
                throw std::invalid_argument( "received string value" );
            }
            sum += value;
            count += 1;
        }
        return sum / count;
    }

    float Aggregate::noGroupCount() {
        int count = 0;
        float avg = 0;
        while (iter->getNextTuple(tuple) != RM_EOF) {
            count += 1;
        }
        return count;
    }

    float Aggregate::noGroupMax() {
        float max = FLT_MIN;
        while (iter->getNextTuple(tuple) != RM_EOF) {
            float value = FLT_MIN;
            if( getValue(tuple, value) != SUCCESS) {
                throw std::invalid_argument( "received string value" );
            }
            if (max < value) {
                max = value;
            }
        }
        return max;
    }

    float Aggregate::noGroupMin() {
        float min = FLT_MAX;
        while (iter->getNextTuple(tuple) != RM_EOF) {
            float value;
            if( getValue(tuple, value) != SUCCESS) {
                throw std::invalid_argument( "received string value" );
            }
            if (min > value) {
                min = value;
            }
        }
        return min;
    }

    float Aggregate::noGroupSum() {
        float sum = 0;
        while (iter->getNextTuple(tuple) != RM_EOF) {
            float value;
            if (getValue(tuple, value) != SUCCESS) {
                throw std::invalid_argument("received string value");
            }
            sum += value;
        }
        return sum;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs.insert(attrs.begin(), aggregateAttributes.begin(), aggregateAttributes.end());
        return SUCCESS;
    }
} // namespace PeterDB