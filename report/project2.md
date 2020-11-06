## Project 2 Report


### 1. Basic information
 - Team #: 11
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-fall20-team-11
 - Student 1 UCI NetID: sehwanh
 - Student 1 Name:Sehwan Hong
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):

### 2. Meta-data
- Show your meta-data design (Tables and Columns table) and information about each column.

Tables are stored on catalog_table file, and columns are saved on catalog_column. The recordDescriptor about the column data and table data is stored by itself an could be used separately.

### 3. Internal Record Format (in case you have changed from P1, please re-enter here)
- Show your record format design.

same as P1.

- Describe how you store a null field.

same as P1.

- Describe how you store a VarChar field.

same as P1

- Describe how your record design satisfies O(1) field access.

same as P1

### 4. Page Format (in case you have changed from P1, please re-enter here)
- Show your page format design.

The Page Format Design is adopted from P1. However, there was 4 extra bytes were added for delete and update purposes.
One 2Byte unsigned integer is used to store the last Insterted slot number to calculate the end of Data point.
The other 2Byte unsigned integer is used to store the slot without any data stored.

- Explain your slot directory design if applicable.

same as P1

### 5. Page Management (in case you have changed from P1, please re-enter here)
- How many hidden pages are utilized in your design?

same as P1

- Show your hidden page(s) format design if applicable

same as P1

### 6. Describe the following operation logic.
- Delete a record

If the record is deleted, check whether the record is on this page or not. Since there might be a posibility that the page only stores the slotNum and pageNum to real data.
If the record is on the current page, Shift all data after the slotOffSet point with size of the record.
If the record is not on current page, call recursively on the stored PageNum and SlotNum.

- Update a record

If the updated information is smaller than already stored information, the data will be over written and leftover will be shifted to save spaces.
if the updated informaion is greater than already stored information but is smaller than current available space in the page, First previous record will be deleted and then the new record will be stored at the beginning of the free space.
if the updated information is greater than the available storage on the page, then the currently stored data will be deleted and store new data in different pages.

If the updated information is updated again which does not store the data in the current page, then the record will be deleted from the whole file then newly stored in any empty space.

- Scan on normal records

The Scan would go through the record data and see if the record satisfy the condition. If the record satisfy the condition, then it will return the projected Attributes. 

- Scan on deleted records

The Scan ignore if the record is deleted.

- Scan on updated records

The Scan Ignore updated records that is not on the current page. Scan would work when they move to the page where the data is directly stored.



### 7. Implementation Detail
- Other implementation details goes here.



### 8. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 9. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)