# simpledb_437_hw
Implement Buffer Replacement policies and join algorithms in SimpleDB

##SimpleDB Heap Pages and Buffer Replacement Policies

###Abstraction of page header implementation
Currently the code for page header’s implementation (Store 32 bits in one integer) and related arithmetic for calculating
storage is scattered all over the file. So I decided to move this into an inner class ”Header”. The class abstracts the
implementation of header and users of its public api only need to deal with byte[ ] and slot data. This way all the bit
arithmetic remains encapsulated and also leaves a scope for easy modification of header’s implementation in future.

###Encapsulating buffer replacement policy implementation
As there are multiple policies that can be applied to a buffer pool, each requiring different bookkeeping; so I decided to
encapsulate this in class called ”PolicyData”, which maintains this bookkeeping information for various policies. This gives
a benefit that instead of using if-else or switch-case constructs in all dependent functions of class BufferPool, it will now
have to deal with only one class object which will handle the various cases inside itself. A better way of doing this can be
to separate the Policy related implementation and bookkeeping in separate classes and then use decorator design pattern to
use it over our buffer pool. This way would be more suitable when we have a lot more policies with complex and disjoint
implementation schemes.

###HashMap for BufferPool
HashMap provide O(1) access for all the elements in the data-structure so I decided to use it for storing our buffered pages.
We can check for existence of a page in buffer pool in O(1), fetch the same in O(1) and also insert or remove an element from
it in constant time.

###Implementation of LRU and MRU using global counter scheme
Class BufferPool needs to work on both LRU and MRU buffer replacement schemes. So I decided to go for an implementation
which can work in both cases. We keep track of usage of a page using a global counter. Every time a page is accessed, the
global counter’s value is stored in a hashmap corresponding to that page (Using pageId as key), after which counter’s value
is incremented for any further accesses. This way all the new buffered pages keep getting a value higher than any previous
value. When a page needs to be evicted, we check if the policy for eviction is LRU, then find a page with the least count else
if the policy for eviction is MRU then find a page with the highest count value and then return the page found for eviction.

##SimpleDB Join algorithms (SNL, PNL and SMJ)

###Separation of Join Algorithm’s implementation using Factory design
Currently all the join algorithm’s implementation is expected to be in one class ”Join”, I decided to separate there implemen-
tation into multiple classes as different join algorithms required different bookkeeping methodologies which might be useful
for one algorithms but redundant for others. This way we can also avoid long if-else or switch-case chains over ”joinType” in
class ”Join”. I created an abstract class ”AbstractJoin” which extends ”DbIterator” and moved the common functionality
among join algorithms to this abstract class and then other join algorithms simple extend this abstract and implement the
remaining methods. After implementing multiple join classes I decided to use the existing class ”Join” as a factory class
which will update the instance of currently used join algorithm according to requirement and rest of the functions of this
class simply call the active join algorithm’s methods. This way I also avoided making changes to the api expected by join
test cases as they expect a class Join rather my implementation of AbstractJoin.

###Implementation of BufferedDBIterator for Page wise iteration over DB
Page nested loop requires page wise iteration over inner and outer relation. DBIterator (HeapFileIterator) implements this,
but doesn’t expose the notion of Page through its api. So I decided implement a wrapper over DBIterator ”BufferedDbIt-
erator”, which will read one page at a time using the underlying DBIterator instead of one Tuple. But this also required a
way to store the content of extracted page temporarily. For this I modified the existing HeapPage class and implemented
the ”addTuple()” method in this class to facilitate using of HeapPape instance as a temporary buffer for Page wise itera-
tion of DB File. So ”BufferedDbIterator” gives us the next page and then HeapPage returned already has a tuple iterator
implementation which i used to iterate over its tuples. Page nested loop could also have been implemented using the seek()
described below with some additional book keeping, but i think the current implementation of using BufferedDBIterator is
more clean and extendible.

###Additional method for HeapFileIterator - seek()
HeapFileIterator allows to sequentially go backward and forward from current position, but currently there is no way to go
to a particular record position. So I implemented a function ”Seek()” which takes a record ID as input parameter and using
the page number and slot id it then goes to the corresponding tuple’s location in DB File. Using this functionality we can
go back to any known record in DB file as needed in sort merge join implementation to handle the case of duplicate outer
relation join attributes.
