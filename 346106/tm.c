/**
 * @file   tm.c
 * @author [...]
 *
 * @section LICENSE
 *
 * [...]
 *
 * @section DESCRIPTION
 *
 * Implementation of your own transaction manager.
 * You can completely rewrite this file (and create more files) as you wish.
 * Only the interface (i.e. exported symbols and semantic) must be preserved.
**/

// Requested features
#define _GNU_SOURCE
#define _POSIX_C_SOURCE   200809L
#ifdef __STDC_NO_ATOMICS__
    #error Current C11 compiler does not support atomic operations
#endif

// External headers
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdatomic.h>
// Internal headers
#include <tm.h>

// -------------------------------------------------------------------------- //

/** Define a proposition as likely true.
 * @param prop Proposition
**/
#undef likely
#ifdef __GNUC__
    #define likely(prop) \
        __builtin_expect((prop) ? 1 : 0, 1)
#else
    #define likely(prop) \
        (prop)
#endif

/** Define a proposition as likely false.
 * @param prop Proposition
**/
#undef unlikely
#ifdef __GNUC__
    #define unlikely(prop) \
        __builtin_expect((prop) ? 1 : 0, 0)
#else
    #define unlikely(prop) \
        (prop)
#endif

/** Define one or several attributes.
 * @param type... Attribute names
**/
#undef as
#ifdef __GNUC__
    #define as(type...) \
        __attribute__((type))
#else
    #define as(type...)
    #warning This compiler has no support for GCC attributes
#endif

// GENERIC LISTS -------------------------------------------------------------------------- //
typedef struct duplicate_byte d_byte_t;
typedef enum{
    TByte,
    TSeg
}LType;

typedef struct linked_list_element{
    LType T;
    void *self; 
    struct linked_list_element *next;
}element_t;

typedef struct linked_list{
    LType T;
    pthread_mutex_t lock;
    size_t length;
    element_t *first;
    element_t *last;
}list_t;

void print_list(list_t *l);

list_t *list_new(LType type){
    list_t *list = (list_t *)malloc(sizeof(list_t));
    pthread_mutex_init(&list->lock, NULL);
    list->T = type;
    list->length = 0;
    list->first = NULL;
    list->last = NULL;
    return list;
}
/*destroys the list, 
does not free the resourcesof the elements*/
void list_destroy(list_t *list){
    if(list == NULL){
        perror("list is null");
        exit(EXIT_FAILURE);
    }
    element_t *i = list->first;
    while(i != NULL){
        element_t *next = i->next;
        free(i);
        i = next;
    }
    pthread_mutex_destroy(&list->lock);
    free(list);

}
/*concatenate list2 onto list1
* moves all element references from list1 to list2 
* after the operation list2 will be empty and list1 will contain list1 and list2
* list2 should not be accessed concurrently with this operation
*/

/*consumes the elements of a list as defined by the consume function
* the consume function must free all resources within the element
*/
void list_consume(list_t *list, void (*consume (void *element))){
    if(unlikely(list == NULL)){
        perror("list is null");
        exit(EXIT_FAILURE);
    }
    pthread_mutex_lock(&list->lock);
    element_t *i = list->first;
    if(i == NULL)
        return;
    while(i != NULL){
        consume(i->self);
        element_t *next = i->next;
        free(i);
        i = next;
    }
    //empty list
    list->first = NULL;
    list->last = NULL;
    list->length = 0;
    pthread_mutex_unlock(&list->lock);
}
/*append a generic element to a list*/
void list_append(LType T, list_t *list, void *element){
    if(unlikely(list->T != T)){
        perror("appending wrong type");
        exit(EXIT_FAILURE);
    }
    if(unlikely(list == NULL)){
        perror("list is null");
        exit(EXIT_FAILURE);
    }
    if(unlikely(element == NULL))
    {
        perror("element is null");
        exit(EXIT_FAILURE);
    }
    element_t *e = (element_t *)malloc(sizeof(element_t));
    e->T = T;
    e->self = element;
    e->next = NULL;
    pthread_mutex_lock(&list->lock);
    //printf("append:");
    //print_list(list);
    if (list->length == 0)
    {
        list->first = e;
        list->last = e;
    }
    else{
        list->last->next = e;
        list->last = e;
    }
    list->length++;
    pthread_mutex_unlock(&list->lock);
}
/*pops the first element in the array*/
void *list_pop(LType T, list_t *list){
    if(unlikely(list->T != T)){
        perror("appending wrong type");
        exit(EXIT_FAILURE);
    }
    if(unlikely(list == NULL)){
        perror("list is null");
        exit(EXIT_FAILURE);
    }
    pthread_mutex_lock(&list->lock);
    //printf("pop:");
    //print_list(list);
    if(unlikely(list->length == 0)){
        perror("cannot pop an empty list");
        exit(EXIT_FAILURE);
    }
    
    void *e = list->first->self;
    element_t *next = list->first->next;
    free(list->first);
    list->first = next;
    list->length--;
    if(list->length == 0)
        list->last = NULL;
    //printf("%ld\n", list->length);
    pthread_mutex_unlock(&list->lock);

    return e;
}

void list_concat(list_t *list1, list_t *list2){
    if(unlikely(list1->T != list2->T)){
        perror("list types do not match");
        exit(EXIT_FAILURE);
    }
    if(unlikely(list1 == NULL || list2 == NULL)){
        perror("list is null");
        exit(EXIT_FAILURE);
    }

    //concatenate
    //printf("test\n");
    pthread_mutex_lock(&list1->lock);
    /*
    if(list1->T == TByte){
        printf("concat1:");
        print_list(list1);
        printf("\n");
    }
    */
    //if list1 is empty
    if(list1->length == 0){
        list1->first = list2->first;     
    }
    else{
        list1->last->next = list2->first;   
    }
    //if list2 is not empty
    if(list2->length > 0){
        list1->last = list2->last;
    }
    
    list1->length = list1->length + list2->length;
    /*
    if(list1->T == TByte){
        printf("concat2:");
        print_list(list1);
        printf("\n");
    }*/
    pthread_mutex_unlock(&list1->lock);
    //empty list 2
    list2->first = NULL;
    list2->last = NULL;
    list2->length = 0;
    //if(list1->length > 0 && list1->T == TByte)
        //printf("[%p]%p, %d\n",list1, list1->first, ((d_byte_t *)list1->first->self)->control);
}

//TRANSACTIONAL MEMORY --------------------------------------------------------------------------- //
#define REG_CTL_WR (1<<0)       //indicates wether bytes has been written to this epoch
#define REG_CTL_RD (1<<1)       //indicates wether bytes as been read from this epoch
#define REG_CTL_VALID (1<<2)    //indicates which copy holds the readable value (1=read_copy, 0=write_copy)
#define BITS_48 0xFFFFFFFFFFFF
#define SEG_PART(x) ((uint64_t)x >> 48)           //takes the 16 MSbs of x (segment part)
#define BYTE_PART(x) ((uint64_t)x & BITS_48)     //isolates the 48 LSbs of x (byte part)
#define MAKE_VIRTUAL(seg, byte) (virt_addr)(seg << 48 | (byte & BITS_48))
//segments start from 0 in array while virtual address start from 1
#define VIRTUAL_SEG(seg_offset) (v_addr)((uint64_t)(seg_offset + 1) << 48)  

typedef pthread_mutex_t lock_t;
typedef pthread_rwlock_t rw_lock_t;
typedef struct cond_variable {
    pthread_mutex_t mutex;
    pthread_cond_t cv;
}cvar_t;
#define LOCK(x) pthread_mutex_lock(x)
#define UNLOCK(x) pthread_mutex_unlock(x)
#define LOCK_WR(x) pthread_rwlock_wrlock(x)
#define UNLOCK_RW(x) pthread_rwlock_unlock(x)
#define LOCK_RD(x) pthread_rwlock_rdlock(x)
#define WAIT(x) pthread_cond_wait(&(x->cv), &(x->mutex))
#define SIGNAL(x) pthread_cond_broadcast(&(x->cv))

typedef struct transaction transaction_t;
typedef struct segment segment_t;

/*
typedef struct duplicate_byte{
    //char control;                 //control array 
    char write_copy;              //write array  
    char read_copy;               //read array
    lock_t lock;
    //int accesses;                 //number of accesses
    //transaction_t* access_set;    //the latest accessed transaction
}d_byte_t;
*/
#define CTRL_RD 0                           //only a read has been made by the first transaction
#define CTRL_WR 1                           //a write has been made by the first transaction
#define CTRL_RD_MUL 2                       //reads has been made by the several transactions
#define CTRL_TX_MASK (0xfffffffc)           //30 LSbs (since a transaction is not less than 4 bytes the 2 LSbs are not needed for identification)
#define CTRL_ACCESS_MASK (0x3)              //2 LSbs
#define CTRL_EPOCH_MASK (0xffffffff00000000)//32 MSb
#define CTRL_SET(tx, n, e) ((int64_t)(tx & CTRL_TX_MASK) | ((int64_t)n & CTRL_ACCESS_MASK) | ((int64_t)e<<32 & CTRL_EPOCH_MASK))
#define CTRL_GET_ACC(ctrl) ((int64_t)ctrl & CTRL_ACCESS_MASK)
#define CTRL_GET_TX(ctrl) ((int64_t)ctrl & CTRL_TX_MASK)
#define CTRL_GET_EPOCH(ctrl) (((int64_t)ctrl & CTRL_EPOCH_MASK) >>32)
typedef struct duplicate_word{
    atomic_uint_least64_t ctrl;     
    //char control;                 //control array
    int64_t write_copy;              //write array  
    int64_t read_copy;               //read array
    //lock_t lock;
    //int accesses;                 //number of accesses
    //transaction_t* access_set;    //the latest accessed transaction
}d_word_t;
/*
typedef struct control_block{
    //atomic_uint_least64_t ctrl;
    //atomic_int epoch;
    int last_epoch;               //last epoch this control block has been accessed
    char control;                 //control array 
    int accesses;                 //number of accesses
    transaction_t* access_set;    //the latest accessed transaction
}ctl_block_t;
*/
//internal implemention of tx_t
//Each thread can only run one transaction AT A TIME
struct transaction{
    enum {READ_ONLY, READ_WRITE} access;
    //d_byte_list_t *writes;
    //seg_list_t *frees;
    list_t *writes;
    list_t *frees;
};

typedef struct batcher{
    int batch_num;               //number of the current batch
    int current_threads;         //number of threads in the current batch
    int waiting_threads;         //number of threads that are blocked
    int wakeups;                 //pending wakeups
    lock_t lock;
    pthread_cond_t cv;
    //tx_list_t *blocked;
}batcher_t;

#define THREAD_STATUS_BLOCKED 0
#define THREAD_STATUS_RUNNING 1

#define VIRTUAL_HEAP_START 0x00000001

typedef void* v_addr;

typedef struct segment{
    bool deallocated;
    //struct segment *prev;
    //struct segment *next;
    v_addr virtual_address;        //the virtual starting address of the segment
    size_t size;
    //ctl_block_t *control;
    d_word_t *words;
}segment_t;

//Dynamic array of segments
typedef struct segment_array{
    size_t length;                  //number of segments in the array
    size_t size;                    //maximum capacity of the array
    segment_t **array;
}seg_array_t;

//each shared region has a virtual address space
typedef struct region{
    batcher_t batcher;
    size_t alignment;
    rw_lock_t lock;
    //segment_t *start;
    seg_array_t segments;
    //d_byte_list_t *commits;
    //seg_list_t *deallocs;
    list_t *commits;
    list_t *deallocs;
}shared_region_t;

//OPTIMIZATION: CLEAR ONLY WRITTEN BYTES
/*
void region_clear_controls(shared_region_t *region){
    for (size_t i = 0; i < region->segments.length; ++i)
    {
        segment_t *seg = region->segments.array[i];
        //printf("phys:[%p] virt:[%p]\n", (void *)seg, seg->virtual_address);
        //printf("[%p] -> [%p]\n", (void *)&seg->bytes[0], (void *)&seg->bytes[seg->size]);
        
        for (size_t j = 0; j < seg->size; ++j)
        {
            seg->bytes[j].control = 0;
            seg->bytes[j].accesses = 0;
            seg->bytes[j].access_set = NULL;
        }
        
        if(seg != NULL)
            memset(seg->control, 0, sizeof(ctl_block_t)*seg->size/sizeof(int64_t));
    }
    
    segment_t *seg = region->start;
    while(seg != NULL){
        for (size_t i = 0; i < seg->size; ++i)
        {
            seg->bytes[i].control = 0;
            seg->bytes[i].accesses = 0;
            seg->bytes[i].access_set = NULL;
            //seg->bytes[i].read_copy = seg->bytes[i].write_copy;
        }
        seg = seg->next;
    }
}*/

//transaction "thread" tries to enter a batch in "batcher"
void batcher_enter(batcher_t *batcher){
    LOCK(&batcher->lock);
    //printf("current: %d, waiting: %d\n", batcher->current_threads, batcher->waiting_threads);
    //if the batch is empty, the thread may enter, else wait for wake-up

    if(batcher->current_threads == 0){
        batcher->current_threads = 1;
    }else{
        batcher->waiting_threads++;
        //UNLOCK(&batcher->lock);
        //printf("[%d] waiting: %d \n", gettid(), batcher->waiting_threads);
        pthread_cond_wait(&batcher->cv, &batcher->lock);

        /*
        if(batcher->wakeups > 0){
            printf("[%d] signal: \n", gettid());
            batcher->wakeups--;
            pthread_cond_broadcast(&batcher->wakeup.cv);
        }
        */
    }
    //printf("[%d] entered: \n", gettid());
    UNLOCK(&batcher->lock);
}
void commit(shared_region_t *reg as(unused)){
    while(reg->commits->length > 0)
    {
        //printf("%ld\n", i);
        d_word_t *word = (d_word_t *)list_pop(TByte, reg->commits);
        word->read_copy = word->write_copy;
    }
    while(reg->deallocs->length > 0)
    {
        segment_t *seg = (segment_t *)list_pop(TSeg, reg->deallocs);
        seg->deallocated = true;
        //remove segment from array
        /*
        int seg_off = SEG_PART(seg->virtual_address)-1;
        region->segments.array[seg_off] = NULL;
        //destroy the locks in each word
        for(size_t i = 0; i <seg->size/sizeof(int64_t); i++)
            pthread_mutex_destroy(&seg->words[i].lock);
        free(seg->control);
        free(seg);
        */
    }

}
void batcher_leave(shared_region_t *reg as(unused), batcher_t *batcher, transaction_t *t as(unused), bool abort as(unused)){
    LOCK(&batcher->lock);
    batcher->current_threads--;
    //printf("[%d] left: %d \n", gettid(), batcher->current_threads);
    //printf("current threads: %d\n", batcher->current_threads);
    //printf("waiting threads: %d\n", batcher->waiting_threads);
    //printf("[%d] left : current : %d \n", gettid(),batcher->current_threads);
    if(batcher->current_threads == 0){
        //printf("NEW BACTH\n");
        //printf("size: %ld\n", reg->commits->length);
        //region->deallocs = NULL;
        commit(reg);
        //region_clear_controls(reg);
        //printf("commit: \n");
        //print_list(reg->commits);
        //printf("\n");
        //printf("NEW BATCH\n");
        batcher->batch_num++;
        batcher->current_threads = batcher->waiting_threads;
        batcher->waiting_threads = 0;
        //printf("waiting %d : current : %d \n", batcher->waiting_threads ,batcher->current_threads);
        pthread_cond_broadcast(&batcher->cv);
    }
    UNLOCK(&batcher->lock);
    
    if(abort){
        transaction_t *transaction = (transaction_t *)t;
        list_destroy(transaction->writes);
        list_destroy(transaction->frees);
        free(t);
    }
}

/*get the d_word_t address (physical) given a virtual address*/
d_word_t *get_phys_addr(shared_region_t *region, v_addr virt_addr){
    size_t seg_offset = SEG_PART(virt_addr)-1;
    size_t byte_offset = BYTE_PART(virt_addr);

    LOCK_RD(&region->lock);
    if(unlikely(seg_offset > region->segments.length)){
        perror("fail phys addr");
        exit(EXIT_FAILURE);
    }
    segment_t *seg = region->segments.array[seg_offset];
    UNLOCK_RW(&region->lock);

    if(unlikely(seg == NULL)){
        perror("address is not allocated");
        exit(EXIT_FAILURE);
    }
    if(unlikely(seg->deallocated)){
        perror("address has been freed");
        exit(EXIT_FAILURE);
    }
    /*
    if(&seg->words[byte_offset] < (d_byte_t *)0xfffff)
        printf("%ld, %ld \n", seg_offset, byte_offset);
    */
    if(unlikely(byte_offset > seg->size)){
        perror("address is not allocated");
        exit(EXIT_FAILURE);
    }
    d_word_t *ret = &seg->words[byte_offset/sizeof(int64_t)];
    return ret;
}
/** Declarations of helper functions
**/
bool tm_read_word(shared_t shared as(unused), tx_t tx as(unused), d_word_t *word as(unused), int64_t const* target as(unused));
bool tm_write_word(shared_t shared as(unused), tx_t tx as(unused), d_word_t *word as(unused), int64_t const* source as(unused));

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size as(unused), size_t align as(unused)) {
    //check that multiple of alignment
    if(unlikely(size%align != 0))
        return invalid_shared;

    //check power of two
    if(unlikely(align & (align - 1)))
        return invalid_shared;
    
    //check max size
    //printf("create\n");

    //allocate first segment  
    segment_t *start = (segment_t *)malloc(sizeof(segment_t));
    //void *v_words = (void *)start->words;
    if(unlikely(posix_memalign((void **)(&start->words), align, size/sizeof(int64_t)*sizeof(d_word_t)))){
        perror("memory allocation failed");
        exit(EXIT_FAILURE);
    }
    //start->control = (ctl_block_t *)malloc(sizeof(ctl_block_t)*size/sizeof(int64_t));
    memset(start->words, 0, size/sizeof(int64_t)*sizeof(d_word_t));
    //REMOVE THIS SAFE???
    for (size_t i = 0; i < size/sizeof(int64_t); ++i)
    {
        atomic_init(&start->words[i].ctrl, 0);
        //atomic_init(&start->control[i].ctrl, 0);
    }
    start->deallocated = false;
    start->size = size;
    start->virtual_address = VIRTUAL_SEG(0);
    //start->next = NULL;
    //start->prev = NULL;
    //allocate dynamic list of segments

    //create batcher
    batcher_t batcher = {0};
    pthread_mutex_init(&batcher.lock, NULL);
    pthread_cond_init(&(batcher.cv), NULL);

    //allocate memory region
    shared_region_t *region = malloc(sizeof(shared_region_t));
    region->alignment = align;
    //region->start = start;
    
    region->segments.size = 4;
    region->segments.length = 1;
    region->segments.array = (segment_t **)malloc(sizeof(segment_t *)*region->segments.size);
    
    region->segments.array[0] = start;
    region->commits = list_new(TByte);
    region->deallocs = list_new(TSeg);
    pthread_rwlock_init(&region->lock, NULL);
    //pthread_mutex_init(&region->lock, NULL);
    //printf("create\n");
    
    return (shared_t)(region);
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared as(unused)) {
    //printf("destroy\n");
    shared_region_t* region = (shared_region_t*)(shared);
    //free all segments
    /*
    segment_t *i = region->start;
    while(i != NULL){
        segment_t *next = i->next;
        free(i);
        i = next;
    }*/
    
    //free all segments
    for(size_t i = 0; i < region->segments.length; i++){
        segment_t *seg = region->segments.array[i];
        /*
        for (size_t i = 0; i < seg->size/sizeof(int64_t); ++i)
        {
            pthread_mutex_destroy(&seg->words[i].lock);
        }*/
        //free(seg->control);
        free(seg->words);
        free(seg);          
    }
    //free uncommited writes
    list_destroy(region->commits);
    /*
    d_byte_list_t *c = region->commits;
    while(c!=NULL){
        d_byte_list_t *next = c->next;
        free(c);
        c = next;
    }*/
    //free uncommited frees
    list_destroy(region->deallocs);
    /*
    seg_list_t *d = region->deallocs;
    while(d!=NULL){
        seg_list_t *next = d->next;
        free(d);
        d = next;
    }*/

    pthread_mutex_destroy(&region->batcher.lock);
    pthread_cond_destroy(&region->batcher.cv);
    pthread_rwlock_destroy(&region->lock);
    free(region);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared as(unused)) {
    shared_region_t* region = (shared_region_t*)(shared);
    return region->segments.array[0]->virtual_address;
    //return region->start->virtual_address;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared as(unused)) {
    shared_region_t* region = (shared_region_t*)(shared);
    return region->segments.array[0]->size;
    //return region->start->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared as(unused)) {
    shared_region_t* region = (shared_region_t*)(shared);
    return region->alignment;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared as(unused), bool is_ro as(unused)) {
    //printf("begin\n");
    shared_region_t* region = (shared_region_t*)(shared);
    transaction_t *t = (transaction_t *)malloc(sizeof(transaction_t));

    t->writes = list_new(TByte);
    t->frees = list_new(TSeg);
    if(is_ro){
        t->access = READ_ONLY;
    }
    else{
        t->access = READ_WRITE;
    }
    //printf("[%p] waiting: \n", (void *)t);
    batcher_enter(&region->batcher);
    //printf("[%p] entered: \n", (void *)t);
    return (tx_t)t;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared as(unused), tx_t tx as(unused)) {
    //printf("end\n");
    shared_region_t *region = (shared_region_t *)shared;
    transaction_t *t = (transaction_t*)tx;

    //defer commits of all the writes
    //LOCK(&region->lock);
    //add transaction writes to commits
    list_concat(region->commits, t->writes);
    list_destroy(t->writes);
    //add transaction frees to deallocs
    list_concat(region->deallocs, t->frees);
    list_destroy(t->frees);
    //UNLOCK(&region->lock);
    batcher_leave(region, &region->batcher, t, false);
    free(t);
    //printf("end: [%p]\n", (void *)tx);
    return true;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
alloc_t tm_alloc(shared_t shared as(unused), tx_t tx as(unused), size_t size as(unused), void** target as(unused)) {
    //printf("alloc\n");
    shared_region_t *region = (shared_region_t *)shared;
    transaction_t *transaction = (transaction_t*)tx;

    //check that multiple of alignment
    if(unlikely(size%region->alignment != 0)){
        printf("alloc: [%p] abort\n", (void *)tx);
        batcher_leave(region, &region->batcher, transaction, true);
        return abort_alloc;
    }

    //allocate a new segment
    segment_t *seg = (segment_t *)malloc(sizeof(segment_t));
    //printf("(alloc %p, size %ld \n)", seg, size);
    if(unlikely(seg == NULL)){
        printf("alloc: [%p]:[%p] nomem_alloc\n", (void *)tx, (void*)seg);
        return nomem_alloc;
    }
    //void *v_words = (void *)seg->words;
    if(unlikely(posix_memalign((void **)(&seg->words), region->alignment, size/sizeof(int64_t)*sizeof(d_word_t)))){
        perror("memory allocation failed");
        exit(EXIT_FAILURE);
    }
    //start->control = (ctl_block_t *)malloc(sizeof(ctl_block_t)*size/sizeof(int64_t));
    memset(seg->words, 0, size/sizeof(int64_t)*sizeof(d_word_t));
    //seg->control = (ctl_block_t *)malloc(sizeof(ctl_block_t)*size/sizeof(int64_t));
    //SAFE TO REMOVE THIS??
    for (size_t i = 0; i < size/sizeof(int64_t); ++i)
    {
        atomic_init(&seg->words[i].ctrl, 0);
        //atomic_init(&start->control[i].epoch, 0);
        //pthread_mutex_init(&seg->words[i].lock, NULL);
    }
        
    seg->deallocated = false;
    seg->size = size;
    seg->virtual_address = VIRTUAL_SEG(region->segments.length);

    *target = seg->virtual_address;

    

    LOCK_WR(&region->lock);
    //if segment list is full, reallocate array
    if(region->segments.length == region->segments.size){
        region->segments.size *= 2;
        segment_t **new_arr = (segment_t **)malloc(sizeof(segment_t*)*region->segments.size);
        memcpy((void *)new_arr, (void *)&region->segments.array[0], sizeof(segment_t*)*region->segments.size);
        //memset(new_arr, 0, sizeof(segment_t *)*region->segments.size);
        //copy each segment pointer to the new array
        /*
        for(size_t i = 0; i < region->segments.length; i++)
            new_arr[i] = region->segments.array[i];
        */
        segment_t **old_arr = region->segments.array;
        region->segments.array = new_arr;
        free(old_arr);
    }
    
    region->segments.array[region->segments.length] = seg;
    region->segments.length++;
    //give address to target
    //*target = seg->virtual_address;
    

    //printf("(alloc (virtual) %p, size %ld \n)", *target, size);

    UNLOCK_RW(&region->lock);
    printf("alloc: [%p]:[%p] success\n", (void *)tx, (void*)seg);
    return success_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared as(unused), tx_t tx as(unused), void* target as(unused)) {
    //printf("free\n");
    shared_region_t *reg = (shared_region_t *)shared;
    transaction_t *transaction = (transaction_t*)tx;

    int seg_index = SEG_PART(target)-1;
    segment_t *seg = reg->segments.array[seg_index];

    //find segment with given virtual address
    /*
    segment_t *seg = reg->start;
    while(seg != NULL){
        if(seg->virtual_address == (v_addr)target)
            break;
        seg = seg->next;
    }
    if(seg == NULL){
        perror("segment does not exist");
        exit(EXIT_FAILURE);
    }
    */
    if(unlikely(reg->segments.array[0] == seg)){
        perror("may not free start");
        exit(EXIT_FAILURE);
    }

    //add free to transaction
    list_append(TSeg, transaction->frees, (void *)seg);
    /*
    int err = 0;
    if(transaction->frees == NULL){
        seg_list_t *element  = (seg_list_t *)malloc(sizeof(seg_list_t*));
        element->self = seg;
        element->next = NULL;
        transaction->frees = element;
    }
    else{
       err = seg_list_append(transaction->frees, seg); 
    }
    if(err == -1){
        perror("free : failed to append");
        exit(EXIT_FAILURE);
    }*/
    //printf("free: [%p]:[%p] success\n", (void *)tx, (void *)target);
    return true;
}
/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t shared as(unused), tx_t tx as(unused), void const* source as(unused), size_t size as(unused), void* target as(unused)) {
    //printf("read\n");
    shared_region_t *region = (shared_region_t *)shared;
    transaction_t *transaction = (transaction_t *)tx;
    //check that size is a multiple of alignment
    if(unlikely(size%region->alignment != 0)){
        perror("bad alignetn");
        exit(EXIT_FAILURE);
    }

    //get words to read from
    v_addr virt_addr = (v_addr)source;
    d_word_t *source_words = get_phys_addr(shared, virt_addr);

    if(unlikely(source_words == NULL)){
        //printf("(read %p)\n", virt_addr);
        perror("read:null adress");
        exit(EXIT_FAILURE);
    }
    int64_t *target_words = (int64_t *)target;
    //segment_t *seg = region->segments.array[SEG_PART(virt_addr)-1];

    for (size_t i = 0; i < size/sizeof(int64_t); ++i)
    {
        //ctl_block_t *ctl = &seg->control[BYTE_PART(virt_addr)/sizeof(int64_t) + i];
        bool cont = tm_read_word(shared, tx, &source_words[i], &target_words[i]);
        if(!cont)
        {
            //printf("read: [%p]:[%p] abort\n", (void *)tx, (void*)virt_addr);
            batcher_leave(region, &region->batcher, transaction, true);
            return false;
        }
    }
    //printf("read [%p]:%d\n", source, *(int *)target);
    //printf("read: [%p]:[%p] success\n", (void *)tx, (void*)virt_addr);
    return true;
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t shared as(unused), tx_t tx as(unused), void const* source as(unused), size_t size as(unused), void* target as(unused)) {
    //printf("write\n");
    shared_region_t *region = (shared_region_t *)shared;
    transaction_t *transaction = (transaction_t *)tx;

    //check that size is a multiple of alignment
    if(unlikely(size%region->alignment != 0)){
        perror("bad alignment");
        exit(EXIT_FAILURE);
    }

    v_addr virt_addr = (v_addr)target;

    //printf("(write %p) : size %ld \n", virt_addr, region->start->size);
    
    d_word_t *target_words = get_phys_addr(region, virt_addr);
    if(unlikely(target_words == NULL)){
        perror("write:null adress");
        exit(EXIT_FAILURE);
    }
    //printf("test\n");
    //segment_t *seg = region->segments.array[SEG_PART(virt_addr)-1];
    //printf("write: [%p]:[%p]\n", target, target_word);
    
    //printf("test\n");
    int64_t *source_words = (int64_t *)source;

    for (size_t i = 0; i < size/sizeof(int64_t); ++i)
    {
        //printf("test\n");
        //ctl_block_t *ctl = &seg->control[BYTE_PART(virt_addr)/sizeof(int64_t) + i];
        bool cont = tm_write_word(shared, tx, &target_words[i], &source_words[i]);
        if(!cont)
        {   
            //printf("write: [%p]:[%p] abort\n", (void *)tx, (void*)virt_addr);
            batcher_leave(region, &region->batcher, transaction, true);
            return false;
        }

    }

    //printf("write[%p]:%d\n",target, *(int *)source);
    //printf("write: [%p]:[%p] success\n", (void *)tx, (void*)virt_addr);
    return true;
}
/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write_word(shared_t shared as(unused), tx_t tx as(unused), d_word_t *word as(unused), int64_t const* source as(unused)){
    if(unlikely((tx == invalid_tx) | (shared == invalid_shared)))
        return false;

    shared_region_t *region = (shared_region_t *)shared;
    transaction_t *transaction = (transaction_t*)tx;
    int64_t tx_id = CTRL_GET_TX(tx);
    int64_t *source_pointer = (int64_t*)source;
    
    
    uint64_t this_epoch = (uint64_t)region->batcher.batch_num;
    uint64_t latest_ctrl = atomic_load(&word->ctrl);
    uint64_t latest_epoch = CTRL_GET_EPOCH(latest_ctrl);
    if(latest_epoch < this_epoch){
        uint64_t cleared = CTRL_SET(0, 0, this_epoch);
        atomic_compare_exchange_strong(&word->ctrl, &latest_ctrl, cleared);
    }
    
    //bool ret;
    uint64_t exp_empty = CTRL_SET(0, 0, this_epoch);
    uint64_t exp_self = CTRL_SET(tx_id, CTRL_RD, this_epoch);
    uint64_t desired = CTRL_SET(tx_id, CTRL_WR, this_epoch);
    atomic_compare_exchange_strong(&word->ctrl, &exp_empty, desired);                         //overwrite if empty        
    bool success = atomic_compare_exchange_strong(&word->ctrl, &exp_self, desired);           //overwrite if read by self only
    if (success || (CTRL_GET_TX(exp_self) == tx_id && CTRL_GET_ACC(exp_self) != CTRL_RD_MUL))    //if overwrite succeded or if this tx already accessed it and no other tx has read from it
    {
        word->write_copy = *source_pointer;
        list_append(TByte, transaction->writes, (void *)word);
        return true;
    }
    return false; 

    /*
    LOCK(&word->lock);

    //if word has not been accessed this epoch, then clear the control
    if(region->batcher.batch_num > control->last_epoch){
        control->last_epoch = region->batcher.batch_num;
        control->access_set = NULL;
        control->control = 0;
        control->accesses = 0;
    }
    //if word has been written to already
    if(control->control & REG_CTL_WR){
        //if it was written to/last read from by this transaction
        if(control->access_set == transaction){
            //write to writable copy
            word->write_copy = *source_pointer;
            ret = true;
        }
        else{
            ret = false;
        }
    }
    else{
        //if the bytes was accessed by some other transaction
        if((control->access_set != transaction && control->access_set != NULL) || (control->control & REG_CTL_RD) ){
            ret = false;
        }
        else{
            //write to writable copy
            word->write_copy = *source_pointer;
            //mark as written
            control->control |= REG_CTL_WR;
            //add to access set
            control->access_set = transaction;
            //byte->control |= REG_CTL_RD;
            //shared_mem->bytes[index].accesses++;
            
            ret = true;
        }
    }
    UNLOCK(&word->lock);
    //printf("[%d]write\n", gettid());

    //add byte write to transaction
    list_append(TByte, transaction->writes, (void *)word);
    return ret;
    */
}
/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param index  the location of the word inside the shared memory region
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read_word(shared_t shared as(unused), tx_t tx as(unused), d_word_t *word as(unused), int64_t const* target as(unused)){
    //printf("read\n");
    
    if(unlikely((tx == invalid_tx) | (shared == invalid_shared)))
        return false;

    shared_region_t *region = (shared_region_t *)shared;
    transaction_t *transaction = (transaction_t*)tx;
    uint64_t tx_id = CTRL_GET_TX(tx);
    uint64_t *target_word = (uint64_t *)target;

    if(transaction->access == READ_ONLY){
        //read readable copy
        *target_word = word->read_copy;
        return true;
        //printf("read only\n");
    }
    
    uint64_t this_epoch = (uint64_t)region->batcher.batch_num;
    uint64_t latest_ctrl = atomic_load(&word->ctrl);
    uint64_t latest_epoch = CTRL_GET_EPOCH(latest_ctrl);
    if(latest_epoch < this_epoch){
        uint64_t cleared = CTRL_SET(0, 0, this_epoch);
        atomic_compare_exchange_strong(&word->ctrl, &latest_ctrl, cleared);
    }
    
    uint64_t exp_empty = CTRL_SET(0, 0, this_epoch);;
    uint64_t desired = CTRL_SET(tx_id, CTRL_WR, this_epoch);
    bool first = atomic_compare_exchange_strong(&word->ctrl, &exp_empty, desired);      //overwrites if empty
    if(first || CTRL_GET_TX(exp_empty) == tx_id){                                         //if was empty or occupied by this transaction
        *target_word = word->write_copy;                                                  
        return true;
    }
    else if(CTRL_GET_ACC(exp_empty)){                                                           //if other transaction was first but has not yet written
        uint64_t exp_read = CTRL_SET(CTRL_GET_TX(exp_empty), CTRL_RD, this_epoch);                           //expect that transaction has read only
        uint64_t desired_mread = CTRL_SET(CTRL_GET_TX(exp_empty), CTRL_RD_MUL, this_epoch);                  //update to multiple reads
        bool success = atomic_compare_exchange_strong(&word->ctrl, &exp_read, desired_mread); //try to update
        if(success || CTRL_GET_ACC(exp_read) == CTRL_RD_MUL){                                   //if update success or was already updated to multple reads
            *target_word = word->read_copy;                                           
            return true;
        }
    }
    return false;


    //bool ret = false;
    /*
    LOCK(&word->lock);
    //if word has not been accessed this epoch, then clear the control
    if(region->batcher.batch_num > control->last_epoch){
        control->last_epoch = region->batcher.batch_num;
        control->access_set = NULL;
        control->control = 0;
        control->accesses = 0;
    }
    //if transaction is read-only, read the readable copy
    if(transaction->access == READ_ONLY){
        //read readable copy
        *target_word = word->read_copy;
        ret = true;
        //printf("read only\n");
    }
    //if the transaction writes
    else{
        
        //if the word has been written to this epoch
        if(control->control & REG_CTL_WR){
            //printf("if\n");
            //if the bytes was accessed by this transaction this epoch
            if(control->access_set == transaction){
                //printf("ifif\n");
                //read wriateble copy
                *target_word = word->write_copy;
                ret = true;
            }
            //if the word was written to by some other transaction this epoch
            else{
                //printf("ifelse\n");
                ret = false;
            }
        }
        //if the word has not been written to this epoch
        else{
            //printf("else\n");
            //read readable copy
            *target_word = word->read_copy;
            
            //return true;
            //mark that this has been read by another value
            if(control->access_set != transaction && control->access_set != NULL)
                control->control |= REG_CTL_RD;
                //control->control |= REG_CTL_RD && (((control->access_set & ~transaction) && (control->access_set & ~0)) && !(control->control & REG_CTL_WR))
                //control->control = control->control | REG_CTL_RD && (((control->access_set & ~transaction)) && !(control->control & REG_CTL_WR))
                //control->control = control->control | REG_CTL_RD && (((control->access_set & ~transaction)) && !(control->control & REG_CTL_WR))

            //add to access set
            control->access_set = transaction;
            
            //shared_mem->bytes[index].accesses++;
            ret = true;
        }

    }
    UNLOCK(&word->lock);
    return ret;*/
}
/*
void print_list(list_t *l){
    if(l->T != TByte)
        return;
    printf("{length=%ld ,first=[%p], last=[%p]\n ", l->length, l->first, l->last);
    //printf("{");
    element_t *e1 = l->first;
    if(e1 == NULL){
            printf("NULL\n");
    }
    else{
        d_byte_t *byte1 = (d_byte_t *)e1->self;
        printf("\t{ctr=%d, wr=%d, rd=%d, acc=%d, set=[%p]}\n ", byte1->control, byte1->write_copy, byte1->read_copy, byte1->accesses, byte1->access_set);             
        element_t *e2 = l->last;
        d_byte_t *byte2 = (d_byte_t *)e2->self;
        printf("\t{ctr=%d, wr=%d, rd=%d, acc=%d, set=[%p]}\n ", byte2->control, byte2->write_copy, byte2->read_copy, byte2->accesses, byte2->access_set);               
    }

    for (size_t i = 0; i < l->length; ++i)
    {
        d_byte_t *byte = (d_byte_t *)e->self;
        if(e == NULL){
            printf("NULL");
        }
        else{
                printf("{\tctr=%d, wr=%d, rd=%d, acc=%d, set=[%p]\n} ", byte->control, byte->write_copy, byte->read_copy, byte->accesses, byte->access_set);            
        }
    
        e = e->next;
    }
    //printf("}}\n");
}*/
