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

// -------------------------------------------------------------------------- //

#define REG_CTL_WR (1<<0)       //indicates wether bytes has been written to this epoch
#define REG_CTL_RD (1<<1)       //indicates wether bytes as been read from this epoch
#define REG_CTL_VALID (1<<2)    //indicates which copy holds the readable value (1=read_copy, 0=write_copy)

typedef pthread_mutex_t lock_t;
#define LOCK(x) pthread_mutex_lock(x)
#define UNLOCK(x) pthread_mutex_unlock(x)

typedef struct batcher{
    int batch_num;               //number of the current batch
    int current_threads;         //number of threads in the current batch
    int waiting_threads;         //number of threads that are blocked
    int wakeups;                 //pending wakeups
    lock_t lock;
}batcher_t;

typedef char* mem_location_t;

#define THREAD_STATUS_BLOCKED 0
#define THREAD_STATUS_RUNNING 1

#define VIRTUAL_HEAP_START 0x00000000
//internal implemention of tx_t
//Each thread can only run one transaction AT A TIME
typedef struct transaction{
    enum {READ_ONLY, READ_WRITE} access;
}transaction_t;

typedef struct duplicate_byte{
    char control;                 //control array 
    char write_copy;              //write array  
    char read_copy;               //read array
    lock_t lock;
    int accesses;                 //number of accesses
    transaction_t* access_set;    //the latest accessed transaction
}d_byte_t;

typedef void* v_addr;

typedef struct segment{
    struct segment *prev;
    struct segment *next;
    v_addr virtual_address;        //the virtual starting address of the segment
    size_t size;
    d_byte_t bytes[];      //array of bytes
}segment_t;

struct segment_header{
    struct segment *prev;
    struct segment *next;
    v_addr virtual_address;        //the virtual starting address of the segment
    size_t size;
};

//each shared region has a virtual address space
typedef struct region{
    batcher_t batcher;
    size_t alignment;
    lock_t lock;
    segment_t *start;
}shared_region_t;

void region_clear_controls(shared_region_t *region){
    segment_t *seg = region->start;
    while(seg != NULL){
        for (int i = 0; i < seg->size; ++i)
        {
            seg->bytes[i].control = 0;
            seg->bytes[i].accesses = 0;
            seg->bytes[i].access_set = NULL;
            //TODO: SWAP READ-COPY AND WRITE-COPY
        }
        seg = seg->next;
    }
}

//transaction "thread" tries to enter a batch in "batcher" 
void batcher_enter(shared_region_t *region, batcher_t *batcher){
    LOCK(&batcher->lock);
    //if the batch is empty, the thread may enter, else wait for wake-up
    if(batcher->current_threads == 0){
        batcher->current_threads = 1;
        UNLOCK(&batcher->lock);
    }else{
        batcher->waiting_threads++;
        UNLOCK(&batcher->lock);
        //stops new threads while waiting threads are being woken up
        while(!batcher->wakeups > 0){
        }
        //wait until batch is complete
        while(batcher->wakeups == 0){
        }
        LOCK(&batcher->lock);
        batcher->wakeups--;
        LOCK(&batcher->lock);
    }
}
void batcher_leave(shared_region_t *region, batcher_t *batcher){
    LOCK(&batcher->lock);
    batcher->current_threads--;
    if(batcher->current_threads == 0){
        batcher->batch_num++;
        batcher->current_threads = batcher->waiting_threads;
        batcher->wakeups = batcher->waiting_threads;
        batcher->waiting_threads = 0;
        region_clear_controls(region);
        
    }
    UNLOCK(&batcher->lock);
}

/*get the virtual address given a d_byte_t address (physical)*/

v_addr *get_virtual_addr(shared_region_t *region, d_byte_t *byte){
    segment_t *seg = region->start;
    while(seg != NULL){
        //find the correct segment
        if(byte >= &(seg->bytes[0]) && byte < &(seg->bytes[0]) + seg->size * sizeof(d_byte_t)){
            //translate bytes adress to virtual adress
            size_t phys_offset = (size_t)(byte - &(seg->bytes[0]));
            size_t phys_alignment = sizeof(d_byte_t)*region->alignment;
            return seg->virtual_address + phys_offset/phys_alignment;
        }
        seg = seg->next;
    }
    return NULL;
}

/*get the d_byte_t address (physical) given a virtual address*/
d_byte_t *get_phys_addr(shared_region_t *region, v_addr virt_addr){
    segment_t *seg = region->start;
    while(seg != NULL){
        //if virt_addr is within address space of segment i,
        if(virt_addr >= seg->virtual_address && virt_addr < seg->virtual_address + seg->size){
            //translate virtual address to a bytes adress
            size_t virt_offset = (size_t)(virt_addr - seg->virtual_address);
            //size_t phys_alignment = sizeof(d_byte_t)*region->alignment;
            return &(seg->bytes[virt_offset]);
        }
        seg = seg->next;
    }
    return NULL;
}
typedef struct semaphore{
    lock_t lock;
    int value;
}semaphore_t;
/*
//set semaphore to a certain value
void semaphore_set(semaphore_t *s, int val){
    LOCK(s->lock);
    s->value = val;
    UNLOCK(s->lock);
}
void semaphore_aquire(semaphore_t *s){
    //wait for available resource
    while(s->value == 0);
    LOCK(s->lock);
    s->value--;
    UNLOCK(s->lock);
}
*/

/** Declarations of helper functions
**/
bool tm_read_byte(shared_t shared as(unused), tx_t tx as(unused), d_byte_t *byte, char const* target);
bool tm_write_byte(shared_t shared as(unused), tx_t tx as(unused), d_byte_t *byte, char const* source);

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size as(unused), size_t align as(unused)) {
    //check that multiple of alignment
    if(size%align != 0)
        return invalid_shared;
    //check if power of 2 by counting number of set bits
    int set_bits = 0;
    for(size_t i = 0; i< sizeof(size_t); i++){
        set_bits += (align>>i) & 1;
        if(set_bits > 1)
            return invalid_shared;
    }
    //check max size

    //allocate start segment
    segment_t *start = malloc(sizeof(segment_t) + size * sizeof(d_byte_t));
    start->size = size;
    start->virtual_address = VIRTUAL_HEAP_START;
    start->next = NULL;
    start->prev = NULL;
    memset(start->bytes, 0, size*sizeof(d_byte_t));

    //create batcher
    batcher_t batcher = {0, 0, 0, 0, 0};
    pthread_mutex_init(&batcher.lock, NULL);

    //allocate memory region
    shared_region_t *region = malloc(sizeof(shared_region_t));
    region->alignment = align;
    region->start = start;
    pthread_mutex_init(&region->lock, NULL);
    
    return (shared_t)(region);
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared as(unused)) {
    shared_region_t* region = (shared_region_t*)(shared);
    //free all segments
    segment_t *i = region->start;
    while(i != NULL){
        segment_t *next = i->next;
        free(i);
        i = next;
    }
    pthread_mutex_destroy(&region->lock);
    free(region);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared as(unused)) {
    shared_region_t* region = (shared_region_t*)(shared);
    return &(region->start);
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared as(unused)) {
    shared_region_t* region = (shared_region_t*)(shared);
    return region->start->size;
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
    shared_region_t* region = (shared_region_t*)(shared);
    transaction_t *t = malloc(sizeof(transaction_t));
    if(is_ro){
        t->access = READ_ONLY;
    }
    else{
        t->access = READ_WRITE;
    }
    batcher_enter(region, &region->batcher);
    return (tx_t)t;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared as(unused), tx_t tx as(unused)) {
    shared_region_t *region = (shared_region_t *)shared; 
    batcher_leave(region, &region->batcher);
    tx_t* t = (tx_t*)tx;
    free(t);
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
    shared_region_t *region = (shared_region_t *)shared;
    //check that size is a multiple of alignment
    if(size%region->alignment != 0)
        return false;

    //get byte to read from
    v_addr virt_addr = (v_addr)source;
    d_byte_t *source_byte = get_phys_addr(shared, virt_addr);

    char *target_byte = (char *)target;

    for (size_t i = 0; i < size; ++i)
    {
        bool cont = tm_read_byte(shared, tx, &source_byte[i], &target_byte[i]);
        if(!cont)
        {
            return false;
        }
    }
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
    shared_region_t *region = (shared_region_t *)shared;

    //check that size is a multiple of alignment
    if(size%region->alignment != 0)
        return false;

    v_addr virt_addr = (v_addr)target;

    d_byte_t *target_byte = get_phys_addr(region, virt_addr);

    char *source_byte = (char *)source;

    for (size_t i = 0; i < size; ++i)
    { 
        bool cont = tm_write_byte(shared, tx, &target_byte[i], &source_byte[i]);
        if(!cont)
        {         
            return false;
        }
    }
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
    shared_region_t *region = (shared_region_t *)shared;

    LOCK(&region->lock);
    //check that multiple of alignment
    if(size% region->alignment != 0)
        return abort_alloc;

    //allocate a new segment
    segment_t *seg = malloc(sizeof(segment_t) + size*sizeof(d_byte_t));
    if(seg == NULL)
        return nomem_alloc;

    memset(&seg->bytes[0], 0, size*sizeof(d_byte_t));

    //find head segment of region
    segment_t *head = region->start;
    while(head->next != NULL){
        head = head->next;
    }
    //add new segment to chain
    head->next = seg;
    //set segments fields
    seg->next = NULL;
    seg->prev = head;
    seg->size = size;
    //the virtual address of the new segment is the first available byte
    seg->virtual_address = head->virtual_address + head->size;

    for(int i = 0; i <seg->size; i++)
        pthread_mutex_init(&seg->bytes[i].lock, NULL);
    
    //give address to target
    *target = seg->virtual_address;

    UNLOCK(&region->lock);
    return success_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared as(unused), tx_t tx as(unused), void* target as(unused)) {
    segment_t *seg = (segment_t *)(target);
    shared_region_t *reg = (shared_region_t *)shared;
    LOCK(&reg->lock);
    if(reg->start == seg)
        return false;

    //re-attach links
    segment_t *prev = seg->prev;
    segment_t *next = seg->next;
    prev->next = next;
    next->prev = prev;

    for(int i = 0; i <seg->size; i++)
        pthread_mutex_destroy(&seg->bytes[i].lock);

    free(seg);
    UNLOCK(&reg->lock);
    return false;
}
/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write_byte(shared_t shared as(unused), tx_t tx as(unused), d_byte_t *byte, char const* source){
    if((tx == invalid_tx) | (shared == invalid_shared))
        return false;

    shared_region_t *region = (shared_region_t *)shared;
    transaction_t *transaction = (transaction_t*)tx;
    char *source_pointer = (char*)source;

    bool ret;

    LOCK(&byte->lock);
    //if bytes has been written to already
    if(byte->control & REG_CTL_WR){
        //if it was written to/last read from by this transaction
        if(byte->access_set == transaction){
            //write to writable copy
            byte->write_copy = *source_pointer;
            ret = true;
        }
        else{
            ret = false;
        }
    }
    else{
        //if the bytes was "accessed" by some other transaction
        if(byte->access_set != transaction && (byte->control & REG_CTL_RD)){
            ret = false;
        }
        else{
            //write to writable copy
            byte->write_copy = *source_pointer;
            //add to access set
            byte->access_set = transaction;
            //byte->control |= REG_CTL_RD;
            //shared_mem->bytes[index].accesses++;
            //mark as written
            byte->control |= REG_CTL_WR;
            ret = true;
        }
    }
    UNLOCK(&byte->lock);
    return ret;
}
/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param index  the location of the word inside the shared memory region
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read_byte(shared_t shared as(unused), tx_t tx as(unused), d_byte_t *byte, char const* target){
    if((tx == invalid_tx) | (shared == invalid_shared))
        return false;

    shared_region_t *shared_mem = (shared_region_t *)shared;
    transaction_t *transaction = (transaction_t*)tx;
    char *target_byte = (char *)target;

    bool ret = false;
    LOCK(&byte->lock);
    //if transaction is read-only, read the readable copy
    if(transaction->access == READ_ONLY){
        //read readable copy
        *target_byte = byte->read_copy;
        ret = true;
    }
    //if the transaction writes
    else{
        //if the bytes has been written to this epoch
        if(byte->control & REG_CTL_WR){
            //if the bytes was accessed by this transaction this epoch
            if(byte->access_set == transaction){
                //read wriateble copy
                *target_byte = byte->write_copy;
                ret = true;
            }
            //if the bytes was written to by some other transaction this epoch
            else{
                ret = false;
            }
        }
        //if the bytes has not been written to this epoch
        else{
            //read readable copy
            *target_byte = byte->read_copy;
            //add to access set
            byte->access_set = transaction;
            byte->control |= REG_CTL_RD;
            //shared_mem->bytes[index].accesses++;
            ret = false;
        }

    }
    UNLOCK(&byte->lock);
    return ret;
}

