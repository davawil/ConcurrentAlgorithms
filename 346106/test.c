/*my test program*/
#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>

#include "tm.h"
#define SIZE 10

//test
int test_single_pointer_inc(shared_t shared, int size);
int test_dual_pointer_inc(shared_t shared, int size);

//operation threads
void *thread_inc_wr(void *info);


typedef struct thread_info{
	shared_t shared;
	int size;
	int seed;
	void *ret;
}t_info_t;

int main(void){
	//create the shared memory region
	size_t size = 10;
	shared_t shared = tm_create(sizeof(int)*size, sizeof(int));
	//SINGLE THREADED TEST
	if(test_single_pointer_inc(shared, size) != 1){
		printf("test single pointer increment failed\n");
	}
	else{
		printf("test single pointer increment success\n");
	}
	//2-THREADED TEST
	if(test_dual_pointer_inc(shared, size) != 1){
		printf("test dual pointer increment failed\n");
	}
	else{
		printf("test dual pointer increment success\n");
	}
}

/*performs pointer arithmetic in the shared region
*returns -1 on failure, 0 on abort and 1 on success
*/
int test_single_pointer_inc(shared_t shared, int size){
	tx_t tx = tm_begin(shared, false);
	
	void *p;
	bool abort = false;
	//allocates an array to do pointer arithmetic on
	alloc_t ret_alloc = tm_alloc(shared, tx, sizeof(int)*size, &p);
	if(ret_alloc == abort_alloc)
		abort = true;
	if(ret_alloc == nomem_alloc)
		return -1;

	int i_read;
	//write and then read to each int
	for (int i_write = 0; i_write < size; ++i_write)
	{	
		//write value of i_write to p
		tm_write(shared, tx, (void *)&i_write, sizeof(int), p);
		//read value of p to i_read
		tm_read(shared, tx, p, sizeof(int), (void *)&i_read);
		//check that value read is the value written
		assert(i_read == i_write);
		//perform pointer increment
		p = (int *)p + 1;
	}
	if (!tm_end(shared, tx))
		abort = true;

	if(abort)
		return 0;

	return 1;
}

int test_dual_pointer_inc(shared_t shared, int size){
	tx_t tx = tm_begin(shared, false);

	void *p;
	bool abort = false;

	//try to allocate until successful
	while(abort){
		abort = false;
		alloc_t ret_alloc = tm_alloc(shared, tx, sizeof(int)*SIZE, &p);
		if(ret_alloc == abort_alloc)
			abort = true;		
		if(ret_alloc == nomem_alloc)
			return -1;
	}
	tm_end(shared, tx);

	void *ret1 = malloc(sizeof(bool));
	void *ret2 = malloc(sizeof(bool));
	
	pthread_t tid1;
	t_info_t info1 = {shared, size, 0, ret1};
	pthread_create(&tid1, NULL, &thread_inc_wr, (void *)&info1);
	pthread_t tid2;
	t_info_t info2 = {shared, size, 10, ret2};
	pthread_create(&tid2, NULL, &thread_inc_wr, (void *)&info2);
	
	pthread_join(tid1, ret1);
	pthread_join(tid2, ret2);
	
	if(abort || (*(bool *)ret1) || (*(bool *)ret2))
		return 0;

	free(ret1);
	free(ret2);

	return 1;
}
//BATCHER NOT USED YET, THIS CAUSES THREADS TO LOOP FOREVER BEACUSE THE BATCH NEVER ENDS
void *thread_inc_wr(void *arg){
	t_info_t *info = (t_info_t *)arg;

	shared_t shared = info->shared;
	int size = info->size;
	int seed = info->seed;
	bool *ret = (bool *)(info->ret);
	bool abort = false;
	void *p;

	tx_t tx = tm_begin(shared, false);
	int i_write, i_read;
	for (int i = 0; i < size; ++i)
	{	
		do{		
			i_write = i + seed;

			//write value of i_write to p
			abort = !tm_write(shared, tx, (void *)&i_write, sizeof(int), p);
			//read value of p to i_read
			abort = abort || !tm_read(shared, tx, p, sizeof(int), (void *)&i_read);
		}while(abort);
		//check that value read is the value written
		assert(i_read == i_write);
		//perform pointer increment
		p = (int *)p + 1;
	}
	tm_end(shared, tx);
	return (void *)abort;
}
