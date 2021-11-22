/*my test program*/
#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>

#include "tm.h"
#define SIZE 10

//test
int test_single_pointer_inc(shared_t shared, int size);
int test_dual_pointer_inc(shared_t shared, int size, int num_threads);

//operation threads
void *thread_inc_wr(void *info);


typedef struct thread_info{
	shared_t shared;
	int size;
	int seed;
	void *ret;
	void *alloc;
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
	int res = test_dual_pointer_inc(shared, 1, 5);
	if(res == 0){
		printf("test dual pointer increment failed\n");
	}
	else if(res == -1){
		printf("test dual pointer increment error\n");
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

int test_dual_pointer_inc(shared_t shared, int size, int num_threads){
	tx_t tx = tm_begin(shared, false);

	void *p;
	bool abort = true;

	//try to allocate until successful
	while(abort){
		abort = false;
		alloc_t ret_alloc = tm_alloc(shared, tx, sizeof(int)*size, &p);
		if(ret_alloc == abort_alloc)
			abort = true;		
		if(ret_alloc == nomem_alloc)
			return -1;
	}
	if(abort){
		perror("test_dual_pointer_inc failed to allocate");
		return 0;
	}
	tm_end(shared, tx);

	bool ret[num_threads];
	pthread_t tids[num_threads];
	for (int i = 0; i < num_threads; ++i)
	{
		t_info_t info = {shared, size, i, (void *)&ret[i], p};
		if(pthread_create(&tids[i], NULL, &thread_inc_wr, (void *)&info) != 0)
			return -1;
	}
	for (int i = 0; i < num_threads; ++i)
	{
		pthread_join(tids[i], (void *)&ret[i]);
		if (!ret[i])
			return 0;
	}
	return 1;
}

void *thread_inc_wr(void *arg){
	t_info_t *info = (t_info_t *)arg;
	int back_off = 1;

	shared_t shared = info->shared;
	int size = info->size;
	int seed = info->seed;
	void *p = info->alloc;
	bool *ret = (bool *)(info->ret);
	bool cont = false;

	while(!cont)
	{
		tx_t tx = tm_begin(shared, false);
		int i_write, i_read;
		for (int i = 0; i < size; ++i)
		{	
			i_write = i + seed;
	
			//write value of i_write to p
			cont = tm_write(shared, tx, (void *)&i_write, sizeof(int), p);
			//read value of p to i_read
			cont = cont && tm_read(shared, tx, p, sizeof(int), (void *)&i_read);
			if(cont){
				//check that value read is the value written
				printf("read: %d write: %d \n", i_read, i_write);
				assert(i_read == i_write);
			}
			//perform pointer increment
			p = (int *)p + 1;
		}
		if(!cont){
			printf("abort\n");
		}else{
			printf("cont\n");
		}
		//avoids livelock
		tm_end(shared, tx);
		//back_off += info->seed;
		//sleep((float)back_off/100);
	}	
	return (void *)cont;
}