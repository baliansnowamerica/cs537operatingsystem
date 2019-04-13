#include "types.h"
#include "user.h"
#include "stat.h"
#include "fcntl.h"
#include "param.h"
#include "x86.h"
#define PGSIZE 4096

// global variable to store start addres of each thread
void *addthread[NPROC];

// this routine should call malloc() to create a new user stack, 
// use clone() to create the child thread and get it running. 
// It returns the newly created PID to the parent and 0 to the child, -1 otherwise.
int 
thread_create(void (*start_routine)(void *, void *), void *arg1, void *arg2)
{
  // this routine should call malloc() to create a new user stack 
  void *startstack = malloc(2*PGSIZE); //printf(1, "malloc stack: %d\n", startstack);
  if (startstack == 0) return -1;
  
  uint *stack = (uint *) startstack; // printf(1, "stack: %d\n", stack);  
  uint offset = (uint) stack % PGSIZE; // printf(1, "offset: %d\n", offset);
  stack = (uint *) ((uint) stack + (4096 - offset)); // printf(1, "offset stack: %d\n", stack);

  int threadid = clone(start_routine, arg1, arg2, stack);
  if (threadid != -1) {
    addthread[threadid % NPROC] = startstack;
  }
  
  // use clone() to create the child thread and get it running
  // returns the newly created PID to the parent, -1 otherwise
  return threadid;
}

// int thread_join() call should also be created, which calls the underlying join() system call, 
// frees the user stack, and then returns. 
// It returns the waited-for PID (when successful), -1 otherwise.
int 
thread_join(void) 
{
  void *stack;
  int threadid = join(&stack);
  if(stack == NULL) return threadid;
  else {
    free(addthread[threadid % NPROC]);
  }
  
  return threadid;
}
