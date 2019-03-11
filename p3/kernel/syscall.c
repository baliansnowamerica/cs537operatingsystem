#include "types.h"
#include "defs.h"
#include "param.h"
#include "mmu.h"
#include "proc.h"
#include "x86.h"
#include "syscall.h"
#include "sysfunc.h"

// User code makes a system call with INT T_SYSCALL.
// System call number in %eax.
// Arguments on the stack, from the user call to the C
// library system call function. The saved user %esp points
// to a saved program counter, and then the first argument.

// Fetch the int at addr from process p.
int
fetchint(struct proc *p, uint addr, int *ip)
{ 
  // if addr is in the first unmapping pages, return -1
  if (addr < PGSIZE) {
    cprintf("fetchint: proc %s, pid %d first 4 unmapping pages \n", p->name, p->pid);
    return -1;  
  }

  // if addr is in shared pages, and whether that shared page is mapped
  if ((uint) PGROUNDDOWN(addr) > p->numsh && addr < PGSIZE*4) {
    cprintf("fetchint addr: proc %s, pid %d unmapping shared pages \n", p->name, p->pid);
    return -1;
  }

  // if addr+4 is in shared pages, and whether that shared page is mapped
  // addr+4 > ? ensures that whole of the 32 bit instruction lies within the process's size limit
  if ((uint) PGROUNDDOWN(addr+4) > p->numsh && addr + 4 < PGSIZE*4) {
    cprintf("fetchint addr+4: proc %s, pid %d unmapping shared pages \n", p->name, p->pid);
    return -1;
  }

  // if addr is larger than USERTOP = 0xA0000 = 655360, return -1
  if (addr >= USERTOP || addr + 4 > USERTOP) {
    cprintf("fetchint: proc %s, pid %d larger than USERTOP \n", p->name, p->pid);
    return -1;  
  }

  // if addr is in the unmapping pages between heap and stack, return -1
  if (addr >= p->sz && addr < p->stack_end) {
    cprintf("fetchint addr: proc %s, pid %d heap-stack unmapping pages \n", proc->name, proc->pid);
    return -1; 
  }

  // if addr+4 is in the unmapping pages between heap and stack, return -1
  if (addr + 4 > p->sz && addr + 4 < p->stack_end) {
    cprintf("fetchint addr+4: proc %s, pid %d heap-stack unmapping pages \n", proc->name, proc->pid);
    return -1; 
  }

  // original setting of xv6
  // if(addr >= p->sz || addr+4 > p->sz) return -1;

  *ip = *(int*)(addr);
  return 0;
}

// Fetch the nul-terminated string at addr from process p.
// Doesn't actually copy the string - just sets *pp to point at it.
// Returns length of string, not including nul.
int
fetchstr(struct proc *p, uint addr, char **pp)
{ 
  char *s, *ep;

  // if addr is in the first unmapping pages, return -1
  if (addr < PGSIZE) {
    cprintf("fetchstr: proc %s, pid %d first 4 unmapping pages \n", p->name, p->pid);
    return -1;  
  }

  // if addr is in shared pages, and whether that shared page is mapped
  if ((uint) PGROUNDDOWN(addr) > p->numsh && addr < PGSIZE*4) {
    cprintf("fetchstr: proc %s, pid %d unmapping shared pages \n", p->name, p->pid);
    return -1;
  }

  // if addr is larger than USERTOP = 0xA0000 = 655360, return -1
  if (addr >= USERTOP) {
    cprintf("fetchstr: proc %s, pid %d larger than USERTOP \n", p->name, p->pid);
    return -1;  
  }

  // if addr is in the unmapping pages between heap and stack, return -1
  if (addr >= p->sz && addr < p->stack_end) {
    cprintf("fetchstr: proc %s, pid %d heap-stack unmapping pages \n", proc->name, proc->pid);
    return -1; 
  }

  // original setting of xv6
  // if(addr >= p->sz) return -1;

  // where the string starts
  *pp = (char*)addr;
  // if addr is in code/heap then last legal addr is p->sz.  
  if (addr < p->sz) 
    ep = (char*) p->sz;
  // otherwise, addr is in stack and last legal addr is USERTOP.
  else
    ep = (char*) USERTOP;
  // find where the string ends by "null-terminator 0"
  for(s = *pp; s < ep; s++)
    if(*s == 0)
      return s - *pp;

  return -1;
}

// Fetch the nth 32-bit system call argument.
int
argint(int n, int *ip)
{
  return fetchint(proc, proc->tf->esp + 4 + 4*n, ip);
}

// Fetch the nth word-sized system call argument as a pointer
// to a block of memory of size n bytes.  Check that the pointer
// lies within the process address space.
int
argptr(int n, char **pp, int size)
{ 
  // prepare an integer variable i, assign it with the address of the nth argument of user's initial calling
  int i;
  if(argint(n, &i) < 0)
    return -1;

  // ==============================================================================================
  // check whether os can assign enough memory space for user's nth argument
  // ==============================================================================================

  // cprintf("argptr i: %d, code_end: %d, sz: %d, stack_end: %d\n", (uint) i, proc->code_end, proc->sz, proc->stack_end);
  // if i is in the first unmapping pages, return -1
  if ((uint) i < PGSIZE) {
    cprintf("argptr: proc %s, pid %d 4 unmapping pages \n", proc->name, proc->pid);
    return -1;  
  }

  // if i is in shared pages, and whether that shared page is mapped
  if ((uint) PGROUNDDOWN(i) > proc->numsh && i < PGSIZE*4) {
    cprintf("argptr i: proc %s, pid %d unmapping shared pages \n", proc->name, proc->pid);
    return -1;
  }

  // if i+size is in shared pages, and whether that shared page is mapped
  if ((uint) PGROUNDDOWN(i + size) > proc->numsh && i + size < PGSIZE*4) {
    cprintf("argptr i+size: proc %s, pid %d unmapping shared pages \n", proc->name, proc->pid);
    return -1;
  }

  // if i or i +size is larger than USERTOP = 0xA0000 = 655360, return -1
  if ((uint) i >= USERTOP || (uint) (i+size) > USERTOP) {
    cprintf("argptr: proc %s, pid %d larger than USERTOP \n", proc->name, proc->pid);
    return -1;  
  }

  // if i is in the unmapping pages between heap and stack, return -1
  if ((uint) i >= proc->sz && (uint) i < proc->stack_end) {
    cprintf("argptr i: proc %s, pid %d heap-stack unmapping pages \n", proc->name, proc->pid);
    return -1; 
  }

  // if i+size is in the unmapping pages between heap and stack, return -1
  if ((uint) i + size > proc->sz && (uint) i +size < proc->stack_end) {
    cprintf("argptr i+size: proc %s, pid %d heap-stack unmapping pages \n", proc->name, proc->pid);
    return -1; 
  }

  // original setting of xv6
  // if((uint)i >= proc->sz || (uint)i+size > proc->sz) return -1;

  // ==============================================================================================
  // check done
  // ==============================================================================================
  
  //assign the output pointer char **pp to the address hold by i.
  *pp = (char*) i;
  return 0;
}

// Fetch the nth word-sized system call argument as a string pointer.
// Check that the pointer is valid and the string is nul-terminated.
// (There is no shared writable memory, so the string can't change
// between this check and being used by the kernel.)
int
argstr(int n, char **pp)
{
  int addr;
  if(argint(n, &addr) < 0)
    return -1;
  return fetchstr(proc, addr, pp);
}

// syscall function declarations moved to sysfunc.h so compiler
// can catch definitions that don't match

// array of function pointers to handlers for all the syscalls
static int (*syscalls[])(void) = {
[SYS_chdir]   sys_chdir,
[SYS_close]   sys_close,
[SYS_dup]     sys_dup,
[SYS_exec]    sys_exec,
[SYS_exit]    sys_exit,
[SYS_fork]    sys_fork,
[SYS_fstat]   sys_fstat,
[SYS_getpid]  sys_getpid,
[SYS_kill]    sys_kill,
[SYS_link]    sys_link,
[SYS_mkdir]   sys_mkdir,
[SYS_mknod]   sys_mknod,
[SYS_open]    sys_open,
[SYS_pipe]    sys_pipe,
[SYS_read]    sys_read,
[SYS_sbrk]    sys_sbrk,
[SYS_sleep]   sys_sleep,
[SYS_unlink]  sys_unlink,
[SYS_wait]    sys_wait,
[SYS_write]   sys_write,
[SYS_uptime]  sys_uptime,
[SYS_shmget]  sys_shmget,
};

// Called on a syscall trap. Checks that the syscall number (passed via eax)
// is valid and then calls the appropriate handler for the syscall.
void
syscall(void)
{
  int num;
  
  num = proc->tf->eax;
  if(num > 0 && num < NELEM(syscalls) && syscalls[num] != NULL) {
    proc->tf->eax = syscalls[num]();
  } else {
    cprintf("%d %s: unknown sys call %d\n",
            proc->pid, proc->name, num);
    proc->tf->eax = -1;
  }
}

// this fucntion is system call shared memory get (shmget)
// int sys_shmget(void) is used to read inputs from user mode into kernel
// in kernel mode, we use (int) shmget(page_number) to actuall do something
// return 0 if the argument doesn't exist
// return 0 if the argument is not 0, 1, or 2, because there are up to 3 pages that can be shared
int sys_shmget(void) {
  int page_number;
  if (argint(0, &page_number) < 0) 
    return 0; 

  if (page_number < 0 || 2 < page_number)
    return 0;

  return (int) shmget(page_number);
}
