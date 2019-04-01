#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "mapreduce.h"

// a value linkedlist
typedef struct node_t {
    char *value;
    struct node_t *next;
} values;

// global getter current value
values *gettercv = NULL;

// a pair starts with a key and a value
// but value can keep growing for a pair
typedef struct dict_t {
    char *key;
    values *value;
} pair;

pair mapping[20000];
int pairsize = 0;

/**
 * MR_Emit recevies a key-value pair, and stores this pair in a data structure.
 * If a key is existed in the data structure, then MR_Emit updates the value.
 * 
 * The meaning of key-value pair could be any.
 * One of a possible meaning could be "a specific key has occured value times".
 * 
 * @param key is an address of a string
 * @param value is an address of a string
 */
void MR_Emit(char *key, char *value) {
    for (int i=0; i<=pairsize; i++) {
        // when a specific key doesn't exist, create it in mapping data structure 
        if (i == pairsize) {
            // key
            mapping[i].key = malloc(sizeof(char)*(strlen(key)+1));
            strcpy(mapping[i].key, key);

            // value of values, (*newvalues).value is equal to newvalues->value 
            values *newvalues = malloc(sizeof(values));
            newvalues->value = malloc(sizeof(char)*(strlen(value)+1));
            strcpy((*newvalues).value, value);
            // next of values
            newvalues->next = NULL;
            // all for values
            mapping[i].value = newvalues;
            pairsize ++;
            break;
        }
        // when a specific key exist, updates its value 
        else if (strcmp(mapping[i].key, key) == 0) {

            values *temp = mapping[i].value;
            while (temp->next != NULL && strcmp(value, temp->next->value) <= 0) {
                temp = temp->next;
            }

            // value of values, (*newvalues).value is equal to newvalues->value 
            values *newvalues = malloc(sizeof(values));
            newvalues->value = malloc(sizeof(char)*(strlen(value)+1));
            strcpy((*newvalues).value, value);
            // next of values
            newvalues->next = temp->next;
            // all for values
            temp->next = newvalues;
            break;
        }
    }
}

/**
 * Comparator for data type pair
 */
int pair_comparator(const void *p, const void *q) { 
    return strcmp(((pair *) p)->key, ((pair *) q)->key);
} 





/**
 * The function takes a given key and map it to a number, from 0 to num_partitions - 1.
 * Then modulus the mapping number by num_partitions to figure out which partition is charged of it.
 * In other words, this function decides which partition gets a particular key-value pair to process.
 * 
 * One partition is one reducer thread?
 */
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0') 
        hash = hash * 33 + c;
    return hash % num_partitions;
}

// typedef char *(*Getter)(char *key, int partition_number);
char *get(char *key, int partition_number) {
    if (gettercv != NULL) {
        gettercv = gettercv->next;
        return gettercv == NULL? NULL:gettercv->value;
    } 

    // traverse all partitions, use 1 to replace partition_number temporarily
    // for each partition, traverse all pairs that we have within the partition
    for (int i=0; i<pairsize; i++) {
        // to check which pair is the pair that with pair.key = key
        if (strcmp(key, mapping[i].key) != 0) continue;
        // getter current value
        gettercv = mapping[i].value;
        return gettercv->value;
    }
    

    return NULL;
}
Getter mygetter = get;


//char *p = NULL; // create a pointer and assign it the address of buffer
// if (strcmp(mapping[i].key, key) == 0) {
                // printf("%s vs %s", mapping[i].key, key);
                // char buffer[8]; // create a place to store text
                // *p = buffer;

                // // if (p == NULL) printf("say ya\n");
            //     p = (char*) malloc(sizeof(int));
            //     // strcpy(buffer, "1");
            //     sprintf((void*) p, "%d", mapping[i].value); // create textual representation of 'i' 
            //     printf("wtf\n");
            //     return p;
            // }

/**
 * 
 */
void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition) {
    // pthread_t p[num_mappers];
    
    // for (int i=0; i<num_mappers; i++) {
    //     pthread_create(&p[i], NULL, mythread, &num_mappers);
    //     pthread_join(p[i], NULL);
    // }
        
    printf("argc: %d\n", argc);
    for (int i=0; i<argc; i++) printf("argv[%d]: %s\n", i, argv[i]);
    printf("num_mappers: %d\n", num_mappers);
    printf("num_reducers: %d\n", num_reducers);
    
    for (int i=1; i<argc; i++) map(argv[i]);

    // printf("here\n");
    qsort(&mapping, pairsize, sizeof(pair), pair_comparator);

    // Reduce(char *key, Getter get_next, int partition_number)
    for (int i=0; i<pairsize; i++) {
        gettercv = NULL;
        // printf("%s", mapping[i].key);
        reduce(mapping[i].key, mygetter, num_reducers-1);
    }
    // printf("%s \n", mapping[pairsize-1].key);
    // reduce(mapping[pairsize-1].key, mygetter, num_reducers);
    
    // printf("0th %s", mapping[0].key);
    // printf("%s", mapping[pairsize-1].key);
    // printf("19999th %s", mapping[19999].key);
    
    // for (int i=0; i<pairsize; i++) {
    //     if (strcmp(mapping[i].key, "") != 0) {
    //         printf("%s", mapping[i].key);
    //     }
    //     else {
    //         printf("break\n");
    //         break;
    //     };
    // }
}








// /**
//  * 
//  */
// void *mythread(void* arg) {
//     printf("num_mappers: %d\n", *((int*) arg));
//     return arg;
// }