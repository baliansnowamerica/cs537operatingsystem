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
    int sv; // size of values
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
            // size of values in this pair
            mapping[i].sv ++;
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
            // size of values in this pair
            mapping[i].sv ++;

            // value of values, (*newvalues).value is equal to newvalues->value 
            values *newvalues = malloc(sizeof(values));
            newvalues->value = malloc(sizeof(char)*(strlen(value)+1));
            strcpy((*newvalues).value, value);
            // next of values
            newvalues->next = mapping[i].value;
            // all for values
            mapping[i].value = newvalues;

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
 * 
 */
int value_comparator(const void *p, const void *q) {
    return strcmp((char *) p, (char *) q);
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
        
    // printf("argc: %d\n", argc);
    // for (int i=0; i<argc; i++) printf("argv[%d]: %s\n", i, argv[i]);
    // printf("num_mappers: %d\n", num_mappers);
    // printf("num_reducers: %d\n", num_reducers);
    
    for (int i=1; i<argc; i++) map(argv[i]);

    printf("before sort values\n");

    // sort values of each key
    for (int i=0; i<pairsize; i++) {
        // create array for sorting
        printf("mapping[%d].sv: %d\n", i, mapping[i].sv);
        char *temparray[mapping[i].sv];
        printf("gg\n");
        int index = 0;
        printf("fuck off\n");
        values *temp = mapping[i].value;
        printf("lala i: %d\n", i);
        while (temp != NULL) {
            temparray[index] = temp->value;
            temp = temp->next;
            index ++;
        }
        printf("tata\n");
        // sorting array
        qsort(&temparray, index, sizeof(char*), value_comparator); //
        printf("haha index: %d, sv: %d\n", index, mapping[i].sv);
        // update values linkedlist
        temp = mapping[i].value;
        
        for (int j=0; j<index; j++) {
            if (temp == NULL)
                printf("temparray[%d]: %s \n", j, temparray[j]);
            temp->value = temparray[j];
            temp = temp->next;
        }
        printf("i %d done \n", i);
    }

    printf("before qsort\n");

    // sort key
    qsort(&mapping, pairsize, sizeof(pair), pair_comparator);


    printf("pairsize: %d\n", pairsize);

    // values *check = mapping[400].value;
    // while (check->next != NULL) {
    //     printf("%s\n", check->value);
    //     check = check->next;
    // }
    
    printf("after qsort\n");

    // creat an array for each partition, which is thread for reducer
    // tp = threads pairs, which means that we store some pairs in a thread, and each thread has its own pair set
    pair *tp[num_reducers][20000];
    
    // size of pairs of a thread, which means the number of pairs for each thread 
    int stp[num_reducers];
    for (int i=0; i<num_reducers; i++) stp[i] = 0; // initialize stp[i] to 0
    
    // update array of each partition, an array stores the pairs that need to be processed by the corresponding partition
    for (int i=0; i<pairsize; i++) {
        unsigned long index = partition(mapping[i].key, num_reducers);
        tp[index][stp[index]] = &mapping[i];
        stp[index] ++;
    }

    // Reduce(char *key, Getter get_next, int partition_number)
    for (int i=0; i<num_reducers; i++) {
        for (int j=0; j<stp[i]; j++) {
            gettercv = NULL;
            reduce(tp[i][j]->key, mygetter, i);
        }
    }
    
    // end of MR_Run
}








// /**
//  * 
//  */
// void *mythread(void* arg) {
//     printf("num_mappers: %d\n", *((int*) arg));
//     return arg;
// }