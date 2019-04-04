#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>
#include "mapreduce.h"

pthread_mutex_t mlock;

// linkedlist of values, and each value has its own data 
// one key could be corresponding to many values in a pair
typedef struct node_t {
    char *data;
    struct node_t *nextv;
} value;

// global getter current value
// gettercvs is used to point to address of a "variable value" in the linkedlist
value **gettercvs;

// a pair starts with a key and its corresponding value
// the corresponding value is initialized as head of a linked list
// future values that corresponding to the key will be added to the linked list
typedef struct dict_t {
    char *key;
    value *values; // head of the linked list
    int sv;        // size of values, which is size of the linked list
    struct dict_t *nextp; // different keys may have the same hashcode, so use chainedbuckets to solve this collision
} pair;

/**
 * This function inserts new value into linked list of values for a key-value pair.
 */
void insertpairvalue(pair *curpair, char *data) {
    // create a new variable value for data, which is corresponding to curpair
    // note. (*newvalue).value is equal to newvalue->value 
    value *newvalue = malloc(sizeof(value));
    newvalue->data = malloc(sizeof(char)*(strlen(data)+1));
    strcpy((*newvalue).data, data);
    newvalue->nextv = curpair->values;
    // insert value newvalue into the head of linked list of curpair
    curpair->values = newvalue;
    curpair->sv++;
}

// hashmap for storing key-values pairs
// each index of a hashmap is a linked list that composed of pairs
typedef struct {
    pair **hasharray;   // hashArray with chainedbuckets, each chainedbuckets has an index in array
    int chainedbuckets; // number of chainedBuckets, which is size of the array, that can be used in this array
    int numcurpair;     // currently, number of paris (HashNodes) in this hash array 
    double loadFactor;  // threshold of expanding and rehashing
} pairhashmap;

// global data structure for storing pairs
pairhashmap *mapping;

/**
 * This function inserts a new pair into the end of a "pairs linked list".
 */
void insertpairpair(pairhashmap *map, pair *newpair, int index) {
    // printf("key: %s, value: %s", curpair->key, curpair->values->data);
    pair *head = map->hasharray[index];
    map->hasharray[index] = newpair;
    newpair->nextp = head;
}

/**
 * This function initialize a hashmap
 */
void initpairhashmap(pairhashmap *map, int initialsize) {
    map->hasharray = malloc(sizeof(pair *)*initialsize); // memory size of this hashmap
    for (int i=0; i<initialsize; i++) {
        map->hasharray[i] = NULL;
    }
    map->chainedbuckets = initialsize; // number of indices for this hashmap
    map->numcurpair = 0; // number of pairs that are in this hashmap
    map->loadFactor = 0.7; // increase size of this hashmap when numcurpair/chainedbuckets >= 0.7
}

/**
 * This function return the index of a key for a hashmap
 */
int getindexhashmap(char *key, int size) {
    unsigned long int hash = 0;
    unsigned int i = 0;
    // convert string to int
    while(i < strlen(key)){
        hash = hash << 8;
        hash += key[i];
        i++;
    }

    return hash % size;
}

/**
 * This function returns an existed pair that corresponds to a given key,
 * otherwise, return NULL.
 */
pair *findpairhashmap(pairhashmap *map, char *key, int index) {
    pair *temppair = map->hasharray[index];
    while (temppair != NULL) {
        if (strcmp(temppair->key, key) == 0)
            return temppair;
        temppair = temppair->nextp;
    }
    return NULL;
}

/**
 * This functino inserts a new pair into a hashmap.
 */
void insertpairhashmap(pairhashmap *map, pair *newpair, int index) {
    // insert new pair to the pair linkedlist
    // printf("index: %d\n", index);
    insertpairpair(map, newpair, index);
    map->numcurpair ++;
    // printf("map->hasharray[index]->key: %s", map->hasharray[index]->key);

    // increase hashmap when exceed loadfactor, new chainedBucket = 2*chainedBucket-1
    if (map->numcurpair / map->chainedbuckets >= map->loadFactor) {
        // create a new hasharray
        pair **newhasharray = malloc(sizeof(pair *)*(map->chainedbuckets*2-1));
        for (int i=0; i<map->chainedbuckets*2-1; i++) {
            newhasharray[i] = NULL;
        }
        // put each pair of mapping into newmapping
        map->numcurpair = 0;
        int oldchainedbuckets = map->chainedbuckets;
        map->chainedbuckets = map->chainedbuckets*2-1;
        pair **oldhasharray = map->hasharray; // *
        map->hasharray = newhasharray; // &
        for (int i=0; i<oldchainedbuckets; i++) {
            // get new index for each existed pair in mapping
            if (oldhasharray[i] != NULL) {
                pair *temppair = oldhasharray[i];
                while (temppair != NULL) {
                    index = getindexhashmap(temppair->key, map->chainedbuckets);
                    pair *nextpair = temppair->nextp;
                    temppair->nextp = NULL;
                    insertpairhashmap(map, temppair, index);
                    temppair = nextpair;
                }
                // printf("key: %s, value: %s", map->hasharray[i]->key, map->hasharray[i]->values->data);
            }
        } 
        free(oldhasharray);
    }
}

/**
 * MR_Emit recevies a key-value pair, and stores this pair in a data structure.
 * If a key is existed in the data structure, then MR_Emit updates the value.
 * 
 * The meaning of key-value pair could be any.
 * One of a possible meaning could be "a specific key has occured value times".
 */
void MR_Emit(char *key, char *value) {
    pthread_mutex_lock(&mlock);
    // this function returns the index of a key for a hashmap
    int index = getindexhashmap(key, mapping->chainedbuckets);
    // this function returns an existed pair that corresponds to a given key in a hashmap, otherwise, return NULL
    pair *existedpair = findpairhashmap(mapping, key, index);
    // existedpair doesn't exist, so create a new pair and insert it to mapping
    if (existedpair == NULL) {
        // printf("NULL key: %s, value: %s, index: %d, mapping->numcurpair: %d.\n", key, value, index, mapping->numcurpair);
        // create a new pair
        pair *newpair = malloc(sizeof(pair)); // initialize memory
        newpair->key = malloc(sizeof(char)*(strlen(key)+1));
        newpair->values = NULL;
        newpair->nextp = NULL;
        strcpy(newpair->key, key); // set key
        insertpairvalue(newpair, value); // set values
        newpair->sv = 1; // set size of values, which is a linked list 
        // printf("key: %s, value: %s, sv: %d\n", newpair->key, newpair->values->data, newpair->sv);
        // this functino inserts a new pair into a hashmap
        insertpairhashmap(mapping, newpair, index);
    }
    // existedpair exists, so update its values
    else {
        // printf("NON-NULL key: %s, value: %s, index: %d, mapping->numcurpair: %d.\n", key, value, index, mapping->numcurpair);
        // this function inserts new value into linked list of values for a key-value pair
        insertpairvalue(existedpair, value);
    }
    pthread_mutex_unlock(&mlock);
}

/**
 * typedef char *(*Getter)(char *key, int partition_number);
 */
char *get(char *key, int partition_number) {
    // printf("deal with key: %s", key);
    // gettercv is used to point to address of a "variable value" in the linkedlist
    // gettercv != NULL means that we are already pointing to a value in a linkedlist
    if (gettercvs[partition_number] != NULL) {
        gettercvs[partition_number] = gettercvs[partition_number]->nextv;
        return gettercvs[partition_number] == NULL? NULL:gettercvs[partition_number]->data;
    } 

    // get the index of matching pair in hashmap
    int index = getindexhashmap(key, mapping->chainedbuckets);
    // returns an existed pair that corresponds to a given key in a hashmap, otherwise, return NULL
    pair *existedpair = findpairhashmap(mapping, key, index);
    // there is no matching pair, return NULL
    if (existedpair == NULL) return NULL;
    else {
        gettercvs[partition_number] = existedpair->values;
        return gettercvs[partition_number]->data;
    }
}

// function variable
Getter mygetter = get;

/**
 * Comparator for data type pair
 */
int pair_comparator(const void *p, const void *q) { 
    return strcmp((*(pair **) p)->key, (*(pair **) q)->key);
} 

/**
 * Comparator for values in a pair
 */
int value_comparator(const void *p, const void *q) {
    return strcmp(*(char **) p, *(char **) q);
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

Mapper gmap;
Reducer greduce;

/**
 * Call map to update mapping
 */
void *mapthread(void *arg) {
    gmap((char *) arg);
    return NULL;
}

/**
 * Call reduce to get mapping
 */
void *redthread(void *arg) {
    int *indexpartition = ((void **) arg)[0];
    // printf("thread %d start\n", *indexpartition);
    pair **partitionself = ((void **) arg)[1];
    int *partitionsize = ((void **) arg)[2];
    // Reduce(char *key, Getter get_next, int partition_number)
    for (int i=0; i<*partitionsize; i++) {
        // printf("*indexpartition: %d, *partitionsize: %d\n", *indexpartition, *partitionsize );
        // printf("partitionself[%d]->key: %s", i, partitionself[i]->key);
        greduce(partitionself[i]->key, mygetter, *indexpartition);
        gettercvs[*indexpartition] = NULL;
    }
    // printf("thread %d end\n", *indexpartition);
    return NULL;
}

/**
 * 
 */
void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition) {
    // initialize map lock
    if (pthread_mutex_init(&mlock, NULL) != 0) {
        printf("\n mutex init failed\n");
        return;
    }
    // map threads
    pthread_t mp[num_mappers];

    // initialize a hashmap with size = 37
    mapping = malloc(sizeof(pairhashmap));
    initpairhashmap(mapping, 37);
    
    greduce = reduce;
    gmap = map;
    
    for (int i=1; i<argc; i+=num_mappers) {
        // create map threads
        for (int j=0; j<num_mappers && i+j<argc; j++) {
            pthread_create(&mp[j], NULL, mapthread, argv[i+j]);
        }
        // pthread_join would wait p to be finished
        for (int j=0; j<num_mappers && i+j<argc; j++) {
            pthread_join(mp[j], NULL);
        }
    }
    // printf("map done\n");

    // sort values for each pair in the hashmap
    for (int i=0; i<mapping->chainedbuckets; i++) {
        // check whether this chainedbucket has a pairs linkedlist or not
        if (mapping->hasharray[i] == NULL) continue;
        // traverse all pairs in this pairs linkedlist
        pair *temppair = mapping->hasharray[i];
        // printf("i: %d, temppair->key: %s, temppair->values->data: %s\n", i, temppair->key, temppair->values->data);
        while (temppair != NULL) {
            // create array for sorting
            char **temparray = malloc(sizeof(char *)*temppair->sv);
            // update array
            value *tempvalue = temppair->values;
            int index = 0;
            while (tempvalue != NULL) {
                // printf("tempvalue->data: %s", tempvalue->data);
                temparray[index] = tempvalue->data;
                tempvalue = tempvalue->nextv;
                index ++;
            }
            // sorting array
            qsort(temparray, index, sizeof(char *), value_comparator);
            // update values linkedlist of current pair
            tempvalue = temppair->values;
            for (int j=0; j<index; j++) {
                tempvalue->data = temparray[j];
                tempvalue = tempvalue->nextv;
            }

            free(temparray);
            temppair = temppair->nextp;
        }
    }
    // printf("values sort done\n");

    // sort pairs by key
    // create an array for storing pairs in ascending order, and array index
    pair **pairorder = malloc(sizeof(pair *)*mapping->numcurpair);
    int ipairorder = 0;
    for (int i=0; i<mapping->chainedbuckets; i++) {
        // check whether this chainedbucket has a pairs linkedlist or not
        if (mapping->hasharray[i] == NULL) continue;
        // traverse all pairs in this pairs linkedlist
        pair *temppair = mapping->hasharray[i];
        while (temppair != NULL) {
            // update keyorder array
            pairorder[ipairorder] = temppair;
            temppair = temppair->nextp;
            ipairorder ++;
        }
        // printf("chainedbucket: %d\n", i);
    }
    qsort(pairorder, ipairorder, sizeof(pair *), pair_comparator);
    // printf("keys sort done, ipairorder: %d\n", ipairorder);

    // size of pairs for a "reduce thread", which means the number of pairs for a "partition"
    int stp[num_reducers];
    for (int i=0; i<num_reducers; i++) stp[i] = 0; // initialize stp[i] to 0

    // get the acutal size for each partition
    for (int i=0; i<ipairorder; i++) {
        unsigned long index = partition(pairorder[i]->key, num_reducers);
        stp[index] ++;
    }
    // printf("get partition size done\n");

    // creat an pairs array for each partition, which can be considered as reduce thread or reducer
    // pt = pairs of a thread, this means that we store some pairs in a thread, and each thread has its own pairs set
    pair **pt[num_reducers];
    for (int i=0; i<num_reducers; i++) {
        pt[i] = malloc(sizeof(pair *)*stp[i]);
    } 
    
    // update array of each partition, an array stores the pairs that need to be processed by the corresponding partition
    for (int i=0; i<num_reducers; i++) stp[i] = 0; // re-initialize stp[i] to 0
    for (int i=0; i<ipairorder; i++) {
        unsigned long index = partition(pairorder[i]->key, num_reducers);
        pt[index][stp[index]] = pairorder[i];
        stp[index] ++;
    }
    // printf("update partition done\n");

    // lock threads
    pthread_t lp[num_reducers];
    int subindex[num_reducers];
    gettercvs = malloc(sizeof(value *)*num_reducers);
    void **subarg[num_reducers];

    // create reduce threads
    for (int i=0; i<num_reducers; i++) {
        subarg[i] = malloc(sizeof(void *)*3);
        subindex[i] = i;
        subarg[i][0] = &subindex[i];
        subarg[i][1] = pt[i];
        subarg[i][2] = &stp[i];
        gettercvs[i] = NULL;
        pthread_create(&lp[i], NULL, redthread, subarg[i]);
    }

    // pthread_join would wait p to be finished
    for (int i=0; i<num_reducers; i++) {
        pthread_join(lp[i], NULL);
        free(subarg[i]);
    }

    // printf("reduce done\n");
    
    // free pairs and their values
    for (int i=0; i<ipairorder; i++) {
        // free value linkedlist for each pair
        value *tempvalue = pairorder[i]->values;
        while (tempvalue != NULL) {
            value *forfree = tempvalue->nextv;
            free(tempvalue->data); // newvalue->data = malloc(sizeof(char)*(strlen(data)+1));
            free(tempvalue); // value *newvalue = malloc(sizeof(value));
            tempvalue = forfree;
        }
        // free pair
        free(pairorder[i]->key); // newpair->key = malloc(sizeof(char)*(strlen(key)+1));
        free(pairorder[i]); // // pair *newpair = malloc(sizeof(pair)); // initialize memory
    }
    free(pairorder); // pair **pairorder = malloc(sizeof(pair *)*mapping->numcurpair);

    // free partitions
    for (int i=0; i<num_reducers; i++) {
        free(pt[i]); // pt[i] = malloc(sizeof(pair *)*stp[i]);
    } 

    // free hash array
    free(mapping->hasharray); // map->hasharray = malloc(sizeof(pair *)*initialsize); // memory size of this hashmap

    // free mapping
    free(mapping); // mapping = malloc(sizeof(pairhashmap));

    // free gettercvs
    free(gettercvs); // gettercvs = malloc(sizeof(value *)*num_reducers);
    
    // end of MR_Run
}
