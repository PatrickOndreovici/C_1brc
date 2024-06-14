//
// Created by Patrick Ondreovici on 08.06.2024.
//
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <pthread.h>
#include <string.h>
#define FILEPATH "measurements.txt"
#define NTHREADS 16

int chunk_selector;
long long file_size;
pthread_mutex_t interval_calc_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct TrieNode TrieNode;
struct TrieNode{
    struct TrieNode* neighbours[256];
    int minim;
    int maxim;
    int sum;
    int count;
    int leaf;
};



void get_trie_sol(TrieNode* node, char* name, int cnt, int *first_output){
    if (node->leaf){
        name[cnt] = '\0';
        if (*first_output == 1){
            printf("{");
            *first_output = 0;
        }
        else{
            printf(", ");
        }
        printf("%s;%.1f,%.1f,%1.f", name, node->minim / 10.0, (node->sum / node->count) / 10.0, node->maxim / 10.0);
    }
    for (int i = 0; i < 256; ++i){
        if (node->neighbours[i] != NULL){
            name[cnt] = (char)i;
            get_trie_sol(node->neighbours[i], name, cnt + 1, first_output);
        }
    }
}


TrieNode* insert_trie(TrieNode* node, char ch){
    struct TrieNode* next_node = NULL;

    if (node->neighbours[(unsigned char)ch] == NULL){
        node->neighbours[(unsigned char)ch] = malloc(sizeof(TrieNode));
        node->leaf = 0;
    }
    next_node = node->neighbours[(unsigned char)ch];
    return next_node;
}

void* do_work(void* data){
    char *file_in_memory = (char*)data;
    int chunk_size = 6710886; // 64 MB
    TrieNode* root;
    root = malloc(sizeof (TrieNode));

    while (1){
        pthread_mutex_lock(&interval_calc_mutex);
        long long left = 1LL * chunk_size * (chunk_selector);
        long long right = left + chunk_size;
        chunk_selector++;
        pthread_mutex_unlock(&interval_calc_mutex);
        if (left >= file_size){
            break;
        }
        if (right >= file_size){
            right = file_size - 1;
        }
        const char *start = left > 0
                        ? (char *)memchr(&file_in_memory[left], '\n', chunk_size) + 1
                        : &file_in_memory[left];

        const char *end = (char *)memchr(&file_in_memory[right], '\n', chunk_size) + 1;

        int temperature_as_int;
        int negative;

        while (start != end){
            TrieNode* node = root;
            while (*start != ';'){
                char ch = *start;
                node = insert_trie(node, ch);
                ++start;
            }
            ++start;
            negative = 0;
            if (*start == '-'){
                negative = 1;
                ++start;
            }
            if (*(start + 1) == '.'){
                temperature_as_int = (*start - '0') * 10 + *(start + 2) - '0';
                start = start + 4;
            }
            else{
                temperature_as_int = (*start - '0') * 100 + (*(start + 1) - '0') * 10 + *(start + 3) - '0';
                start = start + 5;
            }
            if (negative == 1){
                temperature_as_int = -temperature_as_int;
            }
            if (node->leaf){
                if (temperature_as_int < node->minim){
                    node->minim = temperature_as_int;
                }
                if (temperature_as_int > node->maxim){
                    node->maxim = temperature_as_int;
                }
                node->sum += temperature_as_int;
                node->count++;
            }
            else{
                node->minim = node->maxim = node->sum = temperature_as_int;
                node->count = node->leaf = 1;
            }
        }
    }
    return root;
}

void merge_trie(TrieNode* node1, TrieNode* node2){
    if (node2->leaf){
        if (node1->leaf){
            if (node2->minim < node1->minim){
                node1->minim = node2->minim;
            }
            if (node2->maxim > node1->maxim){
                node1->maxim = node2->maxim;
            }
            node1->count += node2->count;
            node1->sum += node2->sum;
        }
        else{
            node1->leaf = 1;
            node1->minim = node2->minim;
            node1->maxim = node2->maxim;
            node1->count = node2->count;
            node1->sum = node2->sum;
        }
    }
    for (int i = 0; i < 256; ++i){
        if (node2->neighbours[i] != NULL){
            if (node1->neighbours[i] == NULL){
                node1->neighbours[i] = malloc(sizeof(TrieNode));
            }
            merge_trie(node1->neighbours[i], node2->neighbours[i]);
        }
    }
}

int main(){
    int fd = open(FILEPATH, O_RDONLY, (mode_t)0600);
    if (fd == -1){
        perror("Error when opening file");
        exit(EXIT_FAILURE);
    }
    struct stat sb;
    if (fstat(fd, &sb) == -1){
        exit(EXIT_FAILURE);
    }
    char *file_in_memory = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    file_size = sb.st_size;

    pthread_t threads[NTHREADS];
    for (int i = 0; i < NTHREADS; ++i){
        pthread_create(threads + i, NULL, do_work, file_in_memory);
    }
    TrieNode* tries[NTHREADS];
    for (int i = 0; i < NTHREADS; ++i){
        pthread_join(threads[i], (void **) &tries[i]);
    }
    for (int i = 1; i < NTHREADS; ++i){
        merge_trie(tries[0], tries[i]);
    }
    char* name = malloc(300);
    int* first_output = malloc(sizeof (int));
    *first_output = 1;
    get_trie_sol(tries[0], name, 0, first_output);
    return 0;
}
