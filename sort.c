#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>
#include <stdint.h>

//pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

// Structure to hold the key and the value of the 100 byte lines we are given
struct key_data {
  int key;
  char* value;
}; 
// Structure to hold the paramaters for the threads which we use for mergesorting
struct thread_args{
    struct key_data* arr;
    unsigned int start;
    unsigned int end;
};
// Method used for timing our sort, taken from discussion examples 
uint64_t get_current_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    uint64_t result = tv.tv_sec*(uint64_t)1000000 + tv.tv_usec;
    return result;
}

// Mergesort algorithm based on https://www.geeksforgeeks.org/merge-sort/
// Chaged to compared the key value of the key_data array instead of ints
void merge(struct key_data arr[], unsigned int l, unsigned int m, unsigned int r) {
    //printf("Merge 1\n");
    // Iterators for our sub arrays
    unsigned int i;
    unsigned int j;
    unsigned int k;
    // Get the sizes we need for our temp arrays
    unsigned int firstSubArraySize = m - l + 1;
    unsigned int secondSubArraySize = r - m;
 
    // Need to allocate these on the heap, to prevent stack overflow
    // Cannot initialize a variable sized object, so need to figure that out
    struct key_data *L = malloc(sizeof(struct key_data) * firstSubArraySize); 
    //if (L == NULL) {
    //    printf("Not enough memory for L\n");
    //}
    struct key_data *R = malloc(sizeof(struct key_data) * secondSubArraySize);
    //if (R == NULL) {
    //    printf("Not enough memory for R\n");
    //}

    // Copy data to temp arrays L[] and R[]
    for (i = 0; i < firstSubArraySize; i++) {
        // Lock before we do anything to our main array
        //pthread_mutex_lock(&lock);
        L[i] = arr[l + i];
        // Unlock aftr we make our changes
        //pthread_mutex_unlock(&lock);
    }
    for (j = 0; j < secondSubArraySize; j++) {
        // Lock before we do anything to our main array
        //pthread_mutex_lock(&lock);
        R[j] = arr[m + 1 + j];
        // Unlock aftr we make our changes
        //pthread_mutex_unlock(&lock);
    }
 
    // Merge the temp arrays back into arr[l..r]
    i = 0; // Initial index of first subarray
    j = 0; // Initial index of second subarray
    k = l; // Initial index of merged subarray
    while (i < firstSubArraySize && j < secondSubArraySize) {
        if (L[i].key <= R[j].key) {
            // Lock before we do anything to our main array
            //pthread_mutex_lock(&lock);
            arr[k] = L[i];
            // Unlock aftr we make our changes
            //pthread_mutex_unlock(&lock);
            i++;
        }
        else {
            // Lock before we do anything to our main array
            //pthread_mutex_lock(&lock);
            arr[k] = R[j];
            // Unlock aftr we make our changes
            //pthread_mutex_unlock(&lock);
            j++;
        }
        k++;
    }
 
    // Copy the remaining elements of L[], if there are any
    while (i < firstSubArraySize) {
        // Lock before we do anything to our main array
        //pthread_mutex_lock(&lock);
        arr[k] = L[i];
        // Unlock aftr we make our changes
        //pthread_mutex_unlock(&lock);
        i++;
        k++;
    }
 
    // Copy the remaining elements of R[], if there are any
    while (j < secondSubArraySize) {
        // Lock before we do anything to our main array
        //pthread_mutex_lock(&lock);
        arr[k] = R[j];
        // Unlock aftr we make our changes
        //pthread_mutex_unlock(&lock);
        j++;
        k++;
    }

    free(L);
    free(R);
}
 
// left is for left index and right is right index of the sub-array of arr to be sorted 
void mergeSort(struct key_data arr[], unsigned int left , unsigned int right) {
    //printf("Left: %d\t Right: %d\n", left, right);
    
    if (left < right) {
        unsigned int mid = left + (right - left) / 2;
 
        // Sort the first half of the keys

        mergeSort(arr, left, mid);
        // Sort the second half of the keys
        mergeSort(arr, mid + 1, right);

        // Merge the results of the first and second halves of the keys
        merge(arr, left, mid, right);
    }
    
}

void* _mergeSort(void* args) {
    struct thread_args* range = (struct thread_args*)args;
    mergeSort(range->arr, range->start, range->end);
    return NULL;
}

int main(int argc, char* argv[] ) {
    //printf("Here\n");
    
    // Throw an error if we don't have enough args
    if (argc != 4) {
       printf("Do not have 4 args, we have %d\n", argc);
    }

    //printf("Got here for file: %s\t Threads:%d\n", argv[1], atoi(argv[3]));

    // Get the number of thread we want to use
    char* numThreadsArg = argv[3];
    int numThreads = atoi(numThreadsArg);
    int orginalNumThreads = numThreads;

    // printf("File %s\t numThreads: %d\n", argv[1], numThreads);

    // Open the file
    FILE* fp = fopen(argv[1], "r"); 
    // If the file is nul then print an error
    if (fp == NULL) {
      printf("Input file isn't opening");
    }

    //printf("Got here for file: %s\t Threads:%d\n", argv[1], numThreads);

    // Get the size of the file in bytes
    
    //fseek(fp, 0, SEEK_END);
    //unsigned int size = ftell(fp);
    //rewind(fp);


    struct stat st;
    stat(argv[1], &st);
    unsigned int size = st.st_size;

    //unsigned int size = 0;    
    //while(getc(fp) != EOF)
    //    ++size;



    //printf("Size: %d\n", size);
    //printf("Size: %d \t\n", size);
    
    // File descriptor of input file for mapping 
    int fd = fileno(fp);
    // Use to map the data
    char* map = mmap(NULL, (unsigned int)size, PROT_READ, MAP_SHARED, fd, 0);

    // printf("Got here2 for file: %s\t Threads:%d\n", argv[1], numThreads);

    if (map == MAP_FAILED) {
        printf("Failed here/n");
    }
    // Get the amount of string in the array based on the file size
    unsigned int arraySize = size / 100;
    // Make an array of our key-value struct
    struct key_data *dataMap = malloc(sizeof(struct key_data) * arraySize);
    // Alocate space for the char* we are going to be using to store the current string

    // loop through the mapped data to add it all to our array
    for (unsigned int i = 0; i < size; i+= 100) {
        // Get the index of the current 100 bytes
        unsigned int idx = i/100;
        // Set the value to the current 100 bytes
        dataMap[idx].value = map+i;
        // Set the key to the first 4 bytes
        dataMap[idx].key = *(int*)dataMap[idx].value;
        // Print statement for checking the index, key, and value of the current string
        //printf("i= %d\tCurrent String Key: %d\t CurrentValue %s\n", i,currentString->key, currentString->value);
    }
    // Close the file
    fclose(fp);
    munmap(dataMap, size);
    //printf("Here1.\n");

    // Print the info currently in the data map
    //for(int i = 0; i < arraySize; ++i) {
    //    printf("Index: %d\t Key: %d\t Value: %s\n", i, dataMap[i].key, dataMap[i].value);
    //}

    // If we have more threads than lines, then make it so we match the thread count with the line count
    if (arraySize <= numThreads) {
        numThreads = arraySize;
    }

    //printf("NumThreads: %d\n", numThreads);
    //printf("ArraySize : %d\n", arraySize);
    // The array of threads we are using
    //pthread_t child_threads[numThreads];
    pthread_t *child_threads = malloc(sizeof(pthread_t) * numThreads);
    // struct key_data *L = malloc(sizeof(struct key_data) * firstSubArraySize);

    // Struct of the work ranges for the threads
    //struct thread_args work_ranges[numThreads];
    struct thread_args *work_ranges = malloc(sizeof(struct thread_args) * numThreads); 
    // Keep a variable of the current start of the thread
    unsigned int current_start = 0;
    // Get the range that each thread is taking
    unsigned int range = arraySize / numThreads;


    //printf("Range: %d\n", range);
    //printf("Current Start: %d\n", current_start);
    // Loop through and assign a start and an end for each thread as well as the array 
    for (unsigned int i = 0; i < numThreads; i++) {
        work_ranges[i].arr = dataMap;
        work_ranges[i].start = current_start;
        work_ranges[i].end = current_start + range - 1;
        // Update the current start so the next thread starts in the correct location
        current_start += range;
    }
    // Assign the last thread to the end of the array so all data is covered if there is any left over 
    work_ranges[numThreads-1].end = arraySize - 1;
    //printf("Here2\n");
    
    // Print the current values we have in our dataMap
    //for (int i = 0; i < numThreads; ++i) {
    //    printf("Thread %d: \tStart: %d\t End: %d\n", i, work_ranges[i].start, work_ranges[i].end);
    //}

    // Timing for the performance check
    uint64_t start = get_current_time();

    // Create the threads with the data we just got for them
    for (unsigned int i = 0; i < numThreads; ++i) {
        //printf("Thread %d: \tStart: %d\t End: %d\n", i, work_ranges[i].start, work_ranges[i].end);
        pthread_create((&child_threads[i]), NULL, (void*)_mergeSort, &work_ranges[i]);
        //pthread_create((&child_threads[i]), NULL, mergeSort, &work_ranges[i]);
    }
    // Join the threads back together once they are done
    for (unsigned int i = 0; i < numThreads; ++i) {
        pthread_join(child_threads[i], NULL);
    }

    // Call to test that single threading works, otherwise comment this line out
    //mergeSort(dataMap,0,arraySize-1);
    //printf("Here 2.5\n");
    
    // Merge the sections that each thread sorted
    // Get the head which we will use as the first half of the merge
    struct thread_args *head = &work_ranges[0];
    for (unsigned int i = 1; i < numThreads; i++) {
        // Get the current threads ranges which will be the second half
        struct thread_args *current = &work_ranges[i]; 
        // Merge the two halves together
        merge(dataMap, head->start, current->start - 1, current->end);

    }

    //printf("Here 3\n");
    // Time for the performance check
    uint64_t end = get_current_time();
    // Print the time it took to sort our code
    uint64_t finalTime = end - start;
    //double finalSeconds = (end - start) / 1000000;
    // Print the time it took to run the threads
    printf("Threads: %d\t Total sort time: %lu microseconds\n", orginalNumThreads, finalTime);

    free(child_threads);
    free(work_ranges);
    // Open the output file so that we can mmap to the file
    //printf("Here 4\n");

    int fdOutput = open(argv[2], O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IROTH | S_IWUSR);
    // Get the file descriptor to the output file
    //printf("Here 5\n");
    // Map the data to the output file
    //printf("Here 6\n");
    // Get a map of the data
    char* outputMap = mmap(NULL, (unsigned int)size, PROT_READ | PROT_WRITE, MAP_SHARED, fdOutput, 0);
    int write = pwrite(fdOutput, "", 1, size-1);

    if (write == -1) {
        printf("Error in the output file\n");
    }
    //printf("Here 6.5\n");
    for (unsigned int i = 0; i < size; i+= 100) {
        unsigned int idx = i/100;
        memcpy(outputMap+i,dataMap[idx].value,100);
    }

    if (msync(outputMap, size, MS_SYNC) < 0){
        printf("msync failed");
    }
    //munmap(dataMap, size);
    //printf("Here 7\n");
    // Copy the dataMap to the output map
    //printf("Here 8\n");
    // Close the file
    close(fdOutput);
    free(dataMap);
    //printf("Here9\n");
    return 0;
} 
