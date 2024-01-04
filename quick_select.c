#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <mpi.h>
#include <time.h>
#include <curl/curl.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>

// Struct for byte streaming.
typedef struct {
  char* data;
  size_t current_byte_size;
  size_t total_byte_size;
} STREAM;

// Parsing result.
typedef struct {
  uint32_t* data;
  size_t size;
} ARRAY;

int partitioninplace(int A[], int n, int v, int start, int end);
size_t write_callback(void* data, size_t size, size_t nmemb, void* destination);
ARRAY getWikiPartition(const char *url, int world_rank, int world_size);

int main(int argc, char *argv[]) {
	if (argc != 2) {
		if (!my_rank) printf("False arguments\n");
		exit(1);
	}
    int k = atoi(argv[1]);
    
    int num_procs, my_rank, pivot, pivot_sender, direction, breakpoint, local_size;
	int termination_signal = 0;
	int* A_local;

	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
	
	// ALLOCATE MEMORY FROM WEB
	const char *url="https://dumps.wikimedia.org/other/static_html_dumps/current/en/skins.lst";
	ARRAY result = getWikiPartition(url, my_rank, num_procs);
	local_size = (int)result.size;
	A_local = (int*)result.data;	
	printf("Process %d local size: %d\n", my_rank, local_size);
	// it is assumed that n >> num_procs
	int activeProcs = num_procs;
	
	MPI_Barrier(MPI_COMM_WORLD);
	
	// master process initialization
	if (my_rank == 0) {
		printf("\nData received, starting algorithm\n\n");
		pivot = A_local[0];		
	}
	direction = 1;
	MPI_Bcast(&pivot, 1, MPI_INT, 0, MPI_COMM_WORLD);
	
	//pointer to the available parts of the local arrays
	int pointers[2] = {0, local_size - 1};
	
	//-----------------------------------------------------------------
	int counter = 0;
	while(counter < local_size) {
		counter++;		
		MPI_Barrier(MPI_COMM_WORLD);	

		// EACH PROCESS FINDS A BREAKPOINT
		if (my_rank < activeProcs) {			
			// zero elements left
			if (pointers[0] > pointers[1]) {}
			else if (pointers[0] >= local_size) breakpoint = local_size - 1;
			else if (pointers[1] < 0) breakpoint = 0; 
			// one element left
			else if (pointers[1] == pointers[0]) {
				if (A_local[pointers[0]] >= pivot) breakpoint = pointers[0];
				else breakpoint = pointers[0] + 1;
			}
			// more than one element is left
			else {
				breakpoint = partitioninplace(A_local, local_size, pivot, pointers[0], pointers[1]);				
			}
			
			//send poitners
			MPI_Send(&breakpoint, 1, MPI_INT, 0, my_rank, MPI_COMM_WORLD);
		}
		
		// MASTER PROCESS DECIDES NEXT STEP
		if (my_rank == 0) {			
			//receive breakpoints and sum them
			int recv_data;
			int elementsSmallerThanPivot = 0;
			for(int i = 0; i < activeProcs; i++) {
				MPI_Recv(&recv_data, 1, MPI_INT, i, i, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				elementsSmallerThanPivot += recv_data;
			}
			
			// next step decision
			if (elementsSmallerThanPivot + 1 == k) termination_signal = 1;
			else if (elementsSmallerThanPivot + 1 > k) direction = 0;
			else direction = 1;
				
		}		
		// Exit the loop if termination signal is broadcasted
		MPI_Barrier(MPI_COMM_WORLD);
		MPI_Bcast(&termination_signal, 1, MPI_INT, 0, MPI_COMM_WORLD);
		if (termination_signal) break;
		
		// Broadcast direction
		MPI_Bcast(&direction, 1, MPI_INT, 0, MPI_COMM_WORLD);				
		
		// DIRECTION HANDLING
		
		// change pointers accordingly
		if (my_rank < activeProcs) {
			if (direction) pointers[0] = breakpoint;
			else pointers[1] = breakpoint - 1;
		}

		// exclude the pivot in the next step
		if (my_rank == pivot_sender) {
				// find the pivot index
				int pivotIndex = local_size;
				for(int i = 0; i < local_size; i++) {
					if (A_local[i] == pivot) {
						pivotIndex = i;
						break;
					}
				}		
				
				/* Remove pivot if it's found within the usable spectrum, ie when the direction is right swap it with the left end and
				increase the pointers by one. This works vice versa when the direction is left and ensures that each steps limits
				at least one element of the original data, even if pivot appears many times */
				if (pivotIndex <= pointers[1]) {
					A_local[pivotIndex] = A_local[pointers[!direction]];
					A_local[pointers[!direction]] = pivot;
				
					if (direction) pointers[0]++;
					else pointers[1]--;
				}
				
				// specific occasion that pivot is the only elemnt left
				if (pointers[0] > pointers[1] && direction) breakpoint++;
		}		
				
		// NEW PIVOT ASSSIGNMENT
		
		// assign new pivot sender
		MPI_Barrier(MPI_COMM_WORLD);
		if (my_rank < activeProcs)
			MPI_Send(pointers, 2, MPI_INT, 0, my_rank, MPI_COMM_WORLD);

		if (my_rank == 0) {
			int recv_data[2];
			for(int i = 0; i < activeProcs; i++) {
				MPI_Recv(&recv_data, 2, MPI_INT, i, i, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				if (recv_data[0] <= recv_data[1])
					pivot_sender = i;
			}	
		}
			
		MPI_Bcast(&pivot_sender, 1, MPI_INT, 0, MPI_COMM_WORLD);
		MPI_Barrier(MPI_COMM_WORLD);
		
		// pivot sender assigns new pivot value
		if (my_rank == pivot_sender) {
			pivot = A_local[pointers[0]];
		}
		MPI_Bcast(&pivot, 1, MPI_INT, pivot_sender, MPI_COMM_WORLD);
	}
	//--------------------------------------------------------------------------
	// RESULT DISPLAY AND MEMORY DE-ALLOCATION
	if (my_rank == 0) {
		if (k % 10 == 1) printf("%dst ", k);
		else if (k % 10 == 2) printf("%dnd ", k);
		else if (k % 10 == 3) printf("%drd ", k);
		else printf("%dth ", k);
		printf("smallest element in array is %d\n", pivot);
		printf("Algorithm used %d steps to find it\n", counter);
	}
	free(A_local);
	MPI_Finalize(); 
   
    return 0;
}

// function that partitions an input array of size n according to a pivot value v
int partitioninplace(int A[], int n, int v, int start, int end) {	
    if(end > n - 1) {
    	printf("End = %d out of bounds.\n", end);
    	return 0;
    }
    else if(start < 0) {
    	printf("Start = %d out of bouns\n", start);
    	return 0;
    }
    int temp = 0;
    
    // pointers at each end of the array (as specified by input)
    int left = start;
    int right = end;
	
	// repeats until left reaches right or all elements are checked
    while (left <= right && left < end) {    	 
        if (A[left] >= v && A[right] < v) {
        	// if elements at both pointers are on the wrong "side" of the array -> swap them
            temp = A[left];
            A[left] = A[right];
            A[right] = temp;
        }
        else  {
        // increase or decrease pointers as needed
            if (A[left] < v)
                left++;
            if (A[right] >= v)
                right--;
        }
    }
    
    if (A[left] < v) left++;
    return left;
}

// Write callback function for curl.
size_t write_callback(void* data, size_t size, size_t nmemb, void* destination){
  STREAM* stream=(STREAM*)destination;
  size_t realsize=size*nmemb;
  // If first time calling, allocate the needed memory.
  if(stream->data==NULL){
    stream->data=(char*)malloc(stream->total_byte_size*sizeof(char));
  }
  memcpy(&(stream->data[stream->current_byte_size]),data,realsize);
  stream->current_byte_size+=realsize;

  return realsize;
}

ARRAY getWikiPartition(const char *url,int world_rank,int world_size){
  // Input checking
  if(world_rank<0||world_rank>=world_size){
    printf("World rank out of bounds.\n");
    exit(1);
  }
  CURL* curl_handle;
  CURLcode res; // Error checking
  double content_length;
  ARRAY result;
  
  curl_global_init(CURL_GLOBAL_ALL);
  curl_handle=curl_easy_init();
  if(curl_handle){
    // Set up url 
    curl_easy_setopt(curl_handle,CURLOPT_URL,url);
    // Get file total size. (No body yet)
    curl_easy_setopt(curl_handle,CURLOPT_NOBODY,1L);
    //printf("Request1\n");
    res=curl_easy_perform(curl_handle);
    //printf("Request1 done\n");
    if(res!=CURLE_OK){
      printf("Error in curl_easy_perform()\n");
      exit(1);
    }
    else{
      res = curl_easy_getinfo(curl_handle,CURLINFO_CONTENT_LENGTH_DOWNLOAD,&content_length);
      if(res!=CURLE_OK){
        printf("Error in curl_easy_getinfo().\n");
        exit(1);
      }
    }
    curl_easy_reset(curl_handle);
    // Got size, convert to size_t.
    size_t file_byte_size=(size_t)content_length;
    // Round out to multiples of 4 since element size is 32 bits (ignore remainder)
    file_byte_size=(file_byte_size/4)*4;
    // Partition is done using 32 bit ints as elements 
    size_t file_int_size=file_byte_size/4;
    size_t start_byte=(file_int_size/world_size)*world_rank*4;
    size_t end_byte=(file_int_size/world_size)*(world_rank+1)*4-1;
    // Last guy takes the remaining elements
    if(world_rank==world_size-1){
      end_byte=file_byte_size-1;
    }
    
    // Init downstream
    STREAM stream;
    stream.data=NULL;
    stream.current_byte_size=0;
    stream.total_byte_size=end_byte-start_byte+1;
    char* range=(char*)malloc(100*sizeof(char));
    sprintf(range,"%zu-%zu",start_byte,end_byte);
    
    // Request body and set rest of options
    curl_easy_setopt(curl_handle,CURLOPT_URL,url);
    curl_easy_setopt(curl_handle,CURLOPT_RANGE,range);
    curl_easy_setopt(curl_handle,CURLOPT_WRITEFUNCTION,write_callback);
    curl_easy_setopt(curl_handle,CURLOPT_WRITEDATA,(void*)&stream);
    
    // Get data..
    res=curl_easy_perform(curl_handle);
    if(res!=CURLE_OK){
      printf("Problem in data reception\n");
      exit(1);
    }
    
    // Check if you got everything you asked for.
    if(stream.current_byte_size<stream.total_byte_size){
      printf("MISSING %zu BYTES\n",stream.total_byte_size-stream.current_byte_size);
    }
    
    // Clean up and pass everything to the array struct. 
    curl_easy_cleanup(curl_handle);
    curl_global_cleanup();
    free(range);
    result.data=(uint32_t*)stream.data;
    result.size=stream.current_byte_size/4;
  }
  return result;
}
