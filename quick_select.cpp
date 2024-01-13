#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <mpi.h>
#include <time.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <inttypes.h>

int partitioninplace(uint32_t A[], int n, int v, int start, int end);
uint32_t *getData(long int *local_size, int my_rank, int num_procs);

int main(int argc, char *argv[]) {    
    int num_procs, my_rank, pivot_sender, direction, breakpoint;
	int termination_signal = 0;
	long int local_size;
	uint32_t* A_local;
	uint32_t pivot;
	
	time_t startTime, endTime;
	
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
	
	int k;
	int activeProcs = num_procs;
	
	//ALLOCATE MEMORY LOCALLY
	A_local = getData(&local_size, my_rank, num_procs);
	
	MPI_Barrier(MPI_COMM_WORLD);
	// master process initialization
	if (my_rank == 0) {
		printf("\nData received successfully\n");
		srand(time(NULL));
    	k = rand() % (local_size * num_procs) + 1;	
		pivot = A_local[0];		
	}
	
	double timePassed;
	startTime = time(NULL);
	direction = 1;
	MPI_Bcast(&pivot, 1, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Bcast(&k, 1, MPI_INT, 0, MPI_COMM_WORLD);
	
	//pointer to the available parts of the local arrays
	long int pointers[2] = {0, local_size - 1};
	
	//-----------------------------------------------------------------
	int counter = 0;
	while(counter < local_size * num_procs) {
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
	endTime = time(NULL);
	timePassed = difftime(endTime, startTime);
	// RESULT DISPLAY AND MEMORY DE-ALLOCATION
	if (my_rank == 0) {
		if (k % 10 == 1) printf("%dst ", k);
		else if (k % 10 == 2) printf("%dnd ", k);
		else if (k % 10 == 3) printf("%drd ", k);
		else printf("%dth ", k);
		printf("smallest element in array is %u\n", pivot);
		printf("Algorithm used %d steps and %.16f seconds\n", counter, timePassed);
	}
	
	
	free(A_local);
	MPI_Finalize(); 
   
    return 0;
}

// function that partitions an input array of size n according to a pivot value v
int partitioninplace(uint32_t A[], int n, int v, int start, int end) {	
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

uint32_t *getData(long int* local_size, int my_rank, int num_procs) {
	long int start, end;
	uint32_t *result;
	
	if (my_rank >= num_procs) {
		perror("Rank out of bounds");
		exit(1);
	}
	
	FILE *file;
    char filename[] = "skins.lst"; // Replace with your file name

    // Open the file in binary mode
    file = fopen(filename, "rb");
    
    if (file == NULL) {
        perror("Error opening file");
        exit(1);
    }
	
	// Get the size of the file
    fseek(file, 0, SEEK_END);
    long size = ftell(file);

    // Calculate the total number of uint32_t elements in the file
    long num_elements = size / sizeof(uint32_t);
	
	
	start = my_rank * num_elements / num_procs;	
	if (my_rank + 1 == num_procs)  end = num_elements;
	else end = (my_rank + 1) * num_elements / num_procs;
	
	if (start < 0 || end > num_elements) {
		perror("Limit out of bounds");
		exit(1);
	}
	*local_size = end - start;
	printf("Process %d size: %ld - %ld\n", my_rank, start, end);

	// Allocate memory
	result = (uint32_t*)malloc(*local_size * sizeof(uint32_t));
	if (result == NULL) {
		perror("Error in memory allocation");
		exit(1);
	}	
	
    // Read characters and convert to uint32_t
    uint32_t buffer;    
    fseek(file, start * sizeof(uint32_t), SEEK_SET);    
    for(int i = 0; i < end - start; i++) {
    	fread(&buffer, sizeof(uint32_t), 1, file);
        result[i] = buffer;
    }

    // Close the file
    fclose(file);
    return result;
}
