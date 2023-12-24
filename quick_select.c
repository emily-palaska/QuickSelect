#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <mpi.h>
#include <time.h>

int partitioninplace(int A[], int n, int v, int start, int end);
void kselect(int A[], int n, int k, int argc, char *argv[]);

// auxiliary function to print an int array of size n
void print_array(int *A, int n) {
    for(int i = 0; i < n; i++) printf("%d ", A[i]);
    printf("\n");
}

// auxiliary function for validity tests
void randperm(int n, int *A) {
	int temp;
    // Seed for random number generation
    srand((unsigned int)time(NULL));

    // Initialize array with consecutive integers from 1 to n
    for (int i = 0; i < n; ++i) {
        A[i] = i + 1;
    }

    // Fisher-Yates shuffle algorithm
    for (int i = n - 1; i > 0; --i) {
        int j = rand() % (i + 1);
        temp = A[i];
        A[i] = A[j];
        A[j] = temp;
    }
}

int main(int argc, char *argv[]) {
    int A[1000];
    int n = sizeof(A) / sizeof(int);
    randperm(n, A);
    
    kselect(A, n, 532, argc, argv);

    return 0;
}


/*
	A i an int array with size n
	kselect finds the the element in position k of the sorted A
	
	for the direction: 0 -> left & 1 -> right
*/
void kselect(int A[], int n, int k, int argc, char *argv[]) {
	int num_procs, my_rank, pivot, pivot_sender, direction, breakpoint;
	int termination_signal = 0;
	int* A_local;

	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
	
	if (k > n && !my_rank) {
		printf("k element out of bouns\n");
		return;
	}	
		
	// decide size to be distributed and allocate memory	
	int local_size = n / num_procs;	
	if (n % num_procs) local_size++;
	
	// calculate how many processes will have data distributed to them
	int activeProcs;
	if (num_procs >= n) activeProcs = n;
	else {
		activeProcs = n / local_size;
		if (n % local_size) activeProcs++;
	}
	if (!my_rank) printf("Using %d processes\n", activeProcs);
	
	// allocate memory and distribute data accordingly
	
	A_local = (int*)malloc(local_size * sizeof(int));
	if (A_local == NULL) {
		perror("Memory allocation failed");
		exit(1);
	}	
	
	MPI_Scatter(A, local_size, MPI_INT, A_local, local_size, MPI_INT, 0, MPI_COMM_WORLD);
	
	
	// trim size of last process when needed
	if (my_rank * local_size < n && (my_rank + 1) * local_size > n) local_size = n - my_rank * local_size;
	
	// master process initialization
	if (my_rank == 0) {
		pivot = A[0];
		direction = 1;
		
		// PRINTING		
		//print_array(A, n);
		//printf("New pivot is %d\n", pivot);
	}
	MPI_Bcast(&pivot, 1, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Bcast(&direction, 1, MPI_INT, 0, MPI_COMM_WORLD);
	
	//pointer to the available parts of the local arrays
	int pointers[2] = {0, local_size - 1};
	
	//-----------------------------------------------------------------
	int counter = 0;
	while(counter < n) {
		counter++;
		
		MPI_Barrier(MPI_COMM_WORLD);	

		// PROCCESS LOGIC
		if (my_rank < activeProcs) {
			///if (pointers[1] == local_size) pointers[1]--;			
			
			//printf("Process %d performs partition to (%d, %d)\n", my_rank, pointers[0], pointers[1]);
			// zero elements left
			if (pointers[0] > pointers[1]) {}
			else if (pointers[0] >= local_size) breakpoint = local_size - 1;
			else if (pointers[1] < 0) breakpoint = 0; 
			// one element left
			else if (pointers[1] == pointers[0]) {
				if (A_local[pointers[0]] >= pivot) breakpoint = pointers[0];
				else breakpoint = pointers[0] + 1;
				// make array not usable
				//pointers[0]++;
			}
			// more than one elements are left
			else {
				breakpoint = partitioninplace(A_local, local_size, pivot, pointers[0], pointers[1]);
				
			}
			//printf("Process %d array after partition: ", my_rank);
			//for(int i = pointers[0]; i <= pointers[1]; i++) printf("%d ", A_local[i]);
			//printf("\n");
			
			//send poitners
			MPI_Send(&breakpoint, 1, MPI_INT, 0, my_rank, MPI_COMM_WORLD);
		}
		
		if (my_rank == 0) {			
			//receive breakpoints and sum them
			int recv_data;
			int elementsSmallerThanPivot = 0;
			for(int i = 0; i < activeProcs; i++) {
				MPI_Recv(&recv_data, 1, MPI_INT, i, i, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				elementsSmallerThanPivot += recv_data;
				//printf("Got %d from process %d\n", recv_data, i);
			}
			//printf("Found %d elements smaller than %d\n", elementsSmallerThanPivot, pivot);
			
			//checks for next step
			if (elementsSmallerThanPivot + 1 == k) {
				//printf("Found kth smallest element.\n");
				termination_signal = 1;
			}
			else {
				if (elementsSmallerThanPivot + 1 > k) {
					//printf("Checking on the left part\n");
					direction = 0;
					}
				else {					
					direction = 1;
					//printf("Checking on the right part.\n");
				}		
			}		
		}		
		// Exit the loop if termination signal is broadcasted
		MPI_Barrier(MPI_COMM_WORLD);
		MPI_Bcast(&termination_signal, 1, MPI_INT, 0, MPI_COMM_WORLD);
		if (termination_signal) break;
		
		// Broadcast direction
		MPI_Bcast(&direction, 1, MPI_INT, 0, MPI_COMM_WORLD);				
		
		// handle direction if array still is usable
		if (my_rank < activeProcs) {
			if (direction) pointers[0] = breakpoint;
			else pointers[1] = breakpoint - 1;
		}

		// exclude the pivot in the next step
		if (my_rank == pivot_sender) {
				int pivotIndex = local_size;
				for(int i = 0; i < local_size; i++) {
					if (A_local[i] == pivot) {
						pivotIndex = i;
						break;
					}
				}			
				
				// swap the pivot at end of array if it's found within the usable spectrum
				// ie when direction is right we swap it with the left end and increase the left end
				// vice verse for left direction. this ensures that each steps limits at least one
				// element, even if pivot appears many times
				if (pivotIndex <= pointers[1]) {
					A_local[pivotIndex] = A_local[pointers[!direction]];
					A_local[pointers[!direction]] = pivot;
				
					if (direction) pointers[!direction]++;
					else pointers[!direction]--;
				}
				
				// specific occasion that pivot is the only elemnt left
				if (pointers[0] > pointers[1] && direction) breakpoint++;
		}
		
		
		MPI_Barrier(MPI_COMM_WORLD);
		
		// assign new pivot sender
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
			//printf("\nNew pivot is %d from process %d\n", pivot, my_rank);
		}
		MPI_Bcast(&pivot, 1, MPI_INT, pivot_sender, MPI_COMM_WORLD);
	}
	//--------------------------------------------------------------------------
	// print result and free allocated memory
	if (my_rank == 0) {
		printf("kth smallest element in array is %d\n", pivot);
		printf("used %d steps to find it\n", counter);
	}
	free(A_local);	
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();
	return;
}

// function that partitions an input array of size n according to a pivot value v
int partitioninplace(int A[], int n, int v, int start, int end) {
	int temp = 0;
	// pointers at each end of the array (as specified by input)
    if(end > n - 1) {
    	printf("End = %d out of bounds.\n", end);
    	return 0;
    }
    else if(start < 0) {
    	printf("Start = %d out of bouns\n", start);
    	return 0;
    }
    int left = start;
    int right = end;
	
	// repeats until left reaches right or all elements are checked
    while (left <= right && left < end) {    	 
        if (A[left] >= v && A[right] < v) {
        	// elements at both pointers are on the wrong "side" of the array -> swap them
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
