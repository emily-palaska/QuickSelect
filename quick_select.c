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
    int A[10];    
    int n = sizeof(A) / sizeof(int);
    randperm(n, A);
    
    kselect(A, n, 1, argc, argv);
    	
    return 0;
}


/*
	A i an int array with size n
	kselect finds the the element in position k of the sorted A
	
	for the direction: 0 -> left & 1 -> right
*/
void kselect(int A[], int n, int k, int argc, char *argv[]) {
	int num_procs, my_rank, pivot, pivot_sender, direction;
	int termination_signal = 0;
	
	int* left;
	int* middle;
	int* right;
	int* A_local;
	
	if (k > n && !my_rank) {
		printf("k element out of bouns\n");
		return;
	}
	
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);	
	
	// master process initialization
	if (my_rank == 0) {
		left = malloc(num_procs * sizeof(int));
		middle = malloc(num_procs * sizeof(int));
		right = malloc(num_procs * sizeof(int));
		pivot = A[0];
		direction = 1;
		
		// PRINTING		
		print_array(A, n);
		printf("New pivot is %d\n", pivot);
	}
	
	//decide size to be distributed and allocate memory	
	int local_size = n / num_procs;
	if (n % num_procs) local_size++;
	A_local = (int*)malloc(local_size * sizeof(int));
	
	//distribute the data accordingly
	MPI_Scatter(A, local_size, MPI_INT, A_local, local_size, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Bcast(&pivot, 1, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Bcast(&direction, 1, MPI_INT, 0, MPI_COMM_WORLD);
	
	// change local_size if memory not perfectly divided
	if (my_rank == num_procs - 1) local_size = n - my_rank * local_size;
	//pointer to the available parts of the local arrays
	int pointers[2] = {0, local_size - 1};
	int breakpoint = 0;
	
	//-----------------------------------------------------------------
	int counter = 0;
	while(counter < 10) {
		counter++;
		
		MPI_Barrier(MPI_COMM_WORLD);	

		// PROCCESS LOGIC
		if (my_rank < n) {
			if (pointers[1] == local_size) pointers[1]--;			
			
			printf("Process %d performs partition to (%d, %d)\n", my_rank, pointers[0], pointers[1]);
			// zero elements left
			if (pointers[0] >= local_size) breakpoint = local_size;
			else if (pointers[1] <= 0) breakpoint = 0; 
			// one element left
			else if (pointers[1] == pointers[0]) {
				if (pivot >= A_local[pointers[0]]) breakpoint = pointers[0] + 1;
				else breakpoint = pointers[0];
			}
			// more than one elements are left
			else
				breakpoint = partitioninplace(A_local, local_size, pivot, pointers[0], pointers[1]);
			printf("Process %d array after partition: ", my_rank);
			for(int i = pointers[0]; i <= pointers[1]; i++) printf("%d ", A_local[i]);
			printf("\n");
			
			//send poitners
			MPI_Send(&breakpoint, 1, MPI_INT, 0, my_rank, MPI_COMM_WORLD);
		}
		
		if (my_rank == 0) {
			//receive pointers
			for(int i = 0; i < num_procs; i++) {
				MPI_Recv(&middle[i], 1, MPI_INT, i, i, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			}		
			// elements smaller than pivot
			int elementsSmallerThanPivot = 0;			
			for(int i = 0; i < num_procs; i++) {
				elementsSmallerThanPivot += middle[i];
			}
			printf("Found %d elements smaller than %d\n", elementsSmallerThanPivot, pivot);
			
			//checks for next step
			if (elementsSmallerThanPivot + 1 == k) {
				printf("Found kth smallest element.\n");
				termination_signal = 1;
			}
			else {
				if (elementsSmallerThanPivot + 1 > k) {
					printf("Checking on the left part\n");
					direction = 0;
					}
				else {					
					direction = 1;
					printf("Checking on the right part.\n");
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
		if (direction) pointers[0] = breakpoint;
		else pointers[1] = breakpoint;
		
		// exclude the pivot in the next step
		if (my_rank == pivot_sender) {
				int pivotIndex;
				for(int i = 0; i < local_size; i++)
					if (A_local[i] == pivot) {
						pivotIndex = i;
						break;
					}
				
				
				A_local[pivotIndex] = A_local[pointers[!direction]];
				A_local[pointers[!direction]] = pivot;
				
				if (direction) pointers[!direction]++;
				else pointers[!direction]--;
		}
		
		
		MPI_Barrier(MPI_COMM_WORLD);
		// assign new pivot sender
		
		if (my_rank < n) {
			MPI_Send(pointers, 2, MPI_INT, 0, my_rank, MPI_COMM_WORLD);
		}
		if (my_rank == 0) {
			int recv_data[2];
			for(int i = 0; i < num_procs; i++) {
				MPI_Recv(&recv_data, 2, MPI_INT, i, i, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				if (recv_data[0] <= recv_data[1]) pivot_sender = i;
			}	
		}
			
		MPI_Bcast(&pivot_sender, 1, MPI_INT, 0, MPI_COMM_WORLD);
		MPI_Barrier(MPI_COMM_WORLD);
		
		// pivot sender assigns new pivot value
		if (my_rank == pivot_sender) {
			pivot = A_local[pointers[0]];
			printf("\nNew pivot is %d from process %d\n", pivot, my_rank);
		}
		MPI_Bcast(&pivot, 1, MPI_INT, pivot_sender, MPI_COMM_WORLD);
	}
	//--------------------------------------------------------------------------
	// print result and free allocated memory
	if (my_rank == 0) {
		printf("kth smallest element in array is %d\n", pivot);
	
		free(left);
		free(middle);
		free(right);
	}
	free(A_local);	
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();
	return;
}

// function that partitions an input array of size n according to a pivot value v
int partitioninplace(int A[], int n, int v, int start, int end) {
	int temp = 0;
	int pivotIndex= n;
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
    	// check if pivot is found
    	if (A[left] == v) pivotIndex = left;
    	else if (A[right] == v) pivotIndex = right;
    	 
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
