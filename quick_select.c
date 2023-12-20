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
    
    kselect(A, n, 6, argc, argv);
    	
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
	int pointers[3] = {0, 0, local_size - 1};
	
	//-----------------------------------------------------------------
	int counter = 0;
	while(counter < 10) {
		counter++;
		
		MPI_Barrier(MPI_COMM_WORLD);	

		// PROCCESS LOGIC
		if (my_rank < n) {			
			
			printf("Process %d performs partition to (%d, %d)\n", my_rank, pointers[0], pointers[2]);
			
			// one element left
			if (pointers[2] == pointers[0])
				pointers[1] = pointers[0];
			// more than one elements are left
			else
				pointers[1] = partitioninplace(A_local, local_size, pivot, pointers[0], pointers[2]);
			
			//send poitners
			MPI_Send(pointers, 3, MPI_INT, 0, my_rank, MPI_COMM_WORLD);
		}
		
		MPI_Barrier(MPI_COMM_WORLD);
		if (my_rank == 0) {
			//receive pointers
			int recv_data[3];
			for(int i = 0; i < num_procs; i++) {
				MPI_Recv(recv_data, 3, MPI_INT, i, i, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				left[i] = recv_data[0];
				right[i] = recv_data[1];
			}
			printf("Left is:\t");
			print_array(left, num_procs);
			printf("Right is:\t");
			print_array(right, num_procs);
		
			// elements smaller than pivot
			int sum = 0;
			for(int i = 0; i < num_procs; i++) sum += right[i] - left[i];
			printf("Found %d elements smaller than %d\n", sum, pivot);
			
			//checks for next step
			if (sum + 1 == k) {
				printf("Found kth smallest element.\n\n");
				termination_signal = 1;
			}
			else {
				if (sum + 1 > k) {
					printf("Checking on the left part\n\n");
					direction = 0;
					}
				else {
					printf("Checking on the right part.\n\n");
					direction = 1;
					k = k - sum;
				}
				
					
			}		
		}		
		// Exit the loop if termination signal is broadcasted
		MPI_Barrier(MPI_COMM_WORLD);
		MPI_Bcast(&termination_signal, 1, MPI_INT, 0, MPI_COMM_WORLD);
		if (termination_signal) break;
		
		// Broadcast pivot sender, k and direction
		MPI_Bcast(&direction, 1, MPI_INT, 0, MPI_COMM_WORLD);
		MPI_Bcast(&k, 1, MPI_INT, 0, MPI_COMM_WORLD);
		
		// handle direction if array still is usable
		if (direction) {
			 if (A_local[pointers[1]] == pivot) pointers[0] = pointers[1] + 1;
			 else pointers[0] = pointers[1];
		}
		else {
			if (A_local[pointers[1]] == pivot) pointers[2] = pointers[1] - 1;
			else pointers[2] = pointers[1];		
		}
		
		// find a process with usable values to make pivot sender
		if(my_rank == 0) {
			for(int i = 0; i < num_procs; i++) {
				if (right[i] != left[i]) {
					pivot_sender = i;
					break;
				}
			}
		}		
		MPI_Bcast(&pivot_sender, 1, MPI_INT, 0, MPI_COMM_WORLD);
		// pivot sender assigns new pivot value
		if (my_rank == pivot_sender) {
			if (direction) pivot = A_local[pointers[1]];	
			else pivot = A_local[pointers[1]];
			printf("New pivot is %d\n", pivot);
		}
		MPI_Bcast(&pivot, 1, MPI_INT, pivot_sender, MPI_COMM_WORLD);
	}
	//--------------------------------------------------------------------------
	// print result and free allocated memory
	if (my_rank == 0) {
		printf("kth smallest element in array is %d\n", pivot);
	
		free(left);
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
    // when pivot is found it should be placed at the breaking point
    if(pivotIndex != n) {
    	temp = A[pivotIndex];
    	A[pivotIndex] = A[left];
    	A[left] = temp;
    }
    
    return left;
}
