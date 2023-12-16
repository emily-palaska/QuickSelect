#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <mpi.h>

int partitioninplace(int A[], int n, int v, int start, int end);
void kselect(int A[], int n, int k, int argc, char *argv[]);

// auxiliary function to print an int array of size n
void print_array(int *A, int n) {
    for(int i = 0; i < n; i++) printf("%d ", A[i]);
    printf("\n");
}

int main(int argc, char *argv[]) {
    int A[20] = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11};    
    int n = sizeof(A) / sizeof(int);
    
    kselect(A, n, 4, argc, argv);
    	
    return 0;
}


// function that returns the kth smallest element of an int array A
// of size n, while taking the main arguments as input to handle
// the distribution of the data
void kselect(int A[], int n, int k, int argc, char *argv[]) {
	int num_procs, my_rank, pivot, pivot_sender;
	int direction; // 0 -> left & 1 -> right
	int termination_signal = 0;
	int* left;
	int* right;
	int* A_local;
	
	if (k > n) {
		printf("k element out of bouns\n");
		return;
	}
	
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);	
	
	// master proceess logic
	if (my_rank == 0) {
		left = malloc(num_procs * sizeof(int));
		right = malloc(num_procs * sizeof(int));
		pivot = A[0];
		direction = 1;
		
		// PRINTING
		print_array(A, n);
		printf("pivot = %d\n", pivot);
		//if(k % 10 == 1) printf("%dst ", k);
		//else if(k % 10 == 2) printf("%dnd ", k);
		//else if(k % 10 == 3) printf("%drd ", k);
		//else printf("%dth ", k);
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

		// PROCCESS LOGIC
		if (my_rank < n) {
			//handle direction
			if(direction) pointers[0] = pointers[1];
			else pointers[2] = pointers[1];	

			//printf("Process %d BEFORE partition: ", my_rank);
			//print_array(A_local, local_size);
			
			if (pointers[2] - pointers[0] != 1) {
				pointers[1] = partitioninplace(A_local, local_size, pivot, pointers[0], pointers[2]);
			}
			else {
				if (A_local[pointers[0]] < pivot) pointers[1] = 0;
				else pointers[1] = 1;
			}
			
			printf("Process %d AFTER partition:  ", my_rank);
			for(int i = pointers[0]; i <= pointers[1]; i++) printf("%d ", A_local[i]);
			printf("\n");
			
			//send poitners
			MPI_Send(pointers, 3, MPI_INT, 0, my_rank, MPI_COMM_WORLD);
			//printf("Process %d sent pointers array.\n", my_rank);
		}
		
		MPI_Barrier(MPI_COMM_WORLD);
		if (my_rank == 0) {
			//receive pointers
			int recv_data[3];
			
			for(int i = 0; i < num_procs; i++) {
				MPI_Recv(recv_data, 3, MPI_INT, i, i, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				//printf("Process %d received pointers array.\n", my_rank);
				left[i] = recv_data[0];
				right[i] = recv_data[1];
			}
			//print received pointers
			printf("left:  ");
			print_array(left, num_procs);
			printf("right: ");
			print_array(right, num_procs);
			
			int sum = 0;
			for(int i = 0; i < num_procs; i++) sum = sum + right[i] - left[i];
			printf("%d elements smaller than %d found\n", sum, pivot);
			sum++;
			
			//checks for next step
			if (sum == k) {
				printf("Found kth smallest element.\n\n");
				termination_signal = 1;
			}
			else if (sum > k) {
				printf("Checking on the left part\n\n");
				direction = 0;
				
				// find a process with usable values to make pivot sender
				for(int i = 0; i < num_procs; i++) {
					if (right[i] != left[i]) {
						pivot_sender = i;
						break;
					}
				}		
			}
			else {
				printf("Checking on the right part.\n\n");
				direction = 1;
				k = k - sum;
			}
		}		
		// exit the while loop if termination signal is broadcasted
		MPI_Barrier(MPI_COMM_WORLD);
		MPI_Bcast(&termination_signal, 1, MPI_INT, 0, MPI_COMM_WORLD);
		if (termination_signal) break;
		
		// Broadcast pivot sender and direction
		MPI_Bcast(&direction, 1, MPI_INT, 0, MPI_COMM_WORLD);
		MPI_Bcast(&pivot_sender, 1, MPI_INT, 0, MPI_COMM_WORLD);
		// pivot sender assigns new pivot value
		if (my_rank == pivot_sender) {
			if (direction) {
				for(int i = 0; i < local_size; i++)
					if(A_local[i] >= pivot) {
						pivot = A_local[i];
						break;
					}		
			}
			else {
				for(int i = 0; i < local_size; i++)
					if(A_local[i] <= pivot) {
						pivot = A_local[i];
						break;
					}			
			}
			printf("pivot = %d\n", pivot);
		}
		
		MPI_Barrier(MPI_COMM_WORLD);
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
// result is NUMBER OF ELEMENTS SMALLER THAN P
int partitioninplace(int A[], int n, int v, int start, int end) {
	int temp = 0;
	// pointers at each end of the array (as specified by input)
    if(end > n - 1) {
    	printf("End = %d out of bounds.\n", end);
    	return 1;
    }
    else if(start < 0) {
    	printf("Start = %d out of bouns\n", start);
    	return 1;
    }
    int left = start;
    int right = end;
	
	
	// repeats until left reaches right or all elements are checked
    while (left <= right && left < end) {
        if (A[left] >= v && A[right] < v) {
        // when elements at two pointers are on the wrong "side" of
        // the array, swap them
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
    return left;
}
