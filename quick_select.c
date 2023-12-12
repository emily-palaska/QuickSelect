#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <mpi.h>

void partitioninplace(int A[], int n, int v, int *p);
int kselect(int A[], int n, int k, int argc, char *argv[]);

void print_array(int *A, int n) {
    for(int i = 0; i < n; i++) printf("%d ", A[i]);
    printf("\n");
}

int main(int argc, char *argv[]) {
    int A[20];     
    int n = sizeof(A) / sizeof(int);
    int p;
    
    //partitioninplace(A, n, 5, &p);
    kselect(A, n, 0, argc, argv);

    return 0;
}
// function that returns the kth smallest element of an int array A
// of size n, while taking the main arguments as input to handle
// the distribution of the data
int kselect(int A[], int n, int k, int argc, char *argv[]) {
	int num_procs, my_rank, pivot, p;
	int* A_local;
	int sum = 0;
	
	if (k > n) {
		printf("k element out of bouns\n");
		return 0;
	}
	
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
	
	
	
	
	
	
	// master proceess initializes array
	if (my_rank == 0) {
		for(int i = 0; i < n; i ++) A[i] = i + 1;
		print_array(A, n);
	}
	
	MPI_Bcast(A, n, MPI_INT, 0, MPI_COMM_WORLD);
	
	//decide size to be distributed and allocate memory	
	int local_size = n / num_procs;
	if (n % num_procs) local_size++;
    A_local = (int*)malloc(local_size * sizeof(int));
    
	//distribute the data accordingly
	MPI_Scatter(A, local_size, MPI_INT, A_local, local_size, MPI_INT, 0, MPI_COMM_WORLD);
	
	int local_sum = 0;
	int global_sum = 0;
	//calculate the sum
	if (my_rank < n) {
		int repeat_times;
		if (my_rank == num_procs - 1) repeat_times = n - my_rank * local_size; 
		else repeat_times = local_size;
		for(int i = 0; i < repeat_times; i ++) local_sum += A_local[i];
	}	
	//return to master process the result
	MPI_Reduce(&local_sum, &global_sum, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
	
	//print result
	if (my_rank == 0) {
		printf("The sum of all elements is: %d\n", global_sum);
	}
	
	free(A_local);
	MPI_Finalize();
	
	return global_sum;
}

// function that partitions an input array of size n according to a
// pivot value v and saves the pointer p of elements smaller than v
void partitioninplace(int A[], int n, int v, int *p) {
	int temp = 0;
	// pointers at each end of the array
    int left = 0;
    int right = n - 1;
	
	// repeats until left reaches right or all elements are checked
    while (left <= right && left <= n) {
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

	// decide the value of p according to the ending position of the
	// two pointers
    if (A[left] >= v)
        *p = left;
    else
        *p = left + 1;
    return;
}
