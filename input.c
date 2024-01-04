#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

int main(int argc, char *argv[]) {
	if (argc != 2) {
		printf("false arguments\n");
		exit(1);
	}
    int k = atoi(argv[1]);
    
    int num_procs, my_rank, pivot, pivot_sender, direction, breakpoint, local_size, n;
	int termination_signal = 0;
	int* A_local;
	
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
	
	//ALLOCATE MEMORY LOCALLY with Scatter
	
	//master initilizes array
	if (my_rank == 0) {
		int A[] = {1, 2, 3, 4, 5, 6, 7, 8};
		n = sizeof(A) / sizeof(int);	
	}
	MPI_Bcast(&n, 1, MPI_INT, pivot_sender, MPI_COMM_WORLD);
	
	// decide size to be distributed	
	int local_size = n / num_procs;	
	if (n % num_procs) local_size++;
	
	// calculate how many processes will have data distributed to them (useful when n is relatively small)
	int activeProcs;
	if (num_procs >= n) activeProcs = n;
	else {
		activeProcs = n / local_size;
		if (n % local_size) activeProcs++;
	}
	if (!my_rank) printf("Using %d processes\n\n", activeProcs);
	
	// allocate memory and distribute data accordingly
	A_local = (int*)malloc(local_size * sizeof(int));
	if (A_local == NULL) {
		perror("Memory allocation failed");
		exit(1);
	}		
	MPI_Scatter(A, local_size, MPI_INT, A_local, local_size, MPI_INT, 0, MPI_COMM_WORLD);
	
	
	// trim size of last process when needed
	if (my_rank * local_size < n && (my_rank + 1) * local_size > n) local_size = n - my_rank * local_size;
	
	printf("Process %d size: %d\n", my_rank, local_size);
	free(A_local);	
}
