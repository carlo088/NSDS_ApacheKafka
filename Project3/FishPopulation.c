#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>
#include <mpi.h>

#define LAKE_SIDE 100
#define FISH_QUANTITY 50
#define MINIMUM_DISTANCE 1.0
#define SIZES_NUMBER 5
#define MAXIMUM_SPEED 2

typedef struct{
    double x, y, z;
    double sx, sy, sz;
    int size;
    int eaten;
} Fish;

//Global array use to store updates made by all the processes gathering the informations
Fish** fishes;

double distance(Fish* f1, Fish* f2){
    return sqrt(pow(f2->x - f1->x, 2) + pow(f2->y - f1->y, 2) + pow(f2->z - f1->z, 2) * 1.0);
}

//1 ogni processo gestisce (aggiorna) le posizioni di un subset fishes gestendo le eccezioni di posizioni
//su un array locale
//2 -> aggiorno l'array globale (allgather)
//barrier per avere tutte le computazioni fatte
//3 ogni processo trova i conflitti (pesci mangiano pesci) all'interno di un subset dell'array
//4 processo 0 fa la free


int overlapping(Fish* f1, Fish* f2, double min_distance){

    return distance(f1,f2) <= min_distance;

}

void fish_generation(int quantity, double min_distance, int size){

    //Initializes the whole array of fishes
    fishes = malloc(quantity*sizeof(Fish*));
    
    srand(time(NULL));

    for (int i = 0; i < quantity; i++){

        fishes[i] = malloc(sizeof(Fish));

        fishes[i]->x = (double)rand() / RAND_MAX * LAKE_SIDE;
        fishes[i]->y = (double)rand() / RAND_MAX * LAKE_SIDE;
        fishes[i]->z = (double)rand() / RAND_MAX * LAKE_SIDE;
        fishes[i]->sx = (double)rand() / RAND_MAX * MAXIMUM_SPEED;
        fishes[i]->sy = (double)rand() / RAND_MAX * MAXIMUM_SPEED;
        fishes[i]->sz = (double)rand() / RAND_MAX * MAXIMUM_SPEED;
        fishes[i]->size = rand() % size + 1;
        fishes[i]->eaten = 0;

        for (int j = i-1; j >= 0; j--){

            while(overlapping(fishes[i], fishes[j], min_distance)){

                fishes[i]->x = (double)rand() / RAND_MAX * LAKE_SIDE;
                fishes[i]->y = (double)rand() / RAND_MAX * LAKE_SIDE;
                fishes[i]->z = (double)rand() / RAND_MAX * LAKE_SIDE;

                j = i-1;
            }

        }
    }
}

int main(int argc, char *argv[]){

    MPI_Init(NULL, NULL);

    int rank;
    int num_procs;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
    
    if (rank == 0){

        //Generation of fishes having random positions, size and speeds (directions)
        fish_generation(FISH_QUANTITY, MINIMUM_DISTANCE, SIZES_NUMBER);

        for (int i = 0; i < FISH_QUANTITY; i++){
            printf("coordinate pesce %d: %f,%f,%f\n", i, fishes[i]->x,fishes[i]->y,fishes[i]->z);
            printf("velocità pesce %d: %f,%f,%f\n", i, fishes[i]->sx,fishes[i]->sy,fishes[i]->sz);
        }
    }

    //Local array managed by a single process for updating positions
    Fish** local_fish_array = malloc ((FISH_QUANTITY/num_procs)*sizeof(Fish*));

    //TODO: malloc on each pointer
    
    
    
    
}