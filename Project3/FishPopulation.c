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
#define NUM_DAYS 1
#define DAY_SECONDS 86400
#define TIME_STEP 50

typedef struct{
    double x, y, z;
    double sx, sy, sz;
    int size;
    int eaten;
} Fish;

//Global array use to store updates made by all the processes gathering the informations
Fish* fishes;

double distance(Fish* f1, Fish* f2){
    return sqrt(pow(f2->x - f1->x, 2) + pow(f2->y - f1->y, 2) + pow(f2->z - f1->z, 2) * 1.0);
}


//1 ogni processo gestisce (aggiorna) le posizioni di un subset fishes gestendo le eccezioni di posizioni
//su un array locale
//2 -> aggiorno l'array globale (allgather)
//barrier per avere tutte le computazioni fatte
//3 ogni processo trova i conflitti (pesci mangiano pesci) all'interno di un subset dell'array
//4 processo 0 fa la free


MPI_Datatype fish_definition(){

    int block_lengths[8] = {1,1,1,1,1,1,1,1};

    //Types of the structure's fields
    MPI_Datatype types[8] = {MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_INT,MPI_INT};
    
    //Array of displacements within the structure
    //MPI_Aint used to inform MPI about the type of a variable passed to the routine 
    MPI_Aint offsets[8];

    offsets[0] = offsetof(Fish,x);
    offsets[1] = offsetof(Fish,y);
    offsets[2] = offsetof(Fish,z);
    offsets[3] = offsetof(Fish,sx);
    offsets[4] = offsetof(Fish,sy);
    offsets[5] = offsetof(Fish,sz);
    offsets[6] = offsetof(Fish,size);
    offsets[7] = offsetof(Fish,eaten);

    MPI_Datatype mpi_fish_type;

    //Creates an MPI datatype from a general set of datatypes, displacements and block sizes
    MPI_Type_create_struct(8, block_lengths, offsets, types, &mpi_fish_type);

    //Commits the datatype
    MPI_Type_commit(&mpi_fish_type);

    return mpi_fish_type;
}

int overlapping(Fish* f1, Fish* f2, double min_distance){

    return distance(f1,f2) <= min_distance;

}

void fish_generation(int quantity, double min_distance, int size){

    //Initializes the whole array of fishes
    fishes = malloc(quantity*sizeof(Fish));
    
    srand(time(NULL));

    for (int i = 0; i < quantity; i++){

        fishes[i].x = (double)rand() / RAND_MAX * LAKE_SIDE;
        fishes[i].y = (double)rand() / RAND_MAX * LAKE_SIDE;
        fishes[i].z = (double)rand() / RAND_MAX * LAKE_SIDE;

        fishes[i].sx = -MAXIMUM_SPEED + (2*MAXIMUM_SPEED) * ((double)rand() / RAND_MAX);
        fishes[i].sy = -MAXIMUM_SPEED + (2*MAXIMUM_SPEED) * ((double)rand() / RAND_MAX);
        fishes[i].sz = -MAXIMUM_SPEED + (2*MAXIMUM_SPEED) * ((double)rand() / RAND_MAX);
        fishes[i].size = rand() % size + 1;
        fishes[i].eaten = 0;

        for (int j = i-1; j >= 0; j--){

            while(overlapping(&fishes[i], &fishes[j], min_distance)){

                fishes[i].x = (double)rand() / RAND_MAX * LAKE_SIDE;
                fishes[i].y = (double)rand() / RAND_MAX * LAKE_SIDE;
                fishes[i].z = (double)rand() / RAND_MAX * LAKE_SIDE;

                j = i-1;
            }

        }
    }
}

void position_update(int fish_index){

    double new_x, new_y, new_z;

    new_x = fishes[fish_index].x + TIME_STEP * fishes[fish_index].sx;
    new_y = fishes[fish_index].y + TIME_STEP * fishes[fish_index].sy;
    new_z = fishes[fish_index].z + TIME_STEP * fishes[fish_index].sz;

    //TODO: Solve the problem of racing conditions in updating
    
    //TODO: Forse if sono abbastanza, non serve while

    while (new_x < 0 || new_x > LAKE_SIDE){

        fishes[fish_index].sx = -MAXIMUM_SPEED + (2*MAXIMUM_SPEED) * ((double)rand() / RAND_MAX);
        new_x = fishes[fish_index].x + TIME_STEP * fishes[fish_index].sx;

    }

    

    while(new_y < 0 || new_y > LAKE_SIDE){

        fishes[fish_index].sy = -MAXIMUM_SPEED + (2*MAXIMUM_SPEED) * ((double)rand() / RAND_MAX);
        new_y = fishes[fish_index].y + TIME_STEP * fishes[fish_index].sy;

    }
    
    while(new_z < 0 || new_z > LAKE_SIDE){

        fishes[fish_index].sz = -MAXIMUM_SPEED + (2*MAXIMUM_SPEED) * ((double)rand() / RAND_MAX);;
        new_z = fishes[fish_index].z + TIME_STEP * fishes[fish_index].sz;

    }

    fishes[fish_index].x = new_x;
    fishes[fish_index].y = new_y;
    fishes[fish_index].z = new_z;

}

int main(int argc, char *argv[]){

    //MPI Setup
    MPI_Init(NULL, NULL);

    int rank;
    int num_procs;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    //Definition of an MPI_Datatype for the Fish struct
    MPI_Datatype mpi_fish = fish_definition();

    
    if (rank == 0){

        //Generation of fishes having random positions, size and speeds (directions)
        fish_generation(FISH_QUANTITY, MINIMUM_DISTANCE, SIZES_NUMBER);

        for (int i = 0; i < FISH_QUANTITY; i++){
            printf("coordinate pesce %d: %f,%f,%f\n", i, fishes[i].x,fishes[i].y,fishes[i].z);
            printf("velocità pesce %d: %f,%f,%f\n", i, fishes[i].sx,fishes[i].sy,fishes[i].sz);
        }

    }else{

        //Initializing the buffer to receive the fish array
        fishes = malloc(FISH_QUANTITY*sizeof(Fish));

    }

    //Broadcasting all the generated fishes to start the distributed computations
    MPI_Bcast(fishes, FISH_QUANTITY, mpi_fish, 0, MPI_COMM_WORLD);


    //Local array managed by a single process for updating positions
    Fish* local_fish_array;

    int local_fish_index;

    int* receive_counter = malloc(num_procs*sizeof(int));

    int* displacements = malloc(num_procs*sizeof(int));

    int to_send_counter;

    int spare_fishes = FISH_QUANTITY%num_procs;

    //Local fish array initialization: the first spare_fishes processes manage one fish more
    if (rank < spare_fishes){

        Fish* local_fish_array = malloc (((FISH_QUANTITY/num_procs) + 1)*sizeof(Fish));

    }else{

        Fish* local_fish_array = malloc ((FISH_QUANTITY/num_procs)*sizeof(Fish));

    }

    //Initializing AllGatherv necessary parameters
    for (int i = 0; i < num_procs; i++){

        if (i < spare_fishes){

            receive_counter[i] = FISH_QUANTITY/num_procs + 1;
            displacements[i] = i * ((FISH_QUANTITY/num_procs) + 1)

        }else{

            receive_counter[i] = FISH_QUANTITY/num_procs;
            displacements[i] = //TODO: DEVI SOMMARE PRIMA GLI ALTRI +1, SE CE NE SONO!

        }

        ;

    }



    //TODO: non necessaria?
    MPI_Barrier(MPI_COMM_WORLD);
    
    //Simulation and prints for each day
    for (int day = 0; day < NUM_DAYS; day++){

        //Compute positions aand eating day/timeStep times
        for (int t = 0; t < DAY_SECONDS/TIME_STEP; t++){

            local_fish_index = 0;

            //Each process works on a subset of the fishes
            for (int i = rank; i < FISH_QUANTITY; i += num_procs){
                
                position_update(i);


                //Access of the global array to get the update positions
                local_fish_array[local_fish_index].x = fishes[i].x;
                local_fish_array[local_fish_index].y = fishes[i].y;
                local_fish_array[local_fish_index].z = fishes[i].z;
                local_fish_array[local_fish_index].sx = fishes[i].sx;
                local_fish_array[local_fish_index].sy = fishes[i].sy;
                local_fish_array[local_fish_index].sz = fishes[i].sz;




                local_fish_index++;
            }

            //Gathering, for each process, al the updated positions in the global array
            //TODO:

            //Updating the status of the fishes considering the conflicts

        }

    }
    

    free(fishes);

    MPI_Finalize();
    
    
}