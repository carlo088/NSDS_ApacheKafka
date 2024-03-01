#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>
//#include <mpi.h>

#define LAKE_SIDE 100
#define FISH_QUANTITY 50
#define MINIMUM_DISTANCE 1.0
#define SIZES_NUMBER 5

typedef struct{
    double x,y,z;
    int size;
    int eaten;
} Fish;

Fish** fishes;

double distance(Fish* f1, Fish* f2){
    return sqrt(pow(f2->x - f1->x, 2) + pow(f2->y - f1->y, 2) + pow(f2->z - f1->z, 2) * 1.0);
}

//Fish sizes da definire prima e poi inserire nello spazio randomicamente (ogni fish è nel metro cubo)
//assegnando le coordinate al fish

int overlapping(Fish* f1, Fish* f2, double min_distance){

    return distance(f1,f2) <= min_distance;

}

void fish_generation(int quantity, double min_distance, int size){

    //Initialize the whole array of fishes
    fishes = malloc(quantity*sizeof(Fish*));
    
    srand(time(NULL));

    for (int i = 0; i < quantity; i++){

        fishes[i] = malloc(sizeof(Fish));

        fishes[i]->x = (double)rand() / RAND_MAX * LAKE_SIDE;
        fishes[i]->y = (double)rand() / RAND_MAX * LAKE_SIDE;
        fishes[i]->z = (double)rand() / RAND_MAX * LAKE_SIDE;
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

    fish_generation(FISH_QUANTITY, MINIMUM_DISTANCE, SIZES_NUMBER);
    
    for (int i = 0; i < FISH_QUANTITY; i++){
        printf("coordinate pesce %d: %f,%f,%f\n", i, fishes[i]->x,fishes[i]->y,fishes[i]->z);
    }
    
}