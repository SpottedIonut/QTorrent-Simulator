#include <mpi.h>
#include <pthread.h>
#include <iostream>
#include <stdio.h>
#include <fstream>
#include <stdlib.h>
#include <vector>
#include <string> 
#include <map>


#define TRACKER_RANK 0
#define MAX_FILES 10
#define MAX_FILENAME 15
#define HASH_SIZE 32
#define MAX_CHUNKS 100

using namespace std;

class sworm {
    public:
        string filename;
        int no_segments;
        vector<string> segments;
        vector<int> owners;

        sworm() {
            this->filename = "";
            this->no_segments = 0;
        }

        sworm(string filename) {
            this->filename = filename;
        }
};

class file_t {
    public:
        string filename;
        int no_segments;
        vector<string> segments;

        file_t(string filename, int no_segments) {
            this->filename = filename;
            this->no_segments = no_segments;
        }
};

void read_config(int rank, vector<file_t> &owned_files, vector<string> &desired_files);

void send_config(vector<file_t> files);

void receive_config(int numtasks, map<string, sworm> &tracked_files);

void *download_thread_func(void *arg)
{
    int rank = *(int*) arg;

    return NULL;
}

void *upload_thread_func(void *arg)
{
    int rank = *(int*) arg;

    return NULL;
}

void tracker(int numtasks, int rank) {
    map<string, sworm> files;

    // Salvez toate configuratiile:
    receive_config(numtasks, files);
    
    for (auto it = files.begin(); it != files.end(); it++) {
        cout << it->first << endl;
        cout << it->second.no_segments << endl;
        
        for (int i = 0; i < it->second.owners.size(); i++) {
            cout << it->second.owners[i] << endl;
        }
    }
}

void peer(int numtasks, int rank) {
    pthread_t download_thread;
    pthread_t upload_thread;
    void *status;
    int r;

    
    vector<file_t> files;
    vector<string> desired_files;

    // Citesc fisierul de configuratie
    read_config(rank, files, desired_files);
    
    // verificare
    // for (int i = 0; i < files.size(); i++) {
    //     cout << files[i].filename << " " << files[i].no_segments << endl;
    //     for (int j = 0; j < files[i].no_segments; j++) {
    //         cout << files[i].segments[j] << endl;
    //     }
    // }
    // for (int i = 0; i < desired_files.size(); i++) {
    //     cout << desired_files[i] << endl;
    // }

    // informez trackerul in privinta fisierelor pe care le detin:
    send_config(files);


    r = pthread_create(&download_thread, NULL, download_thread_func, (void *) &rank);
    if (r) {
        printf("Eroare la crearea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_create(&upload_thread, NULL, upload_thread_func, (void *) &rank);
    if (r) {
        printf("Eroare la crearea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_join(download_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_join(upload_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de upload\n");
        exit(-1);
    }
}
 
int main (int argc, char *argv[]) {
    int numtasks, rank;
 
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "MPI nu are suport pentru multi-threading\n");
        exit(-1);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == TRACKER_RANK) {
        tracker(numtasks, rank);
    } else {
        peer(numtasks, rank);
    }
    MPI_Finalize();
}

void read_config(int rank, vector<file_t> &owned_files, vector<string> &desired_files) {
    int no_files_temp, no_sections_temp;
    string filename_temp;
    
    ifstream in;
    char path[30];
    snprintf(path, sizeof(path), "tests/test1/in%d.txt", rank);
    in.open(path);
    if (!in) {
        printf("Fisierul %s nu a fost gasit\n", path);
        exit(-1);
    }

    in >> no_files_temp;
    for (int i = 0; i < no_files_temp; i++) {
        in >> filename_temp;
        in >> no_sections_temp;
        file_t temp(filename_temp, no_sections_temp);

        // extrag hash-urile sectiunilor:
        for (int j = 0; j < no_sections_temp; j++) {
            char hash_temp[HASH_SIZE + 1];
            in >> hash_temp;
            temp.segments.push_back(hash_temp);
        }
        owned_files.push_back(temp);
    }

    // citesc fisierele dorite
    in >> no_files_temp;
    for (int i = 0; i < no_files_temp; i++) {
        in >> filename_temp;
        desired_files.push_back(filename_temp);
    }

    in.close();
}

void send_config(vector<file_t> files) {
    int no_files = files.size();
    MPI_Send(&no_files, 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD);
    for (int i = 0; i < files.size(); i++) {
        MPI_Send(files[i].filename.c_str(), MAX_FILENAME + 1,  MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
        // trimit segmentele acum, incepand cu numarul lor
        MPI_Send(&files[i].no_segments, 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD);
        for (int j = 0; j < files[i].no_segments; j++) {
            const char *segment_temp = files[i].segments[j].c_str();
            MPI_Send(segment_temp, HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
        }
    }
}

void receive_config(int numtasks, map<string, sworm> &tracked_files) {
    // iterez prin toate taskurile, si inregistrez toate 
    for (int i = 1; i < numtasks; i++) {
        // pentru taskul i:
        int no_files;
        MPI_Recv(&no_files, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        for (int j = 0; j < no_files; j++) {
            char filename_temp[MAX_FILENAME + 1];
            MPI_Recv(filename_temp, MAX_FILENAME + 1, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            string filename(filename_temp);

            if (tracked_files.find(filename) == tracked_files.end()) {
                // daca fisierul curent nu este indexat, atunci creez un sworm si il adaug la lista
                sworm new_sworm(filename);
                MPI_Recv(&new_sworm.no_segments, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                for (int k = 0; k < new_sworm.no_segments; k++) {
                    char segment_temp[HASH_SIZE + 1];
                    MPI_Recv(segment_temp, HASH_SIZE + 1, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                    string new_segment(segment_temp);
                    new_sworm.segments.push_back(new_segment);
                }
                new_sworm.owners.push_back(i);
                tracked_files[filename] = new_sworm;

            } else {
                // daca fisierul curent este indexat, atunci doar adaug noul owner:
                tracked_files[filename].owners.push_back(i);

            }
        }
    }
}


// TODO: sa verific posibilitatea sa nu am niciun fisier la inceput / sa nu am niciun fisier dorit