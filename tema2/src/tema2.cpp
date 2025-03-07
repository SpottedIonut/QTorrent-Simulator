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

#define REQUEST_SWORM_TAG 0
#define UPLOAD_TAG 1
#define DOWNLOAD_TAG 2

#define FINISH_FILE_TAG 3
#define FINISH_DOWNLOAD_TAG 4

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

        file_t() {
            this->filename = "";
            this->no_segments = 0;
        }
};

class download_t {
    public:
        int rank;
        vector<string> desired_files;
        vector<file_t> owned_files;

        download_t(int rank) {
            this->rank = rank;
        }
};

void read_config(int rank, download_t &download_info);

void send_config(vector<file_t> files);

void receive_config(int numtasks, map<string, sworm> &tracked_files);

void send_sworm(sworm sworm, int destination);

void check_segment(string segment, vector<string> segments, char *message);

int find_best_owner(vector<int> owners, int rank);

void print_files(vector<file_t> files, int rank);

file_t get_file(string filename, vector<file_t> files);

vector<string> sort_segments(file_t &current_file, vector<string> desired_segments);

sworm receive_sworm(string filename) {

    MPI_Send(filename.c_str(), MAX_FILENAME + 1, MPI_CHAR, TRACKER_RANK, REQUEST_SWORM_TAG, MPI_COMM_WORLD);
    sworm desired_sworm(filename);

        MPI_Recv(&desired_sworm.no_segments, 1, MPI_INT, TRACKER_RANK, REQUEST_SWORM_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE); 
        for (int j = 0; j < desired_sworm.no_segments; j++) {
            char segment_temp[HASH_SIZE + 1];
            MPI_Recv(segment_temp, HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK, REQUEST_SWORM_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            string segment(segment_temp);
            desired_sworm.segments.push_back(segment);
        }
        int no_owners;
        MPI_Recv(&no_owners, 1, MPI_INT, TRACKER_RANK, REQUEST_SWORM_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        for (int j = 0; j < no_owners; j++) {
            int owner;
            MPI_Recv(&owner, 1, MPI_INT, TRACKER_RANK, REQUEST_SWORM_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            desired_sworm.owners.push_back(owner);
        }
        return desired_sworm;
}

void download_protocol(file_t &current_file, int rank) {
    
    while (true) {
        // initiez cererea catre tracker:
        int segments_downloaded = 0;

        // astept raspunsul de la tracker cu swormul dorit: no_segments -> segments -> no_owners -> owners
        sworm desired_sworm = receive_sworm(current_file.filename);

        //verific daca am temrinat fisierul:
        if (current_file.no_segments == desired_sworm.no_segments) {
            break;
        }

        // descarc cate 10 segmente pana actualizez sworm-ul, sau ma opresc la finalul fisierului
        while (segments_downloaded < 10 && current_file.no_segments < desired_sworm.no_segments) {
            // aleg urmatorul segment de descarcat:
            string segment_to_download = desired_sworm.segments[current_file.no_segments]; // practi iau segmentul de unde am ramas

            // etapa 1 de download: verific daca ownerul are segmentul
            vector<int> valid_owners;
            int code;
            
            for (int i = 0; i < desired_sworm.owners.size(); i++) {
                int current_owner = desired_sworm.owners[i];

                // verific daca clientul i are segmentul cautat
                code = 1;
                // cout << "[ " << rank << " ]" << " Download |ET 1|, trimit la: " << current_owner << endl;
                
                MPI_Send(&code, 1, MPI_INT, current_owner, UPLOAD_TAG, MPI_COMM_WORLD);
                MPI_Send(current_file.filename.c_str(), MAX_FILENAME + 1, MPI_CHAR, current_owner, UPLOAD_TAG, MPI_COMM_WORLD);
                MPI_Send(segment_to_download.c_str(), HASH_SIZE + 1, MPI_CHAR, current_owner, UPLOAD_TAG, MPI_COMM_WORLD);

                // astept raspunsul de la owner
                char response[7];
                MPI_Recv(response, 7, MPI_CHAR, current_owner, DOWNLOAD_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                
                if (strcmp(response, "ACCEPT") == 0) {
                    // ajung aici daca ownerul detine segmentul dorit de mine, asa ca il adaug in lista de owneri
                    valid_owners.push_back(current_owner);
                }

            }
            
            // etapa 2 de download | cer numarul de upload-uri, pentru a balansa workload-ul
            int best_dest = find_best_owner(valid_owners, rank);
            //etapa 3 download cer segmentul de la ownerul ales
            code = 3;
            MPI_Send(&code, 1, MPI_INT, best_dest, UPLOAD_TAG, MPI_COMM_WORLD);
            MPI_Send(current_file.filename.c_str(), MAX_FILENAME + 1, MPI_CHAR, best_dest, UPLOAD_TAG, MPI_COMM_WORLD);
            MPI_Send(segment_to_download.c_str(), HASH_SIZE + 1, MPI_CHAR, best_dest, UPLOAD_TAG, MPI_COMM_WORLD);
            
            // astept segmentul de la owner
            char message[7];
            MPI_Recv(message, 7, MPI_CHAR, best_dest, DOWNLOAD_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            // cout << "[ " << rank << " ]" << " DOWNLOAD |ET 3| am primit raspunsul: " << message << " de la: " << best_dest << endl;
            if (strcmp(message, "ACCEPT") == 0) {
                // am primit segmentul
                current_file.no_segments++;
                segments_downloaded++;
                current_file.segments.push_back(segment_to_download);
            } else {
                // nu am primit segmentul
                cout << "EROARE: nu am primit segmentul de la owner\n";
                exit(-1);
            }
        }
        if (current_file.no_segments == desired_sworm.no_segments) {
            // sortez segmetele in ordinea din sworm
            current_file.segments = sort_segments(current_file, desired_sworm.segments);
            break;
        }
    }
}

void *download_thread_func(void *arg)
{
    download_t download_info = *(download_t *)arg;

    // parcurg lista de fisiere dorite si pentru fiecare aplic protocolul de download
    // potocol: 
    //  1. trimit numele fisierului dorit la tracker si astept sworm-ul
    //  2. cer de la toti ownerii numarul lor de upload-uri
    //  4. acum iterez prin lista de segmente si cer de la ownerul cu cel mai mic numar de upload-uri segmentul respectiv
    //  5. din 10 in 10 segmente, repet acest protocol

    

    // parcurg lista de fisiere dorite si pentru fiecare trimit cerere la tracker
    for (int i = 0; i < download_info.desired_files.size(); i++) {

        file_t current_file(download_info.desired_files[i], 0);
        download_protocol(current_file, download_info.rank);
        download_info.owned_files.push_back(current_file);
        // aici ar trb as anunt trackerul ca am terminat de downloadat fisierul

        string filename = current_file.filename;
        MPI_Send(filename.c_str(), MAX_FILENAME + 1, MPI_CHAR, TRACKER_RANK, FINISH_FILE_TAG, MPI_COMM_WORLD);
    }

    char message[] = "finish";
    MPI_Send(message, 6, MPI_CHAR, TRACKER_RANK, FINISH_DOWNLOAD_TAG, MPI_COMM_WORLD);
    
    // salvez toate fisierele detinute 
    print_files(download_info.owned_files, download_info.rank);

    return NULL;
}

void *upload_thread_func(void *arg)
{
    download_t client_info = *(download_t *) arg;
    int no_uploads = 0; // pentru balansarea workload-ului

    // la un moment dat o sa trebuiasca sa raspund la mai multe tipuri de cereri:
    // prima data o sa astept un numar care va semni
    // aici pot sa primesc atat cereri de fisiere cat si cereri de numar de upload-uri 

    // prima data vreau sa primesc un cod ca sa stiu ce dau mai departe:
    // 1 -> confirmare ca am fisierul => raspund cu "ACCEPT" / "REJECT"
    // 2 -> numar de uploaduri => raspund cu numarul de uploaduri
    // 3 -> cerere de segment => raspund cu segmentul respectiv
    // 4 -> trackerul ma informeaza ca s-a terminat download-ul
    // OBS: folosesc acelasi tag pentru toate requesturile din upload
    while (true) {

        int code;
        MPI_Status status;
        MPI_Recv(&code, 1, MPI_INT, MPI_ANY_SOURCE, UPLOAD_TAG, MPI_COMM_WORLD, &status);
        const int dest = status.MPI_SOURCE;
        
       char message[7];
        char filename[MAX_FILENAME + 1];
        char segment[HASH_SIZE + 1];
        file_t required_file;

        switch (code) {
            case 1: {
                // confirmare ca am segmentul pe care urmeaza sa il primesc
                MPI_Recv(filename, MAX_FILENAME + 1, MPI_CHAR, status.MPI_SOURCE, UPLOAD_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                
                // verific daca am segmentul
                string requested_filename(filename);
                
                // caut lista de segmnete corespunzatoare fisierului
                required_file = get_file(requested_filename, client_info.owned_files);
                // verific daca am avut fisierul respectiv:
                if (required_file.filename == "") {
                    strcpy(message, "REJECT");
                }
                // astept segmentul cautat
                MPI_Recv(segment, HASH_SIZE + 1, MPI_CHAR, status.MPI_SOURCE, UPLOAD_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                check_segment(segment, required_file.segments, message);
                
                MPI_Send(message, 7, MPI_CHAR, status.MPI_SOURCE, DOWNLOAD_TAG, MPI_COMM_WORLD);
                break;
            }
            case 2: {
                // trimit numaru de upload-uri
                MPI_Send(&no_uploads, 1, MPI_INT, status.MPI_SOURCE, DOWNLOAD_TAG, MPI_COMM_WORLD);
                break;
            }
            case 3: {
                // trimit fisierul cerut
                MPI_Recv(filename, MAX_FILENAME + 1, MPI_CHAR, status.MPI_SOURCE, UPLOAD_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                MPI_Recv(segment, HASH_SIZE + 1, MPI_CHAR, status.MPI_SOURCE, UPLOAD_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                required_file = get_file(filename, client_info.owned_files);
                check_segment(segment, required_file.segments, message);
                MPI_Send(message, 7, MPI_CHAR, status.MPI_SOURCE, DOWNLOAD_TAG, MPI_COMM_WORLD);
                break;
            }
            case 4: {
                return NULL;
                break;
            }
            default:
                break;
        }

        // de adaugat conditie de terminare, atunci cand primim de la tracker probabil
    }


    return NULL;
}

void tracker(int numtasks, int rank) {
    map<string, sworm> files;

    // Salvez toate configuratiile:
    receive_config(numtasks, files);

    // trimit mesaj de start
    char start[] = "start";
    
    MPI_Bcast(start, strlen(start) + 1, MPI_CHAR, TRACKER_RANK, MPI_COMM_WORLD);

    // astept cererile de la peeri
    int active_tasks = numtasks - 1;
    while (active_tasks > 0) {
        char filename[MAX_FILENAME + 1];
        MPI_Status status;
        MPI_Recv(filename, MAX_FILENAME + 1, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        // in functie de tag, raspund astfel:
        // REQUEST_SWORM_TAG -> raspund la cererea de sworm
        // FINISH_FILE_TAG -> anunt ca am terminat de downloadat fisierul
        // FINISH_DOWNLOAD_TAG -> marchez faptul ca un tag a terminat de downloadat toate fisierele
        switch(status.MPI_TAG) {
            case REQUEST_SWORM_TAG: {
                send_sworm(files[filename], status.MPI_SOURCE);
                files[filename].owners.push_back(status.MPI_SOURCE);
                break;
            }
            case FINISH_FILE_TAG: {
                // aici ar trebui sa marchez faptul ca un peer a terminat de downloadat un fisier
                // dar in implementarea temei, nu am nevoie sa fac ceva aici
                break;
            }
            case FINISH_DOWNLOAD_TAG: {
                active_tasks--;
                cout << "active tasks: " << active_tasks << endl;
                break;
            }
            default:
                break;
        }
    }
    cout << "am iesit din while\n";
    // informez clientii sa isi inchida thread-urile
    int code = 4;
    for (int i = 1; i < numtasks; i++) {
        MPI_Send(&code, 1, MPI_INT, i, UPLOAD_TAG, MPI_COMM_WORLD);
    }
}

void peer(int numtasks, int rank) {
    pthread_t download_thread;
    pthread_t upload_thread;
    void *status;
    int r;

    download_t download_info(rank);

    // Citesc fisierul de configuratie
    read_config(rank, download_info);

    // informez trackerul in privinta fisierelor pe care le detin:
    send_config(download_info.owned_files);

    cout << rank << " CHECKING FILES: " << download_info.owned_files.size() << endl;

    // astept mesajul de start
    char start[6];
    MPI_Bcast(start, 6, MPI_CHAR, TRACKER_RANK, MPI_COMM_WORLD);
    if (strcmp(start, "start") != 0) {
        printf("am primit altceva in loc de start\n");
        exit(-1);
    }

    // creez thread-urile de download si upload
    r = pthread_create(&download_thread, NULL, download_thread_func, (void *) &download_info);
    if (r) {
        printf("Eroare la crearea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_create(&upload_thread, NULL, upload_thread_func, (void *) &download_info);
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

void read_config(int rank, download_t &download_info) {
    int no_files_temp, no_sections_temp;
    string filename_temp;
    
    ifstream in;
    char path[30];
    snprintf(path, sizeof(path), "in%d.txt", rank);
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
        download_info.owned_files.push_back(temp);
    }

    // citesc fisierele dorite
    in >> no_files_temp;
    for (int i = 0; i < no_files_temp; i++) {
        in >> filename_temp;
        download_info.desired_files.push_back(filename_temp);
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

            sworm new_sworm(filename);
            MPI_Recv(&new_sworm.no_segments, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for (int k = 0; k < new_sworm.no_segments; k++) {
                char segment_temp[HASH_SIZE + 1];
                MPI_Recv(segment_temp, HASH_SIZE + 1, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                string new_segment(segment_temp);
                new_sworm.segments.push_back(new_segment);
            }
            new_sworm.owners.push_back(i);

            if (tracked_files.find(filename) == tracked_files.end()) {
                // daca fisierul curent nu este indexat, atunci il indexez:
                tracked_files[filename] = new_sworm;

            } else {
                // daca fisierul curent este indexat, atunci doar adaug noul owner:
                tracked_files[filename].owners.push_back(i);
            }
        }
    }
}

void send_sworm(sworm sworm, int dest) {
    MPI_Send(&sworm.no_segments, 1, MPI_INT, dest, REQUEST_SWORM_TAG, MPI_COMM_WORLD);
    for (int i = 0; i < sworm.no_segments; i++) {
        const char *segment_temp = sworm.segments[i].c_str();
        MPI_Send(segment_temp, HASH_SIZE + 1, MPI_CHAR, dest, REQUEST_SWORM_TAG, MPI_COMM_WORLD);
    }
    int no_owners = sworm.owners.size();
    MPI_Send(&no_owners, 1, MPI_INT, dest, REQUEST_SWORM_TAG, MPI_COMM_WORLD);
    for (int i = 0; i < no_owners; i++) {
        MPI_Send(&sworm.owners[i], 1, MPI_INT, dest, REQUEST_SWORM_TAG, MPI_COMM_WORLD);
    }
}

void check_segment(string segment, vector<string> segments, char *message) {
    // in upload, verific daca am segmentul cerut
    strcpy(message, "REJECT");
    for (int i = 0; i < segments.size(); i++) {
        if (segments[i] == segment) {
            strcpy(message, "ACCEPT");
        }
    }
}

int find_best_owner(vector<int> owners, int rank) {
    // trimit la fiecare owner valid, cerere de upload-uri
    vector<int> uploads;
    for (int i = 0; i < owners.size(); i++) {
        int code = 2;
        MPI_Send(&code, 1, MPI_INT, owners[i], UPLOAD_TAG, MPI_COMM_WORLD);
        int no_uploads;
        MPI_Recv(&no_uploads, 1, MPI_INT, owners[i], DOWNLOAD_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // cout << "[ " << rank << " ]" << " DOWNLOAD |ET 2| am primit de la: " << owners[i] << " numarul de uploads: " << no_uploads << endl;

        uploads.push_back(no_uploads);
    }

    // aleg cel mai nesolicitat owner
    int best_owner = 0;
    for (int i = 1; i < uploads.size(); i++) {
        if (uploads[i] < uploads[best_owner]) {
            uploads[best_owner] = uploads[i];
        }
    }
    
    return owners[best_owner];
}

file_t get_file(string filename, vector<file_t> files) {
    for (int i = 0; i < files.size(); i++) {
        if (files[i].filename == filename) {
            return files[i];
        }
    }
    return file_t();
}

vector<string> sort_segments(file_t &current_file, vector<string> desired_segments) {
    vector<string> sorted_segments;
    for (int i = 0; i < desired_segments.size(); i++) {
        for (int j = 0; j < current_file.segments.size(); j++) {
            if (desired_segments[i] == current_file.segments[j]) {
                sorted_segments.push_back(desired_segments[i]);
            }
        }
    }
    return sorted_segments;
}

void print_files(vector<file_t> files, int rank) {
    ofstream out;
    char path[MAX_FILENAME + 10];
    
    for (int i = 0; i < files.size(); i++) {
        snprintf(path, sizeof(path), "client%d_%s", rank, files[i].filename.c_str());
        out.open(path);
        for (int j = 0; j < files[i].segments.size(); j++) {
            out << files[i].segments[j] << endl;
        }
        out.close(); 
    }
}
