#include <iostream>
#include "structs.h"
#include "MetadataManager.h"
#include <vector>
#include <algorithm>
#include <fstream>
#include <queue>
#include <mutex>
#include <sys/time.h>
#include <condition_variable>
#include <unistd.h>
#include <thread>
#include <atomic>
#include <cstring>

std::mutex queue;
std::mutex mtx;
std::condition_variable cv;

void generateRequests(std::queue <StreamRequest>* iq, int number_requests, int number_streams[2],
                      int number_clients, int max_size_request, MetadataManager *MM);
void printRequest(StreamRequest request);
void extractRequest(std::vector<StreamResponse> responses, StreamRequest *request);

void thread(std::queue <StreamRequest>  *input_queue,
            std::queue <StreamResponse> *output_queue, // TODO: Change to an actual response for the server
            MetadataManager *MM,
            std::atomic<bool>& working){

//    std::unique_lock<std::mutex> lck(mtx);
    StreamRequest request;
    while(working){
        queue.lock();
        if(!input_queue->empty()){
            request = input_queue->front();
            input_queue->pop();
            queue.unlock();

            std::vector<StreamResponse> responses = MM->getStream(request);

            extractRequest(responses, &request);

            printRequest(request);
        }
        else{
            queue.unlock();
//            cv.wait(lck);
//            sleep(3);

        }
    }
}

int main() {
    int number_clients = 5;
    int number_streams[2] = {10, 10}; //Memory, Disk
    int number_requests = 100;

    int max_size_request = 3;

    srand(time(nullptr));
    std::atomic<bool> running { true } ;

    int num_threads = 1;

    struct timeval tv1, tv2;

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    std::queue <StreamRequest>  input_queue;
    std::queue <StreamResponse> output_queue;

    MetadataManager* MM = MetadataManager::getInstance();
    generateRequests(&input_queue, number_requests, number_streams, number_clients, max_size_request, MM);

    gettimeofday(&tv1, NULL);

    for (int i=0; i<num_threads; i++)
        threads.emplace_back(std::thread(thread, &input_queue, &output_queue, MM, std::ref(running)));

    // TODO: for now, maybe later we can detect sigint
    while(!input_queue.empty()){

    }

    running = false;
//    cv.notify_all(); // This, in time, should be done when adding data to the queue
    for(unsigned int i=0; i<threads.size(); ++i){
        threads.at(i).join();
    }

    gettimeofday(&tv2, NULL);
    printf ("Total time (GETTIME) = %f seconds\n",
            (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
            (double) (tv2.tv_sec - tv1.tv_sec));
}

void generateRequests(std::queue <StreamRequest>* iq, int number_requests, int number_streams[2],
        int number_clients, int max_size_request, MetadataManager *MM){
//    std::cout << "Number fo streams in memory " << number_streams[0] << std::endl;
//    std::cout << "Number fo streams on disk " << number_streams[1] << std::endl;

    int total_streams = number_streams[0] + number_streams[1];

    for(int i=0; i<number_streams[0]; i++){
        MM->registerStream(i, 0);
    }

    for(int i=0; i<number_streams[1]; i++){
        MM->registerStream(i, 1);
    }

    for(int i=0; i<number_clients; i++){
        MM->registerClient(i);
    }

    for(int i=0; i<number_requests; i++){
        StreamRequest temp;
        temp.size = 3;//(rand() % max_size_request) + 1;
        temp.stream = 1;//rand() % total_streams;
        temp.client = 0;//rand() % number_clients;
        temp.buffer = new char[temp.size];
        iq->push(temp);
    }
}

void printRequest(StreamRequest request){
    std::cout << "Request " << request.client << "-" << request.stream << ": ";
    for (uint i =0; i < request.size; i++)
        std::cout << request.buffer[i] << ", ";
    std::cout << " " << std::endl;

}

void extractRequest(std::vector<StreamResponse> responses, StreamRequest *request){
    uint j= 0;
    for(auto response = responses.begin(); response < responses.end(); response++){
        if(response->type == 0){ //object in memory
            char* data = (char*) response->adress;
//            std::copy(data + response->offset,
//                      data + response->length - response->offset,
//                      request->buffer + j);
            std::memcpy(request->buffer + j,
                    data + response->offset,
                    response->length - response->offset
                    );
            j += request->size;
        }
        else if(response->type == 1){//object in drive
            std::ifstream myfile ((char const*)response->adress);
            if (myfile.is_open())
            {
                myfile.seekg(response->offset);
                myfile.read(request->buffer + j, response->length - response->offset);
                myfile.close();
            }
            else{
                std::cout << "File not found" << std::endl;
            }
        }
        else{
            std::cout << "File type ot implemented" << std::endl;
        }
    }
}
