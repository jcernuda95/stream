#include <iostream>
#include "structs.h"
#include "MetadataManager.h"
#include "operators/abstract_stream_operator.h"
#include "stream_operator_factory.h"
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
#include <sstream>
#include <string>

std::mutex input_queue_lock;
std::mutex output_queue_lock;
std::mutex mtx;
std::condition_variable cv;

void generateRequests(std::queue <Operation>* iq, int number_requests, int number_streams[2],
                      int number_clients, int max_size_request, MetadataManager *MM);
void printRequest(std::vector<StreamRequest> requests);
void extractRequest(std::vector<StreamResponse> responses, StreamRequest *request);
std::string mes2str(Message m);
std::fstream& GotoLine(std::fstream& file, unsigned int num);

void thread(std::queue <Operation>  *input_queue,
            std::queue <Operation> *output_queue, // TODO: Change to an actual response for the server
            MetadataManager *MM,
            std::atomic<bool>& working){

//    std::unique_lock<std::mutex> lck(mtx);
    Operation op;
    while(working){
        input_queue_lock.lock();
        if(!input_queue->empty()){
            op = input_queue->front();
            input_queue->pop();
            input_queue_lock.unlock();

            for(int i = 0; i < op.requests.size(); i++){
                std::vector<StreamResponse> responses = MM->getStream(op.requests[i]);
                extractRequest(responses, &op.requests[i]);
            }

            stream_operator_factory& factory = stream_operator_factory::getInstance();
            switch(op.op) {
                case JOIN:
                    std::cout << "Join not implemented" << std::endl;
                    break;
                case FILTER:
                    if(op.type != NONE){
                        op.requests[0].buffer = factory.getOperator(FILTER, op.type, op.filter_func))->operate(op.requests[0].buffer);
                    }
                    else if(op.type == NONE){
                        op.requests[0].buffer = factory.getOperator(FILTER, op.comparator, op.pivot))->operate(op.requests[0].buffer);
                    }
                    else{
                        std::cout << "Error on Filter" << std::endl;
                    }
                    break;
                case GROUP:
                    op.requests[0].buffer = factory.getOperator(GROUP, op.type)->operate(op.requests[0].buffer);
                    break;
                case ORDER:
                    op.requests[0].buffer = factory.getOperator(ORDER, op.order_ascending)->operate(op.requests[0].buffer);
                    break;
                case SUM:
                    if(op.type != NONE){
                        op.requests[0].buffer = factory.getOperator(SUM, op.type)->operate(op.requests[0].buffer);
                    }
                    else{
                        op.requests[0].buffer = factory.getOperator(SUM)->operate(op.requests[0].buffer);
                    }
                    break;
            }
            output_queue_lock.lock();
            output_queue->push(op);
            output_queue_lock.unlock();
        }
        else{
            input_queue_lock.unlock();
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

    std::queue <Operation>  input_queue;
    std::queue <Operation> output_queue;

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

void generateRequests(std::queue <Operation>* iq, int number_requests, int number_streams[2],
        int number_clients, int max_size_request, MetadataManager *MM){

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
        Operation op;
        StreamRequest temp;
        temp.size = 3;//(rand() % max_size_request) + 1;
        temp.stream = 1;//rand() % total_streams;
        temp.client = 0;//rand() % number_clients;
        temp.buffer.reserve(temp.size);
        op.op = SUM;
        op.type = INTEGER;
        op.requests.push_back(temp);
        iq->push(op);
    }
}

void printRequest(std::vector<StreamRequest> requests) {
    for(int i = 0; i < requests.size(); i++){
        std::cout << "Request " << requests[i].client << "-" << requests[i].stream << ": ";
        for (int i = 0; i < requests[i].buffer.size(); ++i) {
            std::cout << mes2str(requests[i].buffer[i]) << " ";
        }
    }
}

std::ifstream& GotoLine(std::ifstream& file, unsigned int num){
    file.seekg(std::ios::beg);
    for(int i=0; i < num - 1; ++i){
        file.ignore(std::numeric_limits<std::streamsize>::max(),'\n');
    }
    return file;
}

void extractRequest(std::vector<StreamResponse> responses, StreamRequest *request){
    uint j= 0;
    for(auto response = responses.begin(); response < responses.end(); response++){
        if(response->type == 0){ //object in memory
            auto* data = (std::vector<Message>*) response->address;
            auto first = data->begin() + response->offset;
            auto last = data->begin() + response->length;
            std::vector<Message> newVec(first, last);
            request->buffer.insert(
                    request->buffer.end(),
                    std::make_move_iterator(newVec.begin()),
                    std::make_move_iterator(newVec.end())
            );
        }
        else if(response->type == 1){//object in drive
            std::ifstream myfile ((char const*)response->address);
            if (myfile.is_open())
            {
                GotoLine(myfile, response->offset);
                for(int i = 0; i<(response->length-response->offset); i++){
                    Message msg;
                    std::string line;
                    std::string type;

                    myfile >> type;

                    if (type == "INTEGER") {
                        msg.type = INTEGER;
                        myfile >> msg.size;
                        int ft;
                        myfile >> ft;
                        msg.data = new int(ft);
                    } else if (type == "FLOAT") {
                        msg.type = FLOAT;
                        myfile >> msg.size;
                        float ft;
                        myfile >> ft;
                        msg.data = new float(ft);
                    } else if (type == "DOUBLE") {
                        msg.type = DOUBLE;
                        myfile >> msg.size;
                        double ft;
                        myfile >> ft;
                        msg.data = new double(ft);
                    } else if (type == "STRING") {
                        msg.type = STRING;
                        myfile >> msg.size;
                        std::string st;
                        myfile >> st;
                        auto char_array = new char[st.size() + 1];
                        strcpy(char_array, st.c_str());
                        msg.data = char_array;

                    } else {
                        std::cout << "Error, no type definition on file line";
                    }

                    request->buffer.push_back(msg);
                }
//                myfile.read(request->buffer + j, response->length - response->offset);
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

std::string mes2str(Message m) {
    std::ostringstream os;

    switch (m.type) {
        case INTEGER:
            os << *(int*) m.data;
            break;
        case FLOAT:
            os << *(float*) m.data;
            break;
        case DOUBLE:
            os << *(double*) m.data;
            break;
        case STRING:
            os << (char*) m.data;
            break;
    }

    return os.str();
}
