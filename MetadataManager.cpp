//
// Created by jaime on 3/10/2019.
//

#include "MetadataManager.h"

MetadataManager* MetadataManager::instance = 0;

MetadataManager* MetadataManager::getInstance()
{
    if (instance == 0)
    {
        instance = new MetadataManager();
    }

    return instance;
}

MetadataManager::MetadataManager(){
}

std::vector<StreamResponse> MetadataManager::getStream(StreamRequest request){
    std::lock_guard<std::mutex> lk(MMMutex);
    StreamRequest copy_request;
    copy_request.size = request.size;
    copy_request.client = request.client;
    copy_request.stream = request.stream;

    std::vector<StreamResponse> response;
    StreamResponse holder;

    StreamPosition stream_position = client_list[request.client].streams[request.stream];
    Partition partition = stream_list[request.stream].partition[stream_position.partition];

    holder.address = partition.address;
    if (partition.length - stream_position.offset > request.size)
        holder.length = stream_position.offset + request.size;
    else
        holder.length = partition.length;
    holder.offset = stream_position.offset;
    holder.type = partition.type;
    response.push_back(holder);
    request.size -= holder.length - holder.offset;

    int i = 1;
    while(request.size > 0){
        partition = stream_list[request.stream].partition[stream_position.partition + i];

        holder.address = partition.address;
        if (partition.length > request.size)
            holder.length = request.size;
        else
            holder.length = partition.length;
        holder.offset = 0;
        holder.type = partition.type;
        response.push_back(holder);
        if(holder.length >= request.size)
            request.size = 0;
        else
            request.size -= holder.length;

        i++;
    }
    updateClient(copy_request);
    return response;
}

void MetadataManager::updateClient(StreamRequest request){

    StreamPosition stream_position = client_list[request.client].streams[request.stream];
    Partition partition = stream_list[request.stream].partition[stream_position.partition];

    while(request.size > 0){
        if(stream_position.offset + request.size < partition.length){
            stream_position.offset += request.size;
            request.size = 0;
        }
        else {
            stream_position.partition += 1;
            stream_position.offset = 0;
            if(partition.length - stream_position.offset > request.size)
                request.size -= partition.length - stream_position.offset;
            else
                request.size = 0;
            partition = stream_list[request.stream].partition[stream_position.partition];
        }
    }
    client_list[request.client].streams[request.stream] = stream_position;
}

void MetadataManager::registerClient(int ClientId){
    Client client0;
    for(auto stream = stream_list.begin(); stream < stream_list.end(); stream++){
        StreamPosition pos;
        pos.partition = 0;
        pos.offset = 0;
        client0.streams.push_back(pos);
    }
    client_list.push_back(client0);
}


void MetadataManager::registerStream(int Streamid, int Stream_location, ExpType exp_type, int len){
    std::vector<Message> partition111;
    std::vector<Message> partition121;
    std::vector<Message> partition131;
    std::vector<Message> partition141;
    std::vector<Message> partition151;

    std::vector<Message> partition211;
    std::vector<Message> partition221;
    std::vector<Message> partition231;
    std::vector<Message> partition241;
    std::vector<Message> partition251;


    char const* partition112 = "/home/jaime/Streams/stream1/partition1";
    char const* partition122 = "/home/jaime/Streams/stream1/partition2";
    char const* partition132 = "/home/jaime/Streams/stream1/partition3";
    char const* partition142 = "/home/jaime/Streams/stream1/partition4";
    char const* partition152 = "/home/jaime/Streams/stream1/partition5";

    char const* partition212 = "/home/jaime/Streams/stream2/partition1";
    char const* partition222 = "/home/jaime/Streams/stream2/partition2";
    char const* partition232 = "/home/jaime/Streams/stream2/partition3";
    char const* partition242 = "/home/jaime/Streams/stream2/partition4";
    char const* partition252 = "/home/jaime/Streams/stream2/partition5";

    if(Stream_location == 0){
        Stream stream0;
        Partition partition;
        if(Streamid == 0) {
            genStreamMemory(&partition111, len, exp_type);
            partition.address = (void *) &partition111;
            partition.length = len;
            partition.type = 0;
            stream0.partition.push_back(partition);

            genStreamMemory(&partition121, len, exp_type);
            partition.address = (void *) &partition121;
            partition.length = len;
            partition.type = 0;
            stream0.partition.push_back(partition);

            genStreamMemory(&partition131, len, exp_type);
            partition.address = (void *) &partition131;
            partition.length = len;
            partition.type = 0;
            stream0.partition.push_back(partition);

            genStreamMemory(&partition141, len, exp_type);
            partition.address = (void *) &partition141;
            partition.length = len;
            partition.type = 0;
            stream0.partition.push_back(partition);

            genStreamMemory(&partition151, len, exp_type);
            partition.address = (void *) &partition151;
            partition.length = len;
            partition.type = 0;
            stream0.partition.push_back(partition);
            stream_list.push_back(stream0);
        }
        else if(Streamid == 1) {
            genStreamMemory(&partition211, len, exp_type);
            partition.address = (void *) &partition211;
            partition.length = len;
            partition.type = 0;
            stream0.partition.push_back(partition);

            genStreamMemory(&partition221, len, exp_type);
            partition.address = (void *) &partition221;
            partition.length = len;
            partition.type = 0;
            stream0.partition.push_back(partition);

            genStreamMemory(&partition231, len, exp_type);
            partition.address = (void *) &partition231;
            partition.length = len;
            partition.type = 0;
            stream0.partition.push_back(partition);

            genStreamMemory(&partition241, len, exp_type);
            partition.address = (void *) &partition241;
            partition.length = len;
            partition.type = 0;
            stream0.partition.push_back(partition);

            genStreamMemory(&partition251, len, exp_type);
            partition.address = (void *) &partition251;
            partition.length = len;
            partition.type = 0;
            stream0.partition.push_back(partition);
            stream_list.push_back(stream0);
        }
    }
    else if(Stream_location == 1){
        Stream stream1;
        Partition partition;
        if(Streamid == 0) {
            genStreamDisk(partition112, len, exp_type);
            partition.address = (void *) partition112;
            partition.length = len;
            partition.type = 1;
            stream1.partition.push_back(partition);

            genStreamDisk(partition122, len, exp_type);
            partition.address = (void *) partition122;
            partition.length = len;
            partition.type = 1;
            stream1.partition.push_back(partition);

            genStreamDisk(partition132, len, exp_type);
            partition.address = (void *) partition132;
            partition.length = len;
            partition.type = 1;
            stream1.partition.push_back(partition);

            genStreamDisk(partition142, len, exp_type);
            partition.address = (void *) partition142;
            partition.length = len;
            partition.type = 1;
            stream1.partition.push_back(partition);

            genStreamDisk(partition152, len, exp_type);
            partition.address = (void *) partition152;
            partition.length = len;
            partition.type = 1;
            stream1.partition.push_back(partition);
            stream_list.push_back(stream1);
        }
        else if(Streamid == 1) {
            genStreamDisk(partition212, len, exp_type);
            partition.address = (void *) partition212;
            partition.length = len;
            partition.type = 1;
            stream1.partition.push_back(partition);

            genStreamDisk(partition222, len, exp_type);
            partition.address = (void *) partition222;
            partition.length = len;
            partition.type = 1;
            stream1.partition.push_back(partition);

            genStreamDisk(partition232, len, exp_type);
            partition.address = (void *) partition232;
            partition.length = len;
            partition.type = 1;
            stream1.partition.push_back(partition);

            genStreamDisk(partition242, len, exp_type);
            partition.address = (void *) partition242;
            partition.length = len;
            partition.type = 1;
            stream1.partition.push_back(partition);

            genStreamDisk(partition252, len, exp_type);
            partition.address = (void *) partition252;
            partition.length = len;
            partition.type = 1;
            stream1.partition.push_back(partition);
            stream_list.push_back(stream1);
        }
    }
}

