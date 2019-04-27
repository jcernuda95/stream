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

    holder.adress = partition.address;
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

        holder.adress = partition.address;
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

void MetadataManager::registerStream(int Streamid, int Stream_location){
    char const* partition11 = "abcdefghij";
    char const* partition21 = "klmopqrstu";
    char const* partition31 = "vwxyz12345";
    char const* partition12 = "/home/jaime/Streams/stream1/partition1";
    char const* partition22 = "/home/jaime/Streams/stream1/partition2";
    char const* partition32 = "/home/jaime/Streams/stream1/partition3";

    if(Stream_location == 0){
        Stream stream0;
        Partition partition;

        partition.address = (void*)partition11;
        partition.length = 10;
        partition.type = 0;
        stream0.partition.push_back(partition);
        partition.address = (void*)partition21;
        partition.length = 10;
        partition.type = 0;
        stream0.partition.push_back(partition);
        partition.address = (void*)partition31;
        partition.length = 10;
        partition.type = 0;
        stream0.partition.push_back(partition);
        stream_list.push_back(stream0);
    }
    else if(Stream_location == 1){
        Stream stream1;
        Partition partition;

        partition.address = (void*)partition12;
        partition.length = 10;
        partition.type = 1;
        stream1.partition.push_back(partition);
        partition.address = (void*)partition22;
        partition.length = 10;
        partition.type = 1;
        stream1.partition.push_back(partition);
        partition.address = (void*)partition32;
        partition.length = 10;
        partition.type = 1;
        stream1.partition.push_back(partition);
        stream_list.push_back(stream1);
    }
}

