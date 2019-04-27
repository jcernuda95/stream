#ifndef CONCEPT_STRUCTS_H
#define CONCEPT_STRUCTS_H

#include <cstdint>
#include <vector>
struct Partition{
    void* address;
    short type;
    uint64_t length;
};

struct Stream{
    std::vector<Partition> partition;
};


struct StreamPosition{
    uint32_t partition;
    uint64_t offset;
};

struct Client{
    std::vector<StreamPosition> streams;
};

struct StreamResponse{
    void* adress;
    short type;
    uint64_t length;
    uint64_t offset;
};

struct StreamRequest{
    uint32_t client;
    uint32_t stream;
    uint64_t size;
    char *buffer;
};
#endif //CONCEPT_STRUCTS_H
