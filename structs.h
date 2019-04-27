#ifndef CONCEPT_STRUCTS_H
#define CONCEPT_STRUCTS_H

#include <cstdint>
#include <vector>
#include <functional>

typedef enum {
    INTEGER,
    FLOAT,
    DOUBLE,
    NUMERIC,
    STRING,
    NONE
} DataType;

typedef enum {
    JOIN,
    FILTER,
    GROUP,
    ORDER,
    SUM
} OperatorType;

typedef struct {
    DataType type;
    size_t size;    // # of bytes of data
    void *data;
} Message;

struct Partition{
    void *address;
    short type;  // In memory == 0 or in disk == 1
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
    void* address;
    short type; // In memory == 0 or in disk == 1
    uint64_t length;
    uint64_t offset;
};

struct StreamRequest{
    uint32_t client;
    uint32_t stream;
    uint64_t size;
    std::vector<Message> buffer;
};


struct additionalParams{
    bool order_ascending = false;
    std::function< bool(T) > filter_func;
    T pivot;
    char comparator[2];
};

struct Operation{
    std::vector<StreamRequest> requests;
    OperatorType op;
    DataType type;


    // SUM either datatype or none
    // Order datatype and optional bool
    // Group dataType
    // Filter std::function< bool(T) >( f ), dataType - {numeric, string}
};

