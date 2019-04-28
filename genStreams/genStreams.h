//
// Created by jaime on 4/27/19.
//

#ifndef STREAM_GENSTREAMS_H
#define STREAM_GENSTREAMS_H

#include "../structs.h"
#include "../operators/filter_operator.h"
#include <vector>
#include <queue>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <random>
#include <cstring>

typedef enum {
    EXP_NUMERIC,
    EXP_MONOTYPE_INT,
    EXP_MONOTYPE_FLOAT,
    EXP_MONOTYPE_DOUBLE,
    EXP_MONOTYPE_STRING,
    EXP_MIXED,
    EXP_NONE
} ExpType;

void genStreamMemory(std::vector<Message>* msgs, int length, ExpType exp_type);
void genStreamDisk(char const* path, int length, ExpType exp_type);
void genOperators(std::queue <Operation>* iq, int number_requests, int total_streams,
                  int number_clients, int max_size_request, ExpType exp_type);
void printRequest(std::vector<StreamRequest> requests);
#endif //STREAM_GENSTREAMS_H
