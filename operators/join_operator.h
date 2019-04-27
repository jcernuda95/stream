//
// Created by alvaro on 3/11/19.
//

#ifndef HSTREAM_JOIN_OPERATOR_H
#define HSTREAM_JOIN_OPERATOR_H

#include <iostream>

#include "abstract_stream_operator.h"

template <typename... Args>
class join_operator : public abstract_stream_operator<Args...> {
public:
    explicit join_operator(Args... args) {
        std::cout << "Malformed operator query" << std::endl;
    };
    ~join_operator() = default;
    std::vector<Message> operate(std::vector<Message> &request) {
        return request;
    };
};

template <>
class join_operator<int, bool*> : public abstract_stream_operator<int, bool*> {
public:
    std::vector<Message> operate(std::vector<Message> &request) {

    };
};


#endif //HSTREAM_JOIN_OPERATOR_H
