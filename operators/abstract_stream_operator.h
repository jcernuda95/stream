//
// Created by alvaro on 3/11/19.
//

#ifndef HSTREAM_ABSTRACT_STREAM_OPERATOR_H
#define HSTREAM_ABSTRACT_STREAM_OPERATOR_H


#include <cstddef>
#include <vector>

template <typename... Args>
class abstract_stream_operator {
public:
    virtual std::vector<Message> operate(std::vector<Message> &request) = 0;
};


#endif //HSTREAM_ABSTRACT_STREAM_OPERATOR_H