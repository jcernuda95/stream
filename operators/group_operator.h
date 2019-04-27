//
// Created by alvaro on 3/11/19.
//

#ifndef HSTREAM_GROUP_OPERATOR_H
#define HSTREAM_GROUP_OPERATOR_H

#include <iostream>
#include "abstract_stream_operator.h"

template <typename... Args>
class group_operator : public abstract_stream_operator<Args...> {
public:
    explicit group_operator(Args... args) {
        std::cout << "Malformed operator query" << std::endl;
    };
    ~group_operator() = default;
    std::vector<Message> operate(std::vector<Message> &request) {
        return request;
    };
};

template <>
class group_operator<DataType> : public abstract_stream_operator<DataType> {
public:
    group_operator(DataType type) {
        filteredType = type;
    };
    std::vector<Message> operate(std::vector<Message> &request) {
        std::vector<Message> resp;

        for (int i = request.size() - 1; i >= 0; --i) {
            if (request[i].type == filteredType)
                resp.push_back(request[i]);
            else if (filteredType == NUMERIC && request[i].type != STRING)
                resp.push_back(request[i]);
        }
        return resp;
    };

private:
    DataType filteredType;
};


#endif //HSTREAM_GROUP_OPERATOR_H