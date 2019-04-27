//
// Created by alvaro on 3/11/19.
//

#ifndef HSTREAM_ORDER_OPERATOR_H
#define HSTREAM_ORDER_OPERATOR_H


#include <cstring>
#include <algorithm>
#include <iostream>
#include "abstract_stream_operator.h"


template <typename... Args>
class order_operator : public abstract_stream_operator<Args...> {
public:
    explicit order_operator(Args... args) {
        std::cout << "Malformed operator query" << std::endl;
    };
    ~order_operator() = default;
    std::vector<Message> operate(std::vector<Message> &request) {
        return request;
    };
};

template <>
class order_operator<bool> : public abstract_stream_operator<bool> {
public:
    order_operator(bool ascending) { asc = ascending; }
    std::vector<Message> operate(std::vector<Message> &request) {
        switch (request[0].type) {
            case INTEGER:
                std::sort(request.begin(), request.end(), [this] (const Message& req1, const Message& req2) {
                    return this->asc ? *(int*)req1.data < *(int*)req2.data : *(int*)req1.data > *(int*)req2.data;
                });
                break;
            case FLOAT:
                std::sort(request.begin(), request.end(), [this] (const Message& req1, const Message& req2) {
                    return this->asc ? *(float*)req1.data < *(float*)req2.data : *(float*)req1.data > *(float*)req2.data;
                });
                break;
            case DOUBLE:
                std::sort(request.begin(), request.end(), [this] (const Message& req1, const Message& req2) {
                    return this->asc ? *(double*)req1.data < *(double*)req2.data : *(double*)req1.data > *(double*)req2.data;
                });
                break;
            case STRING:
                std::sort(request.begin(), request.end(), [this] (const Message& req1, const Message& req2) {
                    return this->asc ? strcmp((char*)req1.data, (char*)req2.data) : strcmp((char*)req2.data, (char*)req1.data);
                });
                break;
            default:
                std::cerr << "Unknown data type, no ordering done" << std::endl;
        }
        return request;
    };

private:
    bool asc;

};

template <>
class order_operator<> : public abstract_stream_operator<> {
public:
    order_operator() {
        asc = true;
    }
    std::vector<Message> operate(std::vector<Message> &request) {
        switch (request[0].type) {
            case INTEGER:
                std::sort(request.begin(), request.end(), [this] (const Message& req1, const Message& req2) {
                    return this->asc ? *(int*)req1.data < *(int*)req2.data : *(int*)req1.data > *(int*)req2.data;
                });
                break;
            case FLOAT:
                std::sort(request.begin(), request.end(), [this] (const Message& req1, const Message& req2) {
                    return this->asc ? *(float*)req1.data < *(float*)req2.data : *(float*)req1.data > *(float*)req2.data;
                });
                break;
            case DOUBLE:
                std::sort(request.begin(), request.end(), [this] (const Message& req1, const Message& req2) {
                    return this->asc ? *(double*)req1.data < *(double*)req2.data : *(double*)req1.data > *(double*)req2.data;
                });
                break;
            case STRING:
                std::sort(request.begin(), request.end(), [this] (const Message& req1, const Message& req2) {
                    return this->asc ? strcmp((char*)req1.data, (char*)req2.data) : strcmp((char*)req2.data, (char*)req1.data);
                });
                break;
            default:
                std::cerr << "Unknown data type, no ordering done" << std::endl;
        }
        return request;
    };

private:
    bool asc;
};

#endif //HSTREAM_ORDER_OPERATOR_H