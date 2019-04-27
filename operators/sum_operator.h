//
// Created by alvaro on 4/22/19.
//

#ifndef OPERATORS_SUM_OPERATOR_H
#define OPERATORS_SUM_OPERATOR_H

#include <iostream>
#include "abstract_stream_operator.h"

template <typename... Args>
class sum_operator : public abstract_stream_operator<Args...> {
public:
    explicit sum_operator(Args... args) {
        std::cout << "Malformed operator query" << std::endl;
    };
    ~sum_operator() = default;
    std::vector<Message> operate(std::vector<Message> &request) {
        return request;
    };
};

template <>
class sum_operator<DataType> : public abstract_stream_operator<DataType> {
public:
    sum_operator(DataType type) {
        dtype = type;
    };
    std::vector<Message> operate(std::vector<Message> &request) {
        std::vector<Message> resp;
        Message sum = {};
        switch (dtype) {
            case INTEGER:
                sum.type = INTEGER;
                sum.size = sizeof(int);
                sum.data = sumInts(request);
                break;
            case FLOAT:
                sum.type = FLOAT;
                sum.size = sizeof(float);
                sum.data = sumFloats(request);
                break;
            case DOUBLE:
                sum.type = DOUBLE;
                sum.size = sizeof(double);
                sum.data = sumDoubles(request);
                break;
            case NUMERIC:
                sum.type = DOUBLE;
                sum.size = sizeof(double);
                sum.data = sumNumeric(request);
                break;
            default:
                std::cerr << "Incompatible data type" << std::endl;
                return request;
        }
        resp.push_back(sum);
        return resp;
    };

private:
    DataType dtype;

    int* sumInts(std::vector<Message> &req) {
        auto* sum = new int(0);
        for (int i = 0; i < req.size(); ++i) {
            if (req[i].type == INTEGER)
                *sum += *(int*) req[i].data;
        }
        return sum;
    }

    float* sumFloats(std::vector<Message> &req) {
        auto* sum = new float(0);
        for (int i = 0; i < req.size(); ++i) {
            if (req[i].type == FLOAT)
                *sum += *(float*) req[i].data;
        }
        return sum;
    }

    double* sumDoubles(std::vector<Message> &req) {
        auto* sum = new double(0);
        for (int i = 0; i < req.size(); ++i) {
            if (req[i].type == DOUBLE)
                *sum += *(double*) req[i].data;
        }
        return sum;
    }

    double* sumNumeric(std::vector<Message> &req) {
        auto* sum = new double(0);
        for (int i = 0; i < req.size(); ++i) {
            if (req[i].type == INTEGER) {
                *sum += *(int*) req[i].data;
                continue;
            } else if (req[i].type == FLOAT) {
                *sum += *(float*) req[i].data;
                continue;
            } else if (req[i].type == DOUBLE) {
                *sum += *(double*) req[i].data;
            }
        }
        return sum;
    }
};

template <>
class sum_operator<> : public abstract_stream_operator<> {
public:
    std::vector<Message> operate(std::vector<Message> &request) {
        std::vector<Message> resp;
        Message sum = {};
        sum.type = DOUBLE;
        sum.size = sizeof(double);
        sum.data = sumNumeric(request);
        resp.push_back(sum);
        return resp;
    }

    double* sumNumeric(std::vector<Message> &req) {
        auto* sum = new double(0);
        for (int i = 0; i < req.size(); ++i) {
            if (req[i].type == INTEGER) {
                *sum += *(int*) req[i].data;
                continue;
            } else if (req[i].type == FLOAT) {
                *sum += *(float*) req[i].data;
                continue;
            } else if (req[i].type == DOUBLE) {
                *sum += *(double*) req[i].data;
            }
        }
        return sum;
    }
};


#endif //OPERATORS_SUM_OPERATOR_H
