//
// Created by alvaro on 3/11/19.
//

#ifndef HSTREAM_FILTER_OPERATOR_H
#define HSTREAM_FILTER_OPERATOR_H

#include <iostream>
#include <functional>
#include <cstring>
#include "abstract_stream_operator.h"

// Lambda function to std::function converter, required for template specialization
template<class T, typename U>
std::function< bool(T) > Function( const U& f )
{
    return std::function< bool(T) >( f );
}

template <typename... Args>
class filter_operator : public abstract_stream_operator<Args...> {
public:
    explicit filter_operator(Args... args){
        std::cout << "Malformed operator query" << std::endl;
    };
    ~filter_operator() = default;
    std::vector<Message> operate(std::vector<Message> &request) {
        return request;
    };
};


// Accepted filter operators: l(<); le(<=); g(>); ge(>=); e(=)
template <typename T>
class filter_operator<char const*, T> : public abstract_stream_operator<char const*, T> {
public:
    filter_operator(const char* comp, T value) {
        int length = strlen(comp);
        val = value;

        switch (comp[0]) {
            case 'l':
                if (length == 2 && comp[1] == 'e')
                    filter = std::less_equal<T>();
                else if (length == 1)
                    filter = std::less<T>();
                break;
            case 'g':
                if (length == 2 && comp[1] == 'e')
                    filter = std::greater_equal<T>();
                else if (length == 1)
                    filter = std::greater<T>();
                break;
            case 'e':
                if (length == 1)
                    filter = std::equal_to<T>();
                break;
            default:
                std::cout << "Unknown filter operator" << std::endl;
        }
    };

    std::vector<Message> operate(std::vector<Message> &request) {
        std::vector<Message> resp;

        if (filter == nullptr)
            return *(new std::vector<Message>());

        for (size_t i = 0; i < request.size(); ++i) {
            switch (request[i].type) {
                case INTEGER:
                    if (filter(*(int*)request[i].data, val))
                        resp.push_back(request[i]);
                    break;
                case FLOAT:
                    if (filter(*(float*)request[i].data, val))
                        resp.push_back(request[i]);
                    break;
                case DOUBLE:
                    if (filter(*(double *)request[i].data, val))
                        resp.push_back(request[i]);
                    break;
            }
        }
        return resp;
    };

private:
    std::function<bool(T, T)> filter;
    T val;
};

template <typename T>
class filter_operator<DataType, std::function<bool(T)>> : public abstract_stream_operator<DataType, std::function<bool(T)>> {
public:
    filter_operator(DataType dataType, std::function<bool(T)> fn) {
        dtype = dataType;
        filter = fn;
    };

    std::vector<Message> operate(std::vector<Message> &request) {
        std::vector<Message> resp;

        if (filter == nullptr)
            return *(new std::vector<Message>());

        for (size_t i = 0; i < request.size(); ++i) {
            switch (request[i].type) {
                case INTEGER:
                    if ((dtype == INTEGER || dtype == NUMERIC) && filter(*(int*)request[i].data))
                        resp.push_back(request[i]);
                    break;
                case FLOAT:
                    if ((dtype == FLOAT || dtype == NUMERIC) && filter(*(float*)request[i].data))
                        resp.push_back(request[i]);
                    break;
                case DOUBLE:
                    if ((dtype == DOUBLE || dtype == NUMERIC) && filter(*(double *)request[i].data))
                        resp.push_back(request[i]);
                    break;
            }
        }
        return resp;
    };

private:
    std::function<bool(T)> filter;
    DataType dtype;
};

template <>
class filter_operator<DataType, std::function<bool(char*)>> : public abstract_stream_operator<DataType, std::function<bool(char*)>> {
public:
    filter_operator(DataType dataType, std::function<bool(char*)> fn) {
        dtype = dataType;
        filter = fn;
    };

    std::vector<Message> operate(std::vector<Message> &request) {
        std::vector<Message> resp;

        if (filter == nullptr)
            return *(new std::vector<Message>());

        for (size_t i = 0; i < request.size(); ++i) {
            if (dtype == STRING && filter((char*) request[i].data))
                resp.push_back(request[i]);
        }
        return resp;
    };

private:
    std::function<bool(char*)> filter;
    DataType dtype;
};

#endif //HSTREAM_FILTER_OPERATOR_H
