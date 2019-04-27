//
// Created by alvaro on 3/11/19.
//

#ifndef HSTREAM_STREAM_OPERATOR_FACTORY_H
#define HSTREAM_STREAM_OPERATOR_FACTORY_H

#include <cstddef>
#include <iostream>
#include "operators/abstract_stream_operator.h"
#include "operators/order_operator.h"
#include "operators/filter_operator.h"
#include "operators/join_operator.h"
#include "operators/group_operator.h"
#include "operators/sum_operator.h"


class stream_operator_factory {
public:
    template <typename... Args>
    abstract_stream_operator<Args...>* getOperator(OperatorType type, Args... args) {
        if (type == JOIN)
            return new join_operator<Args...>(std::forward<Args>(args)...);
        else if (type == GROUP)
            return new group_operator<Args...>(std::forward<Args>(args)...);
        else if (type == FILTER)
            return new filter_operator<Args...>(std::forward<Args>(args)...);
        else if (type == ORDER)
            return new order_operator<Args...>(std::forward<Args>(args)...);
        else if (type == SUM)
            return new sum_operator<Args...>(std::forward<Args>(args)...);
        std::cout << "Unrecognized operator type" << std::endl;
        return nullptr;
    };
    static stream_operator_factory& getInstance(){
        static stream_operator_factory instance;
        return instance;
    }
    stream_operator_factory(stream_operator_factory const&) = delete;
    void operator=(stream_operator_factory const&) = delete;

private:
    stream_operator_factory() = default;
};


#endif //HSTREAM_STREAM_OPERATOR_FACTORY_H