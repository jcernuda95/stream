//
// Created by alvaro on 4/26/19.
//

#include <cstdlib>
#include <sstream>
#include "operators/abstract_stream_operator.h"
#include "stream_operator_factory.h"


Message generateInt(int a) {
    Message message;
    message.type = INTEGER;
    message.size = sizeof(int);
    message.data = new int(a);
    return message;
}

Message generateFloat(float a) {
    Message message;
    message.type = FLOAT;
    message.size = sizeof(float);
    message.data = new float(a);
    return message;
}

Message generateDouble(double a) {
    Message message;
    message.type = DOUBLE;
    message.size = sizeof(double);
    message.data = new double(a);
    return message;
}

std::string mes2str(Message m) {
    std::ostringstream os;

    switch (m.type) {
        case INTEGER:
            os << *(int*) m.data;
            break;
        case FLOAT:
            os << *(float*) m.data;
            break;
        case DOUBLE:
            os << *(double*) m.data;
            break;
        case STRING:
            os << (char*) m.data;
            break;
    }

    return os.str();
}

int main() {

    std::vector<Message> req;
    std::vector<Message> resp1;
    std::vector<Message> resp2;
    std::vector<Message> resp3;
    std::vector<Message> resp4;

    for (int i = 0; i < 10; ++i)
    {
        switch (rand() % 3) {
            case 0:
                req.push_back(generateInt((rand() % 200) - 100));
                break;
            case 1:
                req.push_back(generateDouble(static_cast <float> (rand() - RAND_MAX/2) / static_cast <float> (RAND_MAX/100)));
                break;
            case 2:
                req.push_back(generateDouble(static_cast<double > (rand() - RAND_MAX/2) / static_cast <double> (RAND_MAX/100)));
                break;
        }
    }

    stream_operator_factory& factory = stream_operator_factory::getInstance();

    resp1 = stream_operator_factory::getInstance().getOperator(SUM, NUMERIC)->operate(req);

    // resp2 = factory.getOperator(ORDER)->operate(req);      // Only works when all the messages are of the same type

    resp3 = factory.getOperator(GROUP, INTEGER)->operate(req);

    resp4 = factory.getOperator(FILTER, INTEGER, Function<int>([](int a) { return (a % 2) == 0;}))->operate(req);

    std::cout << "ORIGINAL:\n[ ";

    for (int i = 0; i < req.size(); ++i) {
        std::cout << mes2str(req[i]) << " ";
    }
    std::cout << "]" << std::endl;

    std::cout << "SUM:\n" << mes2str(resp1[0]) << std::endl;

    std::cout << "GROUP INT:\n[ ";

    for (int i = 0; i < resp3.size(); ++i) {
        std::cout << mes2str(resp3[i]) << " ";
    }
    std::cout << "]" << std::endl;

    std::cout << "FILTER:\n[ ";

    for (int i = 0; i < resp4.size(); ++i) {
        std::cout << mes2str(resp4[i]) << " ";
    }
    std::cout << "]" << std::endl;

}
