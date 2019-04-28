//
// Created by jaime on 4/27/19.
//

#include "genStreams.h"

void genRandomString(char *s, const int len) {
    static const char alphanum[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    s[len] = 0;
}

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

Message generateString(int len) {
    Message message;
    char* str = new char[len + 1];
    genRandomString(str, len);
    message.type = STRING;
    message.size = len;
    message.data = str;
    return message;
}

void genNumericStream(std::vector<Message> *msgs, int length) {
    for (int i = 0; i < length; ++i) {
        switch (rand() % 3) {
            case 0:
                msgs->push_back(generateInt((rand() % 200) - 100));
                break;
            case 1:
                msgs->push_back(generateFloat(static_cast <float> (rand() - RAND_MAX/2) / static_cast <float> (RAND_MAX/100)));
                break;
            case 2:
                msgs->push_back(generateDouble(static_cast<double > (rand() - RAND_MAX/2) / static_cast <double> (RAND_MAX/100)));
        }
    }
}

void genIntStream(std::vector<Message> *msgs, int length) {
    for (int i = 0; i < length; ++i) {
        msgs->push_back(generateInt((rand() % 200) - 100));
    }
}

void genFloatStream(std::vector<Message> *msgs, int length) {
    for (int i = 0; i < length; ++i) {
        msgs->push_back(generateFloat(static_cast <float> (rand() - RAND_MAX/2) / static_cast <float> (RAND_MAX/100)));
    }
}

void genDoubleStream(std::vector<Message> *msgs, int length) {
    for (int i = 0; i < length; ++i) {
        msgs->push_back(generateDouble(static_cast <double > (rand() - RAND_MAX/2) / static_cast <double > (RAND_MAX/100)));
    }
}

void genStringStream(std::vector<Message> *msgs, int length) {
    for (int i = 0; i < length; ++i) {
        msgs->push_back(generateString((rand() % 6) + 5));
    }
}

void genMixedStream(std::vector<Message> *msgs, int length) {
    for (int i = 0; i < length; ++i) {
        switch (rand() % 4) {
            case 0:
                msgs->push_back(generateInt((rand() % 200) - 100));
                break;
            case 1:
                msgs->push_back(generateFloat(static_cast <float> (rand() - RAND_MAX/2) / static_cast <float> (RAND_MAX/100)));
                break;
            case 2:
                msgs->push_back(generateDouble(static_cast<double > (rand() - RAND_MAX/2) / static_cast <double> (RAND_MAX/100)));
                break;
            case 3:
                msgs->push_back(generateString((rand() % 6) + 5));
        }
    }
}

std::string mes2str(Message m) {
    std::stringstream os;

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

void writeMessages(std::vector<Message> msgs, char const* path){
    std::ofstream myfile (path);
    if (myfile.is_open()) {
        for (int i = 0; i < msgs.size(); i++) {
            if (msgs[i].type == INTEGER) {
                myfile << "INTEGER";
                myfile << " ";
                myfile << msgs[i].size;
                myfile << " ";
                myfile << mes2str(msgs[i]);
                myfile << std::endl;
            } else if (msgs[i].type == FLOAT) {
                myfile << "FLOAT";
                myfile << " ";
                myfile << msgs[i].size;
                myfile << " ";
                myfile << mes2str(msgs[i]);
                myfile << std::endl;
            } else if (msgs[i].type == DOUBLE) {
                myfile << "DOUBLE";
                myfile << " ";
                myfile << msgs[i].size;
                myfile << " ";
                myfile << mes2str(msgs[i]);
                myfile << std::endl;
            } else if (msgs[i].type == STRING) {
                myfile << "STRING";
                myfile << " ";
                myfile << msgs[i].size;
                myfile << " ";
                myfile << mes2str(msgs[i]);
                myfile << std::endl;
            }
        }
    }
    myfile.close();
}

void genStreamMemory(std::vector<Message>* msgs, int length, ExpType exp_type) {
    switch (exp_type) {
        case EXP_NUMERIC:
            genNumericStream(msgs, length);
            break;
        case EXP_MONOTYPE_INT:
            genIntStream(msgs, length);
            break;
        case EXP_MONOTYPE_FLOAT:
            genFloatStream(msgs, length);
            break;
        case EXP_MONOTYPE_DOUBLE:
            genDoubleStream(msgs, length);
            break;
        case EXP_MONOTYPE_STRING:
            genStringStream(msgs, length);
            break;
        case EXP_NONE:
        case EXP_MIXED:
            genMixedStream(msgs, length);
    }
}

void genStreamDisk(char const* path, int length, ExpType exp_type){
    std::vector<Message> msgs;
    switch (exp_type) {
        case EXP_NUMERIC:
            genNumericStream(&msgs, length);
            break;
        case EXP_MONOTYPE_INT:
            genIntStream(&msgs, length);
            break;
        case EXP_MONOTYPE_FLOAT:
            genFloatStream(&msgs, length);
            break;
        case EXP_MONOTYPE_DOUBLE:
            genDoubleStream(&msgs, length);
            break;
        case EXP_MONOTYPE_STRING:
            genStringStream(&msgs, length);
            break;
        case EXP_NONE:
        case EXP_MIXED:
            genMixedStream(&msgs, length);
    }
    writeMessages(msgs, path);
}

void genNumericOp(Operation* op, int selector){
    AdditionalParams ap;
    switch (selector) {
        // GROUP
        case 0:
            op->op = GROUP;
            switch (rand() % 4) {
                case 0:
                    op->type = INTEGER;
                    break;
                case 1:
                    op->type = FLOAT;
                    break;
                case 2:
                    op->type = DOUBLE;
            }
            break;
            // SUM
        case 1:
            op->op = SUM;
            break;
            // FILTER
        case 2:
            op->op = FILTER;
            op->ap = ap;
            if (rand() % 2) {
                op->type = NUMERIC;
                op->ap.filter_func = Function<double>([](double a) { return ((int)round(a) % 2) == 0; });
            } else {
                op->ap.comparator[0] = 'l';
                op->ap.comparator[1] = 'e';
                op->ap.pivot = 0;
            }
            break;
    }
}

void genMixedOp(Operation* op, int selector){
    AdditionalParams ap;
    switch (selector) {
        // GROUP
        case 0:
            op->op = GROUP;
            switch (rand() % 4) {
                case 0:
                    op->type = INTEGER;
                    break;
                case 1:
                    op->type = FLOAT;
                    break;
                case 2:
                    op->type = DOUBLE;
                    break;
                case 3:
                    op->type = STRING;
            }
            break;

            // SUM
        case 1:
            op->op = SUM;
            break;

            // FILTER
        case 2:
            op->op = FILTER;
            op->ap = ap;
            if (rand() % 2) {
                op->type = NUMERIC;
                op->ap.filter_func = Function<double>([](double a) { return ((int) round(a) % 2) == 0; });
            } else {
                op->type = STRING;
                op->ap.str_filter_func = Function<char*>([](char* s) { return strlen(s) > 6; });
            }
            break;
    }
}

void genStringOp(Operation* op, int selector) {
    AdditionalParams ap;
    op->type = STRING;
    switch (selector) {
        // FILTER
        case 0:
            op->op = FILTER;
            op->ap = ap;
            op->ap.str_filter_func = Function<char*>([](char* s) { return strlen(s) > 6; });
            break;

            // ORDER
        case 1:
            op->op = ORDER;
            op->ap.order_ascending = rand() % 2;
    }
}

void genDoubleOp(Operation* op, int selector) {
    AdditionalParams ap;
    op->type = DOUBLE;
    switch (selector) {
        // SUM
        case 0:
            op->op = SUM;
            break;

            // FILTER
        case 1:
            op->op = FILTER;
            op->ap = ap;
            if (rand() % 2) {
                op->ap.filter_func = Function<double>([](double a) { return ((int)round(a) % 2) == 0; });
            } else {
                op->ap.comparator[0] = 'l';
                op->ap.comparator[1] = 'e';
                op->ap.pivot = 0;
            }
            break;
            // ORDER
        case 2:
            op->op = ORDER;
            op->ap.order_ascending = rand() % 2;
    }
}

void genFloatOp(Operation* op, int selector) {
    AdditionalParams ap;
    op->type = FLOAT;
    switch (selector) {
        // SUM
        case 0:
            op->op = SUM;
            break;

            // FILTER
        case 1:
            op->op = FILTER;
            op->ap = ap;
            if (rand() % 2) {
                op->ap.filter_func = Function<double>([](double a) { return ((int)round(a) % 2) == 0; });
            } else {
                op->ap.comparator[0] = 'l';
                op->ap.comparator[1] = 'e';
                op->ap.pivot = 0;
            }
            break;
            // ORDER
        case 2:
            op->op = ORDER;
            op->ap.order_ascending = rand() % 2;
    }
}

void genIntOp(Operation* op, int selector) {
    AdditionalParams ap;
    op->type = INTEGER;
    switch (selector) {
        // SUM
        case 0:
            op->op = SUM;
        break;

        // FILTER
        case 1:
            op->op = FILTER;
        op->ap = ap;
        if (rand() % 2) {
            op->ap.filter_func = Function<double>([](double a) { return ((int)round(a) % 2) == 0; });
        } else {
            op->ap.comparator[0] = 'l';
            op->ap.comparator[1] = 'e';
            op->ap.pivot = 0;
        }
        break;
        // ORDER
        case 2:
            op->op = ORDER;
        op->ap.order_ascending = rand() % 2;
    }
}

void genOperators(std::queue <Operation>* iq, int number_requests, int total_streams,
                  int number_clients, int max_size_request, ExpType exp_type) {

    for (int i = 0; i < number_requests; ++i) {
        Operation op;
        std::vector<StreamRequest> req;
        StreamRequest sr;
        sr.client = rand() % number_clients;
        sr.stream = rand() % total_streams;
        sr.size = (rand() % max_size_request) + 1;
        req.push_back(sr);
        op.requests = req;

        switch (exp_type) {
            case EXP_NONE:
                op.op = NOP;
                break;
            case EXP_MIXED:                     // GROUP, SUM, FILTER
                genMixedOp(&op, rand() % 3);
                break;
            case EXP_NUMERIC:                   // GROUP, SUM, FILTER
                genNumericOp(&op, rand() % 3);
                break;
            case EXP_MONOTYPE_STRING:           // FILTER, ORDER
                genStringOp(&op, rand() % 3);
                break;
            case EXP_MONOTYPE_DOUBLE:           // SUM, FILTER, ORDER
                genDoubleOp(&op, rand() % 3);
                break;
            case EXP_MONOTYPE_FLOAT:            // SUM, FILTER, ORDER
                genDoubleOp(&op, rand() % 3);
                break;
            case EXP_MONOTYPE_INT:              // SUM, FILTER, ORDER
                genDoubleOp(&op, rand() % 3);
                break;
        }
        iq->push(op);
    }

}

void printRequest(std::vector<StreamRequest> requests) {
    for(int i = 0; i < requests.size(); i++){
        std::cout << "Request " << requests[i].client << "-" << requests[i].stream << ": ";
        for (int j = 0; j < requests[i].buffer.size(); ++j) {
            switch (requests[i].buffer[j].type) {
                case INTEGER:
                    std::cout << *(int*)requests[i].buffer[j].data << " ";
                case FLOAT:
                    std::cout << *(float*)requests[i].buffer[j].data << " ";
                case DOUBLE:
                    std::cout << *(double*)requests[i].buffer[j].data << " ";
                    break;
                case STRING:
                    std::cout << (char*)requests[i].buffer[j].data << " ";
                    break;
            }
        }
        std::cout << " " << std::endl;
    }
}
