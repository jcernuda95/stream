#ifndef STREAM_METADATAMANAGER_H
#define STREAM_METADATAMANAGER_H

#include <vector>
#include "structs.h"
#include <mutex>
#include "genStreams/genStreams.h"

class MetadataManager{
private:
    std::mutex MMMutex;
    std::vector<Client> client_list;
    std::vector<Stream> stream_list;
    static MetadataManager* instance;
    void updateClient(StreamRequest request);
    MetadataManager();
public:
    static MetadataManager* getInstance();

    std::vector<StreamResponse> getStream(StreamRequest Sr);

    void registerClient(int ClientId);
    void registerStream(int Streamid, int Stream_location, ExpType exp_type, int len);

};


#endif //STREAM_METADATAMANAGER_H
