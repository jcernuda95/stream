#ifndef CONCEPT_METADATAMANAGER_H
#define CONCEPT_METADATAMANAGER_H
#include <vector>
#include "structs.h"
#include <mutex>

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
    void registerStream(int Streamid, int Stream_location);

};


#endif //CONCEPT_METADATAMANAGER_H
