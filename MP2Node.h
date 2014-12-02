/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

#define my_code 1

#ifdef my_code

#define T_FAILCHECK 20
struct Store {
    int successCount;
    int failureCount;
    string key;
    string value;
    Address coordinatorAddress;
    MessageType coordinatorMsg;
    string readValue[3];
    bool isLogged;
    int firstLoggedTime;
};
#endif

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
    // Vector holding the next two neighbors in the ring who have my replicas
    vector<Node> hasMyReplicas;
    // Vector holding the previous two neighbors in the ring whose replicas I have
    vector<Node> haveReplicasOf;
    // Ring
    vector<Node> ring;
    // Hash Table
    HashTable * ht;
    // Member representing this member
    Member *memberNode;
    // Params object
    Params *par;
    // Object of EmulNet
    EmulNet * emulNet;
    // Object of Log
    Log * log;
    
#ifdef my_code
    map<int, Store> store;
    
#endif
    
    
public:
    MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
    Member * getMemberNode() {
        return this->memberNode;
    }
    
    // ring functionalities
    void updateRing();
    vector<Node> getMembershipList();
    size_t hashFunction(string key);
    void findNeighbors();
    
    // client side CRUD APIs
    void clientCreate(string key, string value);
    void clientRead(string key);
    void clientUpdate(string key, string value);
    void clientDelete(string key);
    
    // receive messages from Emulnet
    bool recvLoop();
    static int enqueueWrapper(void *env, char *buff, int size);
    
    // handle messages from receiving queue
    void checkMessages();
    
    // coordinator dispatches messages to corresponding nodes
    void dispatchMessages(Message message);
    
    // find the addresses of nodes that are responsible for a key
    vector<Node> findNodes(string key);
    
    // server
#ifdef my_code
    bool createKeyValue(string key, string value, ReplicaType replica, int transID, Address coordinatorAddress);
    void checkReplyTable(int transID, bool success);
    void checkReadReplyTable(int transID, string value);
    string readKey(string key, int transID, Address coordinatorAddress);
    bool updateKeyValue(string key, string value, ReplicaType replica, int transID, Address coordinatorAddress);
#endif
#ifndef my_code
    bool createKeyValue(string key, string value,  ReplicaType replica);
    string readKey(string key);
    bool updateKeyValue(string key, string value, ReplicaType replica);
#endif
    bool deletekey(string key, int transID, Address coordinatorAddress );
    
    // stabilization protocol - handle multiple failures
    void stabilizationProtocol();
    
    ~MP2Node();
};

#endif /* MP2NODE_H_ */