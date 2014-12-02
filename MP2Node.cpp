/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
    this->memberNode = memberNode;
    this->par = par;
    this->emulNet = emulNet;
    this->log = log;
    ht = new HashTable();
    this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
    delete ht;
    delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
    /*
     * Implement this. Parts of it are already implemented
     */
    vector<Node> curMemList;
    bool change = false;
    
    /*
     *  Step 1. Get the current membership list from Membership Protocol / MP1
     */
    curMemList = getMembershipList();
    
    /*
     * Step 2: Construct the ring
     */
    // Sort the list based on the hashCode
    sort(curMemList.begin(), curMemList.end());
    
    
    /*
     * Step 3: Run the stabilization protocol IF REQUIRED
     */
    // Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
#ifdef my_code
    // remove after this
    // if(ring.size() == 0) {
    ring = curMemList;
    stabilizationProtocol();
    return;
    //}
    
    if(ht->isEmpty())
        return;
    
    if(curMemList.size() != ring.size())
        change = true;
    
    if(!change) {
        for(int i = 0; i < curMemList.size(); i++) {
            if (!( ring.at(i).nodeAddress == curMemList.at(i).nodeAddress)) {
                change = true;
                break;
            }
        }
    }
    
    if(change) {
        stabilizationProtocol();
    }
    
#endif
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
    unsigned int i;
    vector<Node> curMemList;
    for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
        Address addressOfThisMember;
        int id = this->memberNode->memberList.at(i).getid();
        short port = this->memberNode->memberList.at(i).getport();
        memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
        memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
        curMemList.emplace_back(Node(addressOfThisMember));
    }
    return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
    std::hash<string> hashFunc;
    size_t ret = hashFunc(key);
    return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
    /*
     * Implement this
     */
#ifdef my_code
    
    vector<Node> vectorList = findNodes(key);
    //successMap[g_transID] = 0;
    //failMap[g_transID] = 0;
    // failMap.insert(std::pair<int,int>(g_transID,0));
    Message *msg = new Message(g_transID, memberNode->addr, CREATE, key, value, TERTIARY);
    int sz = sizeof(*msg);
    
    store[g_transID].successCount = 0;
    store[g_transID].failureCount = 0;
    store[g_transID].key = key;
    store[g_transID].value = value;
    store[g_transID].coordinatorAddress = memberNode->addr;
    store[g_transID].coordinatorMsg = CREATE;
    store[g_transID].isLogged = false;
    store[g_transID].firstLoggedTime = par->getcurrtime();
    
    g_transID++;
    
    /*
     size_t hashKey = hashFunction(key);
     
     int i;
     for(i = 1; i < ring.size(); i++) {
     
     if (hashKey > ring.at(i - 1).getHashCode() && hashKey <= ring.at(i).getHashCode()) {
     break;
     }
     }
     i = i % ring.size();
     
     Message *msg = new Message(g_transID++, memberNode->addr, CREATE, key, value, TERTIARY);
     
     Address * mainNodeAddress = ring.at(i).getAddress();
     Address * nextNodeAddress = ring.at((i + 1) % ring.size()).getAddress();
     Address * nextNextNodeAddress = ring.at((i + 2) % ring.size()).getAddress();
     
     int sz = sizeof(*msg);
     */
    
    emulNet->ENsend(&memberNode->addr, vectorList.at(0).getAddress(),(char *)msg,sz);
    emulNet->ENsend(&memberNode->addr, vectorList.at(1).getAddress(),(char *)msg,sz);
    emulNet->ENsend(&memberNode->addr, vectorList.at(2).getAddress(),(char *)msg,sz);
#endif
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
    /*
     * Implement this
     */
#ifdef my_code
    
    vector<Node> vectorList = findNodes(key);
    
    Message *msg = new Message(g_transID, memberNode->addr, READ, key);
    int sz = sizeof(*msg);
    
    store[g_transID].successCount = 0;
    store[g_transID].failureCount = 0;
    store[g_transID].key = key;
    store[g_transID].value = "";
    store[g_transID].coordinatorAddress = memberNode->addr;
    store[g_transID].coordinatorMsg = READ;
    store[g_transID].isLogged = false;
    store[g_transID].firstLoggedTime = par->getcurrtime();
    
    g_transID++;
    
    emulNet->ENsend(&memberNode->addr, vectorList.at(0).getAddress(),(char *)msg,sz);
    emulNet->ENsend(&memberNode->addr, vectorList.at(1).getAddress(),(char *)msg,sz);
    emulNet->ENsend(&memberNode->addr, vectorList.at(2).getAddress(),(char *)msg,sz);
    
#endif
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
    /*
     * Implement this
     */
#ifdef my_code
    
    vector<Node> vectorList = findNodes(key);
    Message *msg = new Message(g_transID, memberNode->addr, UPDATE, key, value, TERTIARY);
    int sz = sizeof(*msg);
    
    store[g_transID].successCount = 0;
    store[g_transID].failureCount = 0;
    store[g_transID].key = key;
    store[g_transID].value = value;
    store[g_transID].coordinatorAddress = memberNode->addr;
    store[g_transID].coordinatorMsg = UPDATE;
    store[g_transID].isLogged = false;
    store[g_transID].firstLoggedTime = par->getcurrtime();
    
    g_transID++;
    
    emulNet->ENsend(&memberNode->addr, vectorList.at(0).getAddress(),(char *)msg,sz);
    emulNet->ENsend(&memberNode->addr, vectorList.at(1).getAddress(),(char *)msg,sz);
    emulNet->ENsend(&memberNode->addr, vectorList.at(2).getAddress(),(char *)msg,sz);
#endif
    
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
    /*
     * Implement this
     */
#ifdef my_code
    
    vector<Node> vectorList = findNodes(key);
    Message *msg = new Message(g_transID, memberNode->addr, DELETE, key);
    int sz = sizeof(*msg);
    
    store[g_transID].successCount = 0;
    store[g_transID].failureCount = 0;
    store[g_transID].key = key;
    store[g_transID].coordinatorAddress = memberNode->addr;
    store[g_transID].coordinatorMsg = DELETE;
    store[g_transID].isLogged = false;
    store[g_transID].firstLoggedTime = par->getcurrtime();
    
    g_transID++;
    
    emulNet->ENsend(&memberNode->addr, vectorList.at(0).getAddress(),(char *)msg,sz);
    emulNet->ENsend(&memberNode->addr, vectorList.at(1).getAddress(),(char *)msg,sz);
    emulNet->ENsend(&memberNode->addr, vectorList.at(2).getAddress(),(char *)msg,sz);
#endif
    
}

#ifdef my_code
/**
 * FUNCTION NAME: checkreplytable
 *
 */
void MP2Node::checkReplyTable(int transID, bool success) {
    
    if(success) {
        store[transID].successCount++;
        
        if(store[transID].successCount == 2) {
            switch (store[transID].coordinatorMsg) {
                case CREATE:
                    log->logCreateSuccess(&(memberNode->addr), true, transID, store[transID].key, store[transID].value);
                    store[transID].isLogged = true;
                    break;
                case UPDATE:
                    log->logUpdateSuccess(&(memberNode->addr), true, transID, store[transID].key, store[transID].value);
                    store[transID].isLogged = true;
                    break;
                case DELETE:
                    log->logDeleteSuccess(&(memberNode->addr), true, transID, store[transID].key);
                    break;
                default:
                    break;
            }
            
        }
    } else {
        store[transID].failureCount++;
        if(store[transID].failureCount == 2) {
            switch (store[transID].coordinatorMsg) {
                case CREATE:
                    log->logCreateFail(&(memberNode->addr), true, transID, store[transID].key, store[transID].value);
                    store[transID].isLogged = true;
                    break;
                case UPDATE:
                    log->logUpdateFail(&(memberNode->addr), true, transID, store[transID].key, store[transID].value);
                    store[transID].isLogged = true;
                    break;
                case DELETE:
                    log->logDeleteFail(&(memberNode->addr), true, transID, store[transID].key);
                    store[transID].isLogged = true;
                    break;
                default:
                    break;
            }
        }
        
    }
}

void MP2Node::checkReadReplyTable(int transID, string value) {
    
    int readIndex = store[transID].successCount;
    store[transID].readValue[readIndex] = value;
    
    if (readIndex == 1) {
        if(store[transID].readValue[readIndex].compare(store[transID].readValue[readIndex - 1]) == 0) {
            
            if(!store[transID].readValue[readIndex].empty()) {
                log->logReadSuccess(&(memberNode->addr), true, transID, store[transID].key, value);
                store[transID].isLogged = true;
                
            } else {
                log->logReadFail(&(memberNode->addr), true, transID, store[transID].key);
                store[transID].isLogged = true;
            }
            
            // used to keep track whether we already have the answer
            store[transID].failureCount = 1;
        }
    } else if (readIndex == 2 && store[transID].failureCount == 0) {
        if((!store[transID].readValue[readIndex].empty() &&
            store[transID].readValue[readIndex].compare(store[transID].readValue[readIndex - 1]) == 0 ) ||
           (!store[transID].readValue[readIndex].empty() &&
            store[transID].readValue[readIndex].compare(store[transID].readValue[readIndex - 2]) == 0 )||
           (!store[transID].readValue[readIndex - 1].empty() &&
            store[transID].readValue[readIndex - 1].compare(store[transID].readValue[readIndex - 2]) == 0)) {
               log->logReadSuccess(&(memberNode->addr), true, transID, store[transID].key, value);
               store[transID].isLogged = true;
           } else {
               log->logReadFail(&(memberNode->addr), true, transID, store[transID].key);
               store[transID].isLogged = true;
           }
    }
    
    store[transID].successCount = (store[transID].successCount + 1) % 3;
    
}

#endif

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica, int transID, Address coordinatorAddress) {
    /*
     * Implement this
     */
    // Insert key, value, replicaType into the hash table
    
#ifdef my_code
    bool success = ht->create(key, value);
    if (success) {
        log->logCreateSuccess(&memberNode->addr, false, transID, key, value);
    } else {
        log->logCreateFail(&memberNode->addr, false, transID, key, value);
    }
    Message *msg = new Message(transID, memberNode->addr, REPLY, success);
    int sz = sizeof(*msg);
    
    emulNet->ENsend(&memberNode->addr, &coordinatorAddress, (char *)msg, sz);
    
    return success;
#endif
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key, int transID, Address coordinatorAddress) {
    /*
     * Implement this
     */
    // Read key from local hash table and return value
#ifdef my_code
    string value = ht->read(key);
    if(!value.empty()) {
        log->logReadSuccess(&memberNode->addr, false, transID, key, value);
    } else {
        log->logReadFail(&memberNode->addr, false, transID, key);
    }
    
    Message *msg = new Message(transID, memberNode->addr, value);
    int sz = sizeof(*msg);
    
    emulNet->ENsend(&memberNode->addr, &coordinatorAddress, (char *)msg, sz);
    
    return value;
#endif
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica, int transID, Address coordinatorAddress) {
    /*
     * Implement this
     */
    // Update key in local hash table and return true or false
#ifdef my_code
    bool success = ht->update(key, value);
    if (success) {
        log->logUpdateSuccess(&memberNode->addr, false, transID, key, value);
    } else {
        log->logUpdateFail(&memberNode->addr, false, transID, key, value);
    }
    
    Message *msg = new Message(transID, memberNode->addr, REPLY, success);
    int sz = sizeof(*msg);
    
    emulNet->ENsend(&memberNode->addr, &coordinatorAddress, (char *)msg, sz);
    
    return success;
    
#endif
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key, int transID, Address coordinatorAddress) {
    /*
     * Implement this
     */
    // Delete the key from the local hash table
#ifdef my_code
    bool success = ht->deleteKey(key);
    if (success) {
        log->logDeleteSuccess(&memberNode->addr, false, transID, key);
    } else {
        log->logDeleteFail(&memberNode->addr, false, transID, key);
    }
    
    Message *msg = new Message(transID, memberNode->addr, REPLY, success);
    int sz = sizeof(*msg);
    
    emulNet->ENsend(&memberNode->addr, &coordinatorAddress, (char *)msg, sz);
    
    return success;
    
#endif
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
    /*
     * Implement this. Parts of it are already implemented
     */
    char * data;
    int size;
    
    /*
     * Declare your local variables here
     */
    
    // dequeue all messages and handle them
    while ( !memberNode->mp2q.empty() ) {
        /*
         * Pop a message from the queue
         */
        data = (char *)memberNode->mp2q.front().elt;
        size = memberNode->mp2q.front().size;
        memberNode->mp2q.pop();
        
        string message(data, data + size);
#ifdef my_code
        Message *msg = (Message *)data;
        
        switch(msg->type) {
            case CREATE:
                createKeyValue(msg->key, msg->value, TERTIARY, msg->transID, msg->fromAddr);
                break;
            case REPLY:
                checkReplyTable(msg->transID, msg->success);
                break;
            case READ:
                readKey(msg->key, msg->transID, msg->fromAddr);
                break;
            case READREPLY:
                checkReadReplyTable(msg->transID, msg->value);
                break;
            case UPDATE:
                updateKeyValue(msg->key, msg->value, TERTIARY, msg->transID, msg->fromAddr);
                break;
            case DELETE:
                deletekey(msg->key, msg->transID, msg->fromAddr);
                break;
                
            default:
                break;
        }
#endif
        /*
         * Handle the message types here
         */
        
    }
    
    /*
     * This function should also ensure all READ and UPDATE operation
     * get QUORUM replies
     */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
    size_t pos = hashFunction(key);
    vector<Node> addr_vec;
    if (ring.size() >= 3) {
        // if pos <= min || pos > max, the leader is the min
        if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
            addr_vec.emplace_back(ring.at(0));
            addr_vec.emplace_back(ring.at(1));
            addr_vec.emplace_back(ring.at(2));
        }
        else {
            // go through the ring until pos <= node
            for (int i=1; i<ring.size(); i++){
                Node addr = ring.at(i);
                if (pos <= addr.getHashCode()) {
                    addr_vec.emplace_back(addr);
                    addr_vec.emplace_back(ring.at((i+1)%ring.size()));
                    addr_vec.emplace_back(ring.at((i+2)%ring.size()));
                    break;
                }
            }
        }
    }
    return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
        return false;
    }
    else {
        return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
    /*
     * Implement this
     */
    map<int, Store>::iterator it;
    
    for(it = store.begin(); it != store.end() ; it++) {
        Store s = it->second;
        if ( s.isLogged == false && par->getcurrtime() - s.firstLoggedTime > T_FAILCHECK) {
            
            int transID = it->first;
            
            switch (s.coordinatorMsg) {
                case CREATE:
                    log->logCreateFail(&(memberNode->addr), true, transID, store[transID].key, store[transID].value);
                    store[transID].isLogged = true;
                    break;
                    
                case READ:
                    log->logReadFail(&(memberNode->addr), true, transID, store[transID].key);
                    store[transID].isLogged = true;
                    break;
                    
                    
                case UPDATE:
                    log->logUpdateFail(&(memberNode->addr), true, transID, store[transID].key, store[transID].value);
                    store[transID].isLogged = true;
                    break;
                    
                case DELETE:
                    log->logDeleteFail(&(memberNode->addr), true, transID, store[transID].key);
                    store[transID].isLogged = true;
                    break;
                    
                default:
                    break;
                    
            }
        }
        
    }
    
}