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


	// if #elements in curMemeList != ring then curMemList changed
	if (curMemList.size() != ring.size())
	{
		change = true;
	}
	else
	{
		//curMemList and so ring are sorted, so we just compare
		//the elements at each position in the arrays
		for(int i=0; i < curMemList.size(); ++i)
		{
			if (curMemList[i].getHashCode() != ring[i].getHashCode())
			{
				change = true;
				break;
			}
		}
	}

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	//if membership list changed then updatethe ring and stabilize
	if (change)
	{
		ring = curMemList;
		stabilizationProtocol();
	}
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
	 int trans_id = get_trans_id();
	 // 1) Constructs the message (CREATE, READ, UPDATE, DELETE, REPLY, READREPLY)
	 Message msg1(trans_id, memberNode->addr, CREATE, key, value, PRIMARY);
	 Message msg2(trans_id, memberNode->addr, CREATE, key, value, SECONDARY);
	 Message msg3(trans_id, memberNode->addr, CREATE, key, value, TERTIARY);

	 //find the replicas of the key
	 vector<Node> nodes = findNodes(key);
	 // I am a coordinator, and I send CREATE message to each replica.
	 // and each replica will handle CREATE message in checkMessages
	 storeCreateTransInfo(key, value);
	 emulNet->ENsend(&memberNode->addr, nodes[0].getAddress(), msg1.toString());
	 emulNet->ENsend(&memberNode->addr, nodes[1].getAddress(), msg2.toString());
	 emulNet->ENsend(&memberNode->addr, nodes[2].getAddress(), msg3.toString());
}

void MP2Node::storeCreateTransInfo(string key, string value)
{
	TRANSACTION_INFO info;
	info.msgType = CREATE;
	info.key = key;
	info.value = value;
	info.isCommited = false;
	info.time = par->getcurrtime();
	info.replyCount = 0;
	info.failureCount = 0;
	info.isFailed = false;

	transInfo[get_trans_id()] = info;
	inc_trans_id();
}

void MP2Node::storeDeleteTransInfo(string key)
{
	TRANSACTION_INFO info;
	info.msgType = DELETE;
	info.key = key;
  info.isCommited = false;
	info.time = par->getcurrtime();
	info.replyCount = 0;
	info.failureCount = 0;
	info.isFailed = false;

	transInfo[get_trans_id()] = info;
	inc_trans_id();
}

void MP2Node::storeReadTransInfo(string key)
{
	TRANSACTION_INFO info;
	info.msgType = READ;
	info.key = key;
  info.isCommited = false;
	info.time = par->getcurrtime();
	info.replyCount = 0;
	info.failureCount = 0;
	info.isFailed = false;

	transInfo[get_trans_id()] = info;
	inc_trans_id();
}

void MP2Node::storeUpdateTransInfo(string key, string value)
{
	TRANSACTION_INFO info;
	info.msgType = UPDATE;
	info.key = key;
	info.value = value;
  info.isCommited = false;
	info.time = par->getcurrtime();
	info.replyCount = 0;
	info.failureCount = 0;
	info.isFailed = false;

	transInfo[get_trans_id()] = info;
	inc_trans_id();
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
	 string msg = Message(get_trans_id(), memberNode->addr, READ, key).toString();
	 vector<Node> nodes = findNodes(key);
	 //cout << memberNode->addr.getAddress() << ": Reading key " << key << endl;
	 for(auto& node: nodes)
	 {
		 //cout << memberNode->addr.getAddress() << ": Sending READ " << key << " to " << node.getAddress()->getAddress() << endl;
		 emulNet->ENsend(&memberNode->addr, node.getAddress(), msg);
	 }
	 //
	 storeReadTransInfo(key);
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
	 vector<Node> nodes = findNodes(key);
	 if(nodes.size() > 0)
	 {
		 for(auto& node: nodes)
		 {
			 string msg = Message(get_trans_id(), memberNode->addr, UPDATE, key, value).toString();
			 emulNet->ENsend(&memberNode->addr, node.getAddress(), msg);
		 }
		 storeUpdateTransInfo(key, value);
	 }
	 else
	 {
		 log->logUpdateFail(&memberNode->addr, true, get_trans_id(), key, value);
	 }
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

	 //I am coordinator

	 string msg = Message(get_trans_id(), memberNode->addr, DELETE, key).toString();
	 vector<Node> nodes = findNodes(key);
	 for (auto& node: nodes)
	 {
		 emulNet->ENsend(&memberNode->addr, node.getAddress(), msg);
	 }

	 storeDeleteTransInfo(key);
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */

	 //I am replica, not coordinator
	 //char *rep[] = {"PRIMARY", "SECONDARY", "TERTIARY"};//PRIMARY, SECONDARY, TERTIARY
	// Insert key, value, replicaType into the hash table
	if (!ht->read(key).empty())
	{
		//cout << memberNode->addr.getAddress() << ": createKeyValue "  << key << "=>" << value << " AS " << rep[replica] << " ALREADY exists! so return" << endl;
		return false;//you may not add already existing key value
	}

	//cout << memberNode->addr.getAddress() << ": createKeyValue: " << key << "=>" << value << " AS " << rep[replica] << " ("  << par->getcurrtime() << ")" << endl;

	bool success = ht->create(key, value);
	//update neighbours
	if (replica == PRIMARY)
	{
		//store two next neighbours
		updateHasMyReplicas(key);
	}
	else
	if (replica == TERTIARY)
	{
		//store two previous neighbours
		updateHaveReplicasOf(key);
	}

	return success;
}


void MP2Node::updateHasMyReplicas(string key)
{
	//cout << "updateHasMyReplicas for " << key << endl;
	vector<Node> nodes = findNodes(key);
	//Vector holding the next two neighbors in the ring who have my replicas
	hasMyReplicas.clear();
	hasMyReplicas.push_back(nodes[1]);
	hasMyReplicas.push_back(nodes[2]);

	//cout << memberNode->addr.getAddress() << " of updateHasMyReplicas: " << nodes[1].getAddress()->getAddress() << ", "<<nodes[2].getAddress()->getAddress() << endl;
}

void MP2Node::updateHaveReplicasOf(string key)
{
	//cout << "updateHaveReplicasOf for " << key << endl;
	vector<Node> nodes = findNodes(key);
	//Vector holding the previous two neighbors in the ring whose replicas I have
	haveReplicasOf.clear();
	haveReplicasOf.push_back(nodes[0]);
	haveReplicasOf.push_back(nodes[1]);

	//cout << memberNode->addr.getAddress() << " of updateHaveReplicasOf: " << nodes[0].getAddress()->getAddress() << ", "<<nodes[1].getAddress()->getAddress() << endl;
}



/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false

	if(ht->read(key).empty())
	{
		return false;
	}

	return ht->update(key, value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	//cout << memberNode->addr.getAddress() << ":  DELETE " << key << " ("  << par->getcurrtime() << ")" << endl;
	return ht->deleteKey(key);

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
	TRANSACTION_INFO info;

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
		Message msg(message);
		//cout << message << endl;
		/*
		 * Handle the message types here
		 */

		if (transInfo[msg.transID].isCommited || transInfo[msg.transID].isFailed)
			continue;


		switch(msg.type)
		{
			 case CREATE: onCreateMessage(msg); //I am replica
			              break;
			 case DELETE: onDeleteMessage(msg); //I am replica
						 			  break;
			 case READ:   //cout << memberNode->addr.getAddress() << ": " << msg.toString() << endl;
			              onReadMessage(msg); //I am replica
										break;
			 case UPDATE:
										onUpdateMessage(msg);
										break;

			 case REPLY:  // I am coordinator
                    info = transInfo.find(msg.transID)->second;
										if (info.msgType == CREATE)
										{
											onCreateReplyMessage(msg);
										}
										else
										if (info.msgType == DELETE)
										{
											onDeleteReplyMessage(msg);
										}
										else
										if (info.msgType == UPDATE)
										{
											onUpdateReplyMessage(msg);
										}

										break;
			  case READREPLY:
										onReadReplyMessage(msg);
							      break;
			  default:
			          cerr << "ERROR: unknown message type." << endl;
			          break;
		}

	}

	removeCommittedTransactions();
	timeouts();
	removeFailedTransactions();


	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

//I am replica
void MP2Node::onCreateMessage(Message& msg)
{
  bool success = createKeyValue(msg.key, msg.value, msg.replica);
	//I notify coordinator
	Message rep_msg(msg.transID, memberNode->addr, REPLY, success);
	emulNet->ENsend(&memberNode->addr, &msg.fromAddr, rep_msg.toString());

	if (success)
	{
		//cout << " SUCCESS" << endl;
		log->logCreateSuccess(&memberNode->addr, false, msg.transID, msg.key, msg.value);
	}
	else
	{
		//cout << " FAILURE" << endl;
		log->logCreateFail(&memberNode->addr, false, msg.transID, msg.key, msg.value);
	}
}


//I am coordinator
void MP2Node::onCreateReplyMessage(Message& msg)
{
	TRANSACTION_INFO info = transInfo.find(msg.transID)->second;
	if (msg.success)
	{
		//if we have already one successful reply
		if (info.replyCount > 0)
		{
			log->logCreateSuccess(&memberNode->addr, true, msg.transID, info.key, info.value);
			//cout << "Success " << endl;
			transInfo[msg.transID].isCommited = true;
		}
		else
		{
			transInfo[msg.transID].replyCount += 1;
		}
	}
	else
	{
		if (transInfo[msg.transID].failureCount > 0)
		{
			//cout << "  !!!! Failure " << endl;
			log->logCreateFail(&memberNode->addr, true, msg.transID, info.key, info.value);
			transInfo[msg.transID].isFailed = true;
		}
		else
		{
			transInfo[msg.transID].failureCount += 1;
		}
	}
}

//I am replica
void MP2Node::onDeleteMessage(Message& msg)
{
	bool success = deletekey(msg.key);

	if (success)
		log->logDeleteSuccess(&memberNode->addr, false, msg.transID, msg.key);
	else
		log->logDeleteFail(&memberNode->addr, false, msg.transID, msg.key);

		//notify coordinator
	string strMsg = Message(msg.transID, memberNode->addr, REPLY, success).toString();
	emulNet->ENsend(&memberNode->addr, &msg.fromAddr, strMsg);
}

//I am coordinator
void MP2Node::onDeleteReplyMessage(Message& msg)
{
	TRANSACTION_INFO info = transInfo.find(msg.transID)->second;

	if (msg.success)
	{
		if (info.replyCount > 0)
		{
			log->logDeleteSuccess(&memberNode->addr, true, msg.transID, info.key);
			transInfo[msg.transID].isCommited = true;
		}
		else
		{
			transInfo[msg.transID].replyCount += 1;
		}
	}
	else //failure
	{
		if (transInfo[msg.transID].failureCount > 0)
		{
			//cout << "  !!!! Failure " << endl;
			log->logDeleteFail(&memberNode->addr, true, msg.transID, info.key);
			transInfo[msg.transID].isFailed = true;
		}
		else
		{
			transInfo[msg.transID].failureCount += 1;
		}
	}
}

// I am replica
void MP2Node::onReadMessage(Message& msg)
{
	string value = readKey(msg.key);
	//cout << memberNode->addr.getAddress() << " received key " << msg.key << " => " << value << endl;
	string strMsg =  Message(msg.transID, memberNode->addr, value).toString();//READREPLY by default
	emulNet->ENsend(&memberNode->addr, &msg.fromAddr, strMsg);

	if(!value.empty())
	{
		log->logReadSuccess(&memberNode->addr, false, msg.transID, msg.key, value);
	}
	else
	{
		log->logReadFail(&memberNode->addr, false, msg.transID, msg.key);
	}
}

// I am coordinator
void MP2Node::onReadReplyMessage(Message& msg)
{
	TRANSACTION_INFO info = transInfo.find(msg.transID)->second;
  //printTransInfo(msg.transID, info);

	if (info.replyCount == 0)
	{
		transInfo[msg.transID].replyCount = 1;
		transInfo[msg.transID].value = msg.value; //store the value to compare with the second replica's value
	}
	else
	if (info.replyCount == 1)
	{
		if (!transInfo[msg.transID].value.empty() && (transInfo[msg.transID].value == msg.value))
		{
			log->logReadSuccess(&memberNode->addr, true, msg.transID, info.key, msg.value);
			transInfo[msg.transID].isCommited = true;
			//cout << "Quorum12" << endl;
		}
		else
		{
			transInfo[msg.transID].value2 = msg.value;//store the second value to compare with the first replica's value
			transInfo[msg.transID].replyCount = 2;
		}
	}
	else
	if (info.replyCount == 2)
	{
		//cout << "replyCount == 2, value: " << msg.value << endl;
		if (!transInfo[msg.transID].value.empty() && (transInfo[msg.transID].value == msg.value))
		{
			log->logReadSuccess(&memberNode->addr, true, msg.transID, info.key, msg.value);
			transInfo[msg.transID].isCommited = true;
			//cout << "Quorum13" << endl;
		}
		else
		if (!transInfo[msg.transID].value2.empty() && (transInfo[msg.transID].value2 == msg.value))
		{
			log->logReadSuccess(&memberNode->addr, true, msg.transID, info.key, msg.value);
			transInfo[msg.transID].isCommited = true;
			//cout << "Quorum23" << endl;
		}
		else
		{
			//
			log->logReadFail(&memberNode->addr, true, msg.transID, msg.key);
			transInfo[msg.transID].isFailed = true;
			//cout << "Failure" << endl;
		}
	}

}

/// I am replica
void MP2Node::onUpdateMessage(Message& msg)
{
	bool success = updateKeyValue(msg.key, msg.value, msg.replica);

	if(success)
		log->logUpdateSuccess(&memberNode->addr, false, get_trans_id(), msg.key, msg.value);
 	else
 		log->logUpdateFail(&memberNode->addr, false, get_trans_id(), msg.key, msg.value);

	string strMsg = Message(get_trans_id(), memberNode->addr, REPLY, success).toString();
	emulNet->ENsend(&memberNode->addr, &msg.fromAddr, strMsg);
}


void MP2Node::onUpdateReplyMessage(Message& msg)
{
	TRANSACTION_INFO info = transInfo.find(msg.transID)->second;

	if(msg.success)
	{
		if (info.replyCount > 0)
		{
			log->logUpdateSuccess(&memberNode->addr, true, get_trans_id(), info.key, info.value);
			transInfo[msg.transID].isCommited = true;
		}
		else
		{
			transInfo[msg.transID].replyCount += 1;
		}
	}
	else
	{
		if (info.failureCount > 0)
		{
			log->logUpdateFail(&memberNode->addr, true, get_trans_id(), info.key, info.value);
			transInfo[msg.transID].isFailed = true;
		}
		else
		{
			transInfo[msg.transID].failureCount += 1;
		}
	}
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
void MP2Node::stabilizationProtocol()
{

	vector<Node> prevHasMyReplicas = hasMyReplicas;
	hasMyReplicas.clear();
	haveReplicasOf.clear();

	for(auto& entry: ht->hashTable)
	{
		vector<Node> nodes = findNodes(entry.first);
		if (*nodes[0].getAddress() == memberNode->addr.getAddress())
		{
			clientCreate(entry.first, entry.second);
		}
		else
		if (!(*nodes[0].getAddress() == memberNode->addr) &&
	      !(*nodes[1].getAddress() == memberNode->addr) &&
				!(*nodes[2].getAddress() == memberNode->addr)
			)
			{
				ht->deleteKey(entry.first);
			}


		if (prevHasMyReplicas.size() > 0 )
		{
			if ((prevHasMyReplicas[0].getAddress() == nodes[0].getAddress()) ||
			    (prevHasMyReplicas[0].getAddress() == nodes[1].getAddress()) ||
					(prevHasMyReplicas[0].getAddress() == nodes[2].getAddress()))
			{
				continue;
			}
			else
			{
				string msg = Message(get_trans_id(), memberNode->addr, DELETE, entry.first).toString();
				emulNet->ENsend(&memberNode->addr, prevHasMyReplicas[0].getAddress(), msg);
			}
		}

		if (prevHasMyReplicas.size() > 1 )
		{
			if ((prevHasMyReplicas[1].getAddress() == nodes[0].getAddress()) ||
			    (prevHasMyReplicas[1].getAddress() == nodes[1].getAddress()) ||
					(prevHasMyReplicas[1].getAddress() == nodes[2].getAddress()))
			{
				continue;
			}
			else
			{
				string msg = Message(get_trans_id(), memberNode->addr, DELETE, entry.first).toString();
				emulNet->ENsend(&memberNode->addr, prevHasMyReplicas[1].getAddress(), msg);
			}
		}

	}

}


void MP2Node::timeouts()
{
	map<long, TRANSACTION_INFO> transInfoTemp;
	for(auto& e: transInfo)
	{
		long trans_id = e.first;
		TRANSACTION_INFO info = e.second;

		if ((par->getcurrtime() - info.time) > TIMEOUT)
		{
			if(info.msgType == CREATE)
			{
				log->logCreateFail(&memberNode->addr, false, trans_id, info.key, info.value);
			}
			else
			if(info.msgType == DELETE)
			{
				log->logDeleteFail(&memberNode->addr, false, trans_id, info.key);
			}
			else
			if(info.msgType == READ)
			{
				log->logReadFail(&memberNode->addr, false, trans_id, info.key);
				//cout << "timeout : READ, " << memberNode->addr.getAddress() << "," << info.key << endl;
			}
			else
			if(info.msgType == UPDATE)
			{
				log->logUpdateFail(&memberNode->addr, false, trans_id, info.key, info.value);
				//cout << "timeout : UPDATE, " << memberNode->addr.getAddress() << "," << info.key << endl;
			}
			else
			if(info.msgType == READREPLY)
			{
				log->logReadFail(&memberNode->addr, true, trans_id, info.key);
				//cout << "timeout : READREPLY" << endl;
			}

			//.....
		}
		else
		{
			transInfoTemp[trans_id] = info;
		}
	}

	transInfo = transInfoTemp;
}


void MP2Node::removeFailedTransactions()
{
	map<long, TRANSACTION_INFO> transInfoTemp;
	for(auto& e: transInfo)
	{
		long trans_id = e.first;
		TRANSACTION_INFO info = e.second;

		if(!info.isFailed)
		{
			transInfoTemp[trans_id] = info;
		}

	}

	transInfo = transInfoTemp;
}

void MP2Node::removeCommittedTransactions()
{
	map<long, TRANSACTION_INFO> transInfoTemp;
	for(auto& e: transInfo)
	{
		long trans_id = e.first;
		TRANSACTION_INFO info = e.second;

		if(info.isCommited)
			continue;

		transInfoTemp[trans_id] = info;


	}

	transInfo = transInfoTemp;
}
