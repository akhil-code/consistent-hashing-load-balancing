#include <iostream>
#include <iomanip>
#include <algorithm>
#include <tr1/functional>
#include <string>
#include <sstream>
#include <vector>

using std::tr1::hash;

struct WorkerNode{
    std::string id;
    unsigned pos_id;
};

std::string getString(unsigned i){
    std::stringstream ss;
    ss << i;
    return ss.str();
}

bool compWorkerNode(const WorkerNode *a, const WorkerNode *b){
    return a->pos_id < b->pos_id;
}

class LoadBalancer{
    private:
        // hash function
        hash<std::string> h;
        // maximum number of requests, number of servers
        unsigned max_requests, nodes;
        // array of worker nodes
        std::vector<WorkerNode*> nodeArr;
        void initializeWorkerNodes();
        std::string findNearestNode(unsigned map_pos);
        int findNearestNodeUtil(unsigned low, unsigned high, unsigned map_pos);
    public:
        LoadBalancer(unsigned max_requests, unsigned nodes);
        std::string assignTask(const std::string request_id);
        void printInfo();
        std::string generateRandomRequest();

};

LoadBalancer::LoadBalancer(unsigned max_requests, unsigned nodes){
    this->max_requests = max_requests;
    this->nodes = nodes;
    initializeWorkerNodes();
    std::cout << "Initialized load balancer" << std::endl;
    printInfo();
}

void LoadBalancer::initializeWorkerNodes(){
    for(unsigned i = 0; i<nodes;i++){
        WorkerNode *node = new WorkerNode;
        node->id = getString(i);
        node->pos_id = h(node->id) % max_requests;
        nodeArr.push_back(node);
    }

    std::sort(nodeArr.begin(), nodeArr.end(), compWorkerNode);
}

std::string LoadBalancer::assignTask(const std::string request_id){
    unsigned map_pos = h(request_id) % max_requests;
    return findNearestNode(map_pos);
}

std::string LoadBalancer::findNearestNode(unsigned map_pos){
    // ceil of a number from the given array
    int i = findNearestNodeUtil(0, nodes, map_pos);
    return (i != -1) ? nodeArr[i]->id : nodeArr[0]->id;
}

int LoadBalancer::findNearestNodeUtil(unsigned low, unsigned high, unsigned map_pos){ 
  int mid;
  if(map_pos <= nodeArr[low]->pos_id) 
    return low;
  if(map_pos > nodeArr[high]->pos_id) 
    return -1; 

  mid = (low + high)/2;
  
  if(nodeArr[mid]->pos_id == map_pos) 
    return mid;
  else if(nodeArr[mid]->pos_id < map_pos){ 
    if(mid + 1 <= high && map_pos <= nodeArr[mid+1]->pos_id) 
      return mid + 1; 
    else 
      return findNearestNodeUtil(mid+1, high, map_pos); 
  } 
  else{
    if(mid - 1 >= low && map_pos > nodeArr[mid-1]->pos_id) 
      return mid;
    else     
      return findNearestNodeUtil(low, mid - 1, map_pos); 
  } 
} 

void LoadBalancer::printInfo(){
    std::cout << "----------------------------------------------------------" << std::endl;
    std::cout << "number of worker nodes available: " << nodes << std::endl;
    std::cout << "maximum number of requests served: " << max_requests << std::endl;
    std::cout << std::endl;

    std::cout << "node id" << std::setw(10) << "position" << std::endl;
    for(unsigned i=0;i<nodes;i++){
        std::cout << nodeArr[i]->id << std::setw(10) << nodeArr[i]->pos_id << std::endl;
    }
}

std::string LoadBalancer::generateRandomRequest(){
    unsigned r = rand();
    return getString(r);
}

int main(){
    LoadBalancer balancer(500, 20);

    std::cout << "------------------------------" << std::endl;
    std::cout << "Generating 10 random requests " << std::endl << std::endl;
    for(unsigned i=0;i<10;i++){
        std::string request_id = balancer.generateRandomRequest();
        std::string node_id = balancer.assignTask(request_id);
        std::cout<< request_id << " maps to worker node: " << node_id << std::endl;
    }
    
    return 0;
}
