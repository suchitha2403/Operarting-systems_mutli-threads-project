#include <iostream>
#include <stdlib.h>
#include <fstream>
#include <sstream>
#include <chrono>
#include <algorithm>
#include <pthread.h>
#include <vector>
#include <queue>
#include <mutex>
#include <unistd.h>
#include <condition_variable>
#include <cmath>
#include <ctime>
#include <string>
#include <queue>
#include <set>
#define action_nodes 100 // change to 100
using namespace std;

#define num_nodes 37700 

typedef struct
{
    int user_id;
    int action_id;
    string action_type;
    time_t timestamp;
    
} Action;

// Global variables
struct Comparator
{
    bool operator()(pair<int, Action> const &p1, pair<int, Action> const &p2)
    {
        return p1.first <= p2.first;
    }
};

typedef struct
{
    int node_id;
    queue<Action> Wallqueue;
    priority_queue<pair<int, Action>, vector<pair<int, Action>>, Comparator> Feedqueue;
    int choice; // priority - 0 and chronological - 1
    vector<int> adj;
    int degree;
    pthread_mutex_t Feedmutex;
    pthread_cond_t cond_feed;
    int counterlike;
    int counterpost;
    int countercomment;

} Node;

vector<Node> Nodes(num_nodes + 1);
vector<vector<int>> dp(num_nodes, vector<int>(num_nodes, -1));
queue<Action> shared_queue;
pthread_mutex_t shared_mutex;
pthread_cond_t cond_shared;

queue<int> shared_queue_2;
pthread_mutex_t shared_mutex_2;
pthread_cond_t cond_shared_2;

// Functions

void *userSimulator(void *arg);
void *pushUpdate(void *arg);
void *readPost(void *arg);

string filename = "graph.csv";
FILE *logFile = fopen("sns.log", "a");

int main(int argc, char *argv[])
{
    srand(time(NULL));
    // Load graph from file
    ifstream infile(filename);
    if (!infile)
    {
        cerr << "Failed to open file: " << filename << endl;
        exit(1);
    }
    string line;
    getline(infile, line);
    for (int i = 0; i < num_nodes; i++)
    {
        Nodes[i].node_id = i;
        Nodes[i].degree = 0;
        pthread_mutex_init(&Nodes[i].Feedmutex, NULL);
        pthread_cond_init(&Nodes[i].cond_feed, NULL);
        Nodes[i].choice = rand() % 2;
        Nodes[i].counterpost = 0;
        Nodes[i].countercomment = 0;
        Nodes[i].counterlike = 0;
        printf("Node %d - choice %d\n", Nodes[i].node_id, Nodes[i].choice);
        fprintf(logFile, "Node %d - choice %d\n", Nodes[i].node_id, Nodes[i].choice);
    }

    pthread_mutex_init(&shared_mutex, NULL);
    pthread_mutex_init(&shared_mutex_2, NULL);
    pthread_cond_init(&cond_shared, NULL);
    pthread_cond_init(&cond_shared_2, NULL);

    while (getline(infile, line))
    {
        stringstream ss(line);
        string id1_str, id2_str;
        if (getline(ss, id1_str, ',') && getline(ss, id2_str, ','))
        {
            int id1 = stoi(id1_str);
            int id2 = stoi(id2_str);

            Nodes[id1].adj.push_back(id2);
            Nodes[id2].adj.push_back(id1);
            Nodes[id1].degree++;
            Nodes[id2].degree++;
        }
    }
    infile.close();

    // print the graph
    for (int i = 0; i < num_nodes; i++)
    {
        fprintf(logFile, "%d : ", i);
        for (int j = 0; j < Nodes[i].adj.size(); j++)
        {
            fprintf(logFile, "%d ", Nodes[i].adj[j]);
        }
        fprintf(logFile, "|| %d\n", Nodes[i].degree);
    }

    // Create threads
    vector<pthread_t> user_threads(1);
    vector<pthread_t> push_threads(25);
    vector<pthread_t> read_threads(10);

    for (int i = 0; i < user_threads.size(); i++)
    {
        pthread_create(&user_threads[i], NULL, userSimulator, NULL);
    }
    vector<int> arr;
    for (int i = 0; i < 25; i++)
    {
        arr.push_back(i + 1);
    }

    for (int i = 0; i < push_threads.size(); i++)
    {

        pthread_create(&push_threads[i], NULL, pushUpdate, &arr[i]);
    }

    for (int i = 0; i < read_threads.size(); i++)
    {

        pthread_create(&read_threads[i], NULL, readPost, &arr[i]);
    }

    for (int i = 0; i < user_threads.size(); i++)
    {
        pthread_join(user_threads[i], NULL);
    }

    for (int i = 0; i < push_threads.size(); i++)
    {
        pthread_join(push_threads[i], NULL);
    }

    for (int i = 0; i < read_threads.size(); i++)
    {
        pthread_join(read_threads[i], NULL);
    }
    for (int i = 0; i < num_nodes; i++)
    {
        pthread_mutex_destroy(&Nodes[i].Feedmutex);
        pthread_cond_destroy(&Nodes[i].cond_feed);
    }
    pthread_mutex_destroy(&shared_mutex);
    pthread_mutex_destroy(&shared_mutex_2);
    pthread_cond_destroy(&cond_shared);
    pthread_cond_destroy(&cond_shared_2);

    return 0;
}

void *userSimulator(void *arg)
{
    printf("User Simulator thread started.\n");
    fprintf(logFile, "User Simulator thread started.\n");
    while (true)
    {
        // generated 100 random nodes
        vector<int> nodes(action_nodes);
        set<int> s;
        while (s.size() < action_nodes)
        {
            s.insert(rand() % num_nodes);
        }
        int i = 0;
        cout << "The random nodes generated are ";
        fprintf(logFile, "The random nodes generated are ");
        for (auto it : s)
        {
            nodes[i] = it;
            cout << nodes[i] << " ";
            fprintf(logFile, "%d ", nodes[i]);
            i++;
        }
        fprintf(logFile, "\n");
        cout << endl;
        for (int i = 0; i < action_nodes; i++)
        {
            int deg = Nodes[nodes[i]].degree;
            int numActions = 10 * (log2(deg) + 1);
            printf("No of actions generate by %d is %d and its degree is %d\n", nodes[i], numActions, deg);
            fprintf(logFile, "No of actions generate by %d is %d and its degree is %d\n", nodes[i], numActions, deg);
            for (int j = 0; j < numActions; j++)
            {
                int actionType = rand() % 3;
                string actionTypeName;
                if (actionType == 0)
                {
                    actionTypeName = "post";
                }
                else if (actionType == 1)
                {
                    actionTypeName = "comment";
                }
                else
                {
                    actionTypeName = "like";
                }
                // Create action and add to queue
                Action action;

                action.user_id = nodes[i];
                if (actionType == 0)
                {
                    action.action_id = ++Nodes[nodes[i]].counterpost;
                }
                else if (actionType == 1)
                {
                    action.action_id = ++Nodes[nodes[i]].countercomment;
                }
                else
                {
                    action.action_id = ++Nodes[nodes[i]].counterlike;
                }
                action.action_type = actionTypeName;
                auto now = std::chrono::system_clock::now();                               // get the current time
                auto now_us = std::chrono::time_point_cast<std::chrono::nanoseconds>(now); // convert to nanoseconds
                auto value = now_us.time_since_epoch().count();
                action.timestamp = value;

                Nodes[nodes[i]].Wallqueue.push(action);

                pthread_mutex_lock(&shared_mutex);
                shared_queue.push(action);
                pthread_cond_broadcast(&cond_shared);
                pthread_mutex_unlock(&shared_mutex);
                if (actionType == 0)
                {
                    fprintf(logFile, "User %d generated action %d of type %s at time %ld.\n", nodes[i], Nodes[nodes[i]].counterpost, actionTypeName.c_str(), action.timestamp);
                    printf( "User %d generated action %d of type %s at time %ld.\n", nodes[i], Nodes[nodes[i]].counterpost, actionTypeName.c_str(), action.timestamp);
                    
                }
                else if (actionType == 1)
                {
                    fprintf(logFile, "User %d generated action %d of type %s at time %ld.\n", nodes[i], Nodes[nodes[i]].countercomment, actionTypeName.c_str(), action.timestamp);
                    printf( "User %d generated action %d of type %s at time %ld.\n", nodes[i], Nodes[nodes[i]].countercomment, actionTypeName.c_str(), action.timestamp);
                    
                }

                else
                {
                    fprintf(logFile, "User %d generated action %d of type %s at time %ld.\n", nodes[i], Nodes[nodes[i]].counterlike, actionTypeName.c_str(), action.timestamp);
                    printf( "User %d generated action %d of type %s at time %ld.\n", nodes[i], Nodes[nodes[i]].counterlike, actionTypeName.c_str(), action.timestamp);
                    
                }

            }
        }
        sleep(120); // sleep to 120
    }
    pthread_exit(NULL);
}
void *pushUpdate(void *arg)
{

    int thread_id = *((int *)arg);
    printf("PushUpdate thread %d started\n", thread_id);
    fprintf(logFile, "PushUpdate thread %d started\n", thread_id);
    while (true)
    {
        pthread_mutex_lock(&shared_mutex);
        while (shared_queue.empty())
        {
            pthread_cond_wait(&cond_shared, &shared_mutex);
        }
        Action action = shared_queue.front();
        shared_queue.pop();
        pthread_mutex_unlock(&shared_mutex);
        int deg = Nodes[action.user_id].degree;
        for (int i = 0; i < deg; i++)
        {
            int val = Nodes[action.user_id].adj[i];
            int priority;
            if (Nodes[val].choice == 0)
            {
                if (dp[Nodes[action.user_id].node_id][Nodes[val].node_id] != -1)
                {
                    priority = dp[Nodes[action.user_id].node_id][Nodes[val].node_id];
                }
                else
                {
                    vector<int> intersection;
                    set_intersection(Nodes[val].adj.begin(), Nodes[val].adj.end(),
                                     Nodes[action.user_id].adj.begin(), Nodes[action.user_id].adj.end(),
                                     back_inserter(intersection));
                    priority = intersection.size();
                    dp[Nodes[action.user_id].node_id][Nodes[val].node_id] = priority;
                    dp[Nodes[val].node_id][Nodes[action.user_id].node_id] = priority;
                }
            }
            else
            {
                priority = (-1) * action.timestamp;
            }
            pthread_mutex_lock(&Nodes[val].Feedmutex);

            Nodes[val].Feedqueue.push(make_pair(priority, action));
            pthread_cond_broadcast(&Nodes[val].cond_feed);
            pthread_mutex_unlock(&Nodes[val].Feedmutex);

            printf("pushupdate thread %d pushed the action %s, %d generated by %d to the feed queue of node %d\n", thread_id, action.action_type.c_str(), action.action_id,action.user_id, val);
            fprintf(logFile, "pushupdate thread %d pushed the action %s, %d generated by %d to the feed queue of node %d\n", thread_id, action.action_type.c_str(), action.action_id,action.user_id, val);

            pthread_mutex_lock(&shared_mutex_2);
            shared_queue_2.push(Nodes[val].node_id);
            pthread_cond_broadcast(&cond_shared_2);
            pthread_mutex_unlock(&shared_mutex_2);
        }
    }

    pthread_exit(NULL);
}
void *readPost(void *arg)
{
    int thread = *((int *)arg);
    printf("Size of shared queue 2 -> %lu\n", shared_queue_2.size());
    fprintf(logFile, "Size of shared queue 2 -> %lu\n", shared_queue_2.size());
    printf("Readpost thread %d started\n", thread);
    fprintf(logFile, "Readpost thread %d started\n", thread);
    int x = 2;
    while (true)
    {

        pthread_mutex_lock(&shared_mutex_2);
        while (shared_queue_2.empty())
        {
            pthread_cond_wait(&cond_shared_2, &shared_mutex_2);
        }
        int node = shared_queue_2.front();
        shared_queue_2.pop();
        pthread_mutex_unlock(&shared_mutex_2);

        pthread_mutex_lock(&Nodes[node].Feedmutex);
        while (Nodes[node].Feedqueue.empty())
        {
            pthread_cond_wait(&Nodes[node].cond_feed, &Nodes[node].Feedmutex);
        }
        Action action = Nodes[node].Feedqueue.top().second;
        Nodes[node].Feedqueue.pop();
        pthread_mutex_unlock(&Nodes[node].Feedmutex);
        printf("Readpost thread %d read action number %d of type %s posted by user %d at time %ld from feed queue of User %d\n", thread, action.action_id, action.action_type.c_str(), action.user_id, action.timestamp, node);
        fprintf(logFile, "Readpost thread %d read action number %d of type %s posted by user %d at time %ld from feed queue of User %d\n", thread, action.action_id, action.action_type.c_str(), action.user_id, action.timestamp, node);
        // sleep(1);
    }

    pthread_exit(NULL);
}
