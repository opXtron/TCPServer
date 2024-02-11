#include <iostream>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>
#include <map>
#include <sstream>
#include <queue>
#include <pthread.h>
#include <sys/socket.h>

using namespace std;

#define BUFFER_SIZE 1024
#define THREAD_POOL_SIZE 20

pthread_t threads[THREAD_POOL_SIZE];

std::map<std::string, std::string> KV_Store;
std::queue<int> clientQueue;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mapMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t condition_var = PTHREAD_COND_INITIALIZER;

void* handleClient(int clientSocket) {
   
        char buffer[BUFFER_SIZE];
        int bytesRead;

        while ((bytesRead = recv(clientSocket, buffer, BUFFER_SIZE - 1, 0)) > 0) {
            buffer[bytesRead] = '\0';
            std::cout << "Received: " << buffer;

            std::string message(buffer);
            std::string response;
            std::istringstream iss(message);
            std::string command;
            std::string key; 
            std::string value; 

            while (true) {
                std::getline(iss, command);
                std::cout << "Processing command: " << command << std::endl;

                if (command.find("READ") == 0) {
                    std::getline(iss, command);
                    key = command;
                    pthread_mutex_lock(&mapMutex);
                    auto it = KV_Store.find(key);
                    if (it != KV_Store.end()) {
                        response = it->second;
                    } else {
                        response = "NULL";
                    }
                    pthread_mutex_unlock(&mapMutex);
                    send(clientSocket, response.c_str(), response.size(), 0);
                    send(clientSocket, "\n", 1, 0);
                } else if (command.find("WRITE") == 0) {
                    std::getline(iss, command);
                    key = command;
                    std::getline(iss, command);
                    value = command.substr(1);

                    pthread_mutex_lock(&mapMutex);
                    KV_Store[key] = value;
                    response = "FIN";
                    pthread_mutex_unlock(&mapMutex);

                    send(clientSocket, response.c_str(), response.size(), 0);
                    send(clientSocket, "\n", 1, 0);
                } else if (command.find("COUNT") == 0) {
                    pthread_mutex_lock(&mapMutex);
                    int count = KV_Store.size();
                    pthread_mutex_unlock(&mapMutex);

                    response = std::to_string(count);
                    send(clientSocket, response.c_str(), response.size(), 0);
                    send(clientSocket, "\n", 1, 0);
                } else if (command.find("DELETE") == 0) {
                    std::getline(iss, command);
                    key = command;

                    pthread_mutex_lock(&mapMutex);
                    size_t erased = KV_Store.erase(key);
                    if (erased > 0)
                        response = "FIN";
                    else
                        response = "NULL";
                    pthread_mutex_unlock(&mapMutex);

                    send(clientSocket, response.c_str(), response.size(), 0);
                    send(clientSocket, "\n", 1, 0);
                } else if (command.find("END") == 0) {
                    close(clientSocket);
                    return nullptr;
                }
            }
       }
       return nullptr;
}


void* thread_function(void* arg)
{
	while (true)
    {

        pthread_mutex_lock(&mutex);
         while (clientQueue.empty())
        {
            pthread_cond_wait(&condition_var, &mutex);
        }
        int clientSocket = clientQueue.front();
        clientQueue.pop();
        pthread_mutex_unlock(&mutex);
        if (clientSocket != -1)
            handleClient(clientSocket);
    }
}


int main(int argc, char **argv) {
    int portno; /* port to listen on */

    /*
     * Check command line arguments
     */
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " <port>" << std::endl;
        exit(1);
    }

    // Server port number taken as command line argument
    portno = atoi(argv[1]);

    int parentfd, clientSocket;
    struct sockaddr_in serveraddr, clientaddr;
    socklen_t clientLen = sizeof(clientaddr);
    
    for (int i = 0; i < THREAD_POOL_SIZE;i++) {
        if (pthread_create(&threads[i], NULL, thread_function, NULL) != 0) {
            perror("ERROR creating thread");
            close(parentfd);
            exit(EXIT_FAILURE);
        }
    }

    parentfd = socket(AF_INET, SOCK_STREAM, 0);
    if (parentfd < 0) {
        perror("ERROR opening socket");
        exit(EXIT_FAILURE);
    }

    memset((char *)&serveraddr, 0, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_addr.s_addr = INADDR_ANY;
    serveraddr.sin_port = htons(portno);


    if (bind(parentfd, (struct sockaddr *)&serveraddr, sizeof(serveraddr)) < 0) {
        perror("ERROR on binding");
        close(parentfd);
        exit(EXIT_FAILURE);
    }
    if (listen(parentfd, 100) < 0) {
        perror("ERROR on listen");
        close(parentfd);
        exit(EXIT_FAILURE);
    }

    std::cout << "Server listening on port " << portno << "..." << std::endl;


    while (true) {
        if ((clientSocket = accept(parentfd, (struct sockaddr *)&clientaddr, &clientLen)) < 0) {
            perror("ERROR on accept");
            continue;
        }

        int *pclient = (int *)malloc(sizeof(int));
        *pclient = clientSocket;
        pthread_mutex_lock(&mutex);
        clientSocket = *pclient;
        free(pclient);
        clientQueue.push(clientSocket);
        pthread_cond_signal(&condition_var);
        pthread_mutex_unlock(&mutex);

        std::cout << "Accepted connection from " << inet_ntoa(clientaddr.sin_addr) << ":" << ntohs(clientaddr.sin_port) << std::endl;
    }

    close(parentfd);

    return 0;
}
