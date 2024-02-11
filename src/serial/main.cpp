/* 
 * tcpserver.c - A multithreaded TCP echo server 
 * usage: tcpserver <port>
 * 
 * Testing : 
 * nc localhost <port> < input.txt
 */

#include <iostream>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>
#include <map>
#include <sstream>
using namespace std;

#define BUFFER_SIZE 1024

std::map<std::string, std::string> KV_Store;

void handleClient(int clientSocket) {
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
                auto it = KV_Store.find(key);
                if (it != KV_Store.end()) {
                    response = it->second;
                } else {
                    response = "NULL";
                }
        	send(clientSocket, response.c_str(), response.size(), 0);
        	send(clientSocket, "\n", 1, 0);  
            } else if (command.find("WRITE") == 0) {
                std::getline(iss, command);
                key = command;
                std::getline(iss, command);
                value = command.substr(1);
                KV_Store[key] = value;
                response = "FIN";
                cout << response << " " << response.size() << endl;
                send(clientSocket, response.c_str(), response.size(), 0);
        	send(clientSocket, "\n", 1, 0); 
            } else if (command.find("COUNT") == 0) {
                int count = KV_Store.size();
                response = std::to_string(count);
                send(clientSocket, response.c_str(), response.size(), 0);
        	send(clientSocket, "\n", 1, 0); 
            } else if (command.find("DELETE") == 0) {
                std::getline(iss, command);
                key = command;
                size_t erased = KV_Store.erase(key);
                if (erased > 0)
                    response = "FIN";
                else
                    response = "NULL";
                send(clientSocket, response.c_str(), response.size(), 0);
        	send(clientSocket, "\n", 1, 0); 
            } else if (command.find("END") == 0) {
                close(clientSocket);
                return;
            }
    	}
    
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

    // DONE: Server port number taken as command line argument
    portno = atoi(argv[1]);

    int serverSocket, clientSocket;
    struct sockaddr_in serverAddr, clientAddr;
    socklen_t clientLen = sizeof(clientAddr);

    if ((serverSocket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    memset((char *)&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(portno);

    if (bind(serverSocket, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0) {
        perror("bind");
        close(serverSocket);
        exit(EXIT_FAILURE);
    }

    if (listen(serverSocket, 5) < 0) {
        perror("listen");
        close(serverSocket);
        exit(EXIT_FAILURE);
    }

    std::cout << "Server listening on port " << portno << "..." << std::endl;

    while (true) {
        if ((clientSocket = accept(serverSocket, (struct sockaddr *)&clientAddr, &clientLen)) < 0) {
            perror("accept");
            continue;
        }

        std::cout << "Accepted connection from " << inet_ntoa(clientAddr.sin_addr) << ":" << ntohs(clientAddr.sin_port) << std::endl;

        handleClient(clientSocket);
    }

    close(serverSocket);

    return 0;
}
