#define _GNU_SOURCE
#include "broker.h"
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <time.h>
#include <unistd.h>
#include <dirent.h>

/** Para usar o mkfifo() **/
#include <sys/stat.h>
/** Para usar o open e conseguir abrir o pipe **/
#include <fcntl.h>
/** Para usar threads **/
#include <pthread.h>

void sendConnack(int connfd) {
    char connackMessage[4] = {0x20, 0x02, 0x00, 0x00};
    printf("server CONNACK\n");
    write(connfd, connackMessage, 4);
}

void sendSuback(int connfd) {
    char subackMessage[5] = {0x90, 0x03, 0x00, 0x01, 0x00};
    printf("server SUBACK\n");
    write(connfd, subackMessage, 5);
}

void sendPingresp(int connfd) {
    char pingrespMessage[2] = {0xd0, 0x00};
    printf("server PINGRESP\n");
    write(connfd, pingrespMessage, 2);
}

char * retrieveTopicFromPublish(char *recvline) {
    int sizeTopicName = recvline[2] + recvline[3];
    char *topic = malloc(sizeTopicName);
    strncpy(topic, recvline + 4, sizeTopicName);
    topic[sizeTopicName] = 0;
    return topic;
}

char * retrieveTopicFromSubscribe(char *recvline) {
    int sizeTopicName = recvline[4] + recvline[5];
    char *topic = malloc(sizeTopicName);
    
    for (int i = 0, j = 6; i < sizeTopicName; i++, j++) {
        topic[i] = recvline[j];
    }
    topic[sizeTopicName] = 0;
    return topic;
}

char * retrieveMessage(char *recvline) {
    int sizeTopicName = recvline[2] + recvline[3];
    int sizeMessageName = recvline[1] - sizeTopicName - 2;
    char *message = malloc(sizeMessageName + 1);
    
    for (int i = 0, j = sizeTopicName + 4; i < sizeMessageName; i++, j++) {
        message[i] = recvline[j];
    }
    return message;
}

void publishToTopic(char *topicName, char *message) {
    DIR *directoryPointer;
    struct dirent *dir;
    char *topicDir = malloc(strlen(BASEPIPEPATH) + strlen(topicName) + 1);

    strcpy(topicDir, BASEPIPEPATH);
    strcat(topicDir, topicName);

    directoryPointer = opendir(topicDir);
    if (directoryPointer) {
        while ((dir = readdir(directoryPointer)) != NULL) {
            if (dir->d_type != DT_FIFO) continue;

            char *topicPath = malloc(strlen(topicDir) + 9);
            strcpy(topicPath, topicDir);
            strcat(topicPath, "/");
            strcat(topicPath, dir->d_name);
            int fd = open(topicPath, O_WRONLY);
            write(fd, message, strlen(message));
            close(fd);
            free(topicPath);
        }
        closedir(directoryPointer);
    }

    free(topicDir);
    free(topicName);
    free(message);
}

char * createTopic(char *topicName) {
    char *pipeTemplate = malloc(strlen(BASEPIPEPATH) + strlen(topicName) + 8);
    char *pipeDir = malloc(strlen(BASEPIPEPATH) + strlen(topicName) + 2);
    struct stat st = {0};

    if (stat(BASEPIPEPATH, &st) == -1) {
        if (mkdir(BASEPIPEPATH, 0777)) {
            perror("mkdir ep1 :(");
            exit(1);
        }
    }

    strcpy(pipeDir, BASEPIPEPATH);
    strcat(pipeDir, topicName);
    strncat(pipeDir, "/", 1);

    if (stat(pipeDir, &st) == -1) {
        if (mkdir(pipeDir, 0777)) {
            perror("mkdir :(");
            exit(1);
        }
    }

    strcpy(pipeTemplate, pipeDir);
    strncat(pipeTemplate, "XXXXXX", 6);

    char *pipePath = malloc(strlen(pipeTemplate) + 1);
    char *topicPath = malloc(strlen(pipeTemplate) + 1);
    pipePath = mktemp(pipeTemplate);
    strcpy(topicPath, pipePath);
    if (mkfifo(pipePath, 0644)) {
        perror("mkfifo :(\n");
        exit(1);
    }
    free(pipeTemplate);
    free(pipeDir);

    return topicPath;
}

char *makePublishPacket(int messageSize, char *buffer, char *topicPath) {
    char *topicNameSub = malloc(strlen(topicPath) - strlen(BASEPIPEPATH) - 6);
    strncpy(topicNameSub, topicPath + strlen(BASEPIPEPATH), strlen(topicPath) - strlen(BASEPIPEPATH) - 7);
    int packetRemainingSize = 2 + strlen(topicNameSub) + messageSize;
    char *publishPacket = malloc(2 + packetRemainingSize);

    publishPacket[0] = PUBLISH;
    publishPacket[1] = (char)packetRemainingSize;
    publishPacket[2] = (char)strlen(topicNameSub) >> 8;
    publishPacket[3] = (char)strlen(topicNameSub);
    for (int i = 0, j = 4; i < strlen(topicNameSub); i++, j++) {
        publishPacket[j] = topicNameSub[i];
    }
    for (int i = 0, j = 4 + strlen(topicNameSub); i < messageSize; i++, j++) {
        publishPacket[j] = buffer[i];
    }
    return publishPacket;
}

int disconnected = 0;

void *readFromTopic(void *args) {
    TopicArgs *topicArgs = *(TopicArgs **)args;
    char *topicPath = topicArgs->topicPath;
    int connfd = topicArgs->connfd;

    int fd;
    char buffer[MAXLINE + 1];
    int messageSize;
    while ((fd = open(topicPath, O_RDONLY)) && (messageSize = read(fd, buffer, MAXLINE)) > 0 && !disconnected) {
        char *publishPacket = makePublishPacket(messageSize, buffer, topicPath);
        write(connfd, publishPacket, publishPacket[1] + 2);
        close(fd);
    }
    pthread_exit(NULL);
}

void *readFromConnfd(void *connfdP) {
    int connfd = *(int *)connfdP;
    int n;
    char recvline[MAXLINE + 1];
    while(!disconnected && (n = read(connfd, recvline, MAXLINE)) > 0) {
        enum operations currentOperation = (recvline[0] & 0xf0);

        switch (currentOperation) {
            case PINGREQ:
                sendPingresp(connfd);
            break;
            case DISCONNECT:
                disconnected = 1;
            break;
            default:
                printf("Forbidden operation %02x received from client\n", currentOperation);
                exit(1);
        }
    }
    pthread_exit(NULL);
}

void waitForMessages(int connfd, char *topicPath) {
    pthread_t *tids = malloc(sizeof(pthread_t) * 2);
    TopicArgs *args = malloc(sizeof(TopicArgs));
    args->connfd = connfd;
    args->topicPath = topicPath;
    disconnected = 0;

    if (pthread_create(&tids[0], NULL, readFromTopic, &args)) {
        perror("Create readFromTopic thread :(");
        exit(1);
    }
    if (pthread_create(&tids[1], NULL, readFromConnfd, &connfd)) {
        perror("Create readFromConnfd thread :(");
        exit(1);
    }
    if (pthread_join(tids[1], NULL)) {
        perror("join thread readFromConnfd :(");
        exit(1);
    }
    if (pthread_cancel(tids[0])) {
        perror("cancel thread readFromTopic :(");
        exit(1);
    }
    unlink(topicPath);
    free(tids);
    free(args->topicPath);
    free(args);
    printf("client disconnected\n");
}

int main (int argc, char **argv) {
    /* The sockets. One that will be the socket that will listen for connections
     * and the other that will be the specific socket of each connection */
    int listenfd, connfd;
    /* Socket information (address and port) are in this struct */
    struct sockaddr_in servaddr;
    struct sockaddr_in clientaddr;
    /* Return of the fork function to know who the child process is and
     * who is the parent process */
    pid_t childpid;
    /* Store rows received from the client */
    char recvline[MAXLINE + 1];
    /* Store the length of the string read from the client */
    ssize_t n;
    socklen_t clientaddr_size = sizeof(clientaddr);
 
    if (argc != 2) {
        fprintf(stderr,"Use: %s <Port>\n",argv[0]);
        fprintf(stderr,"Will run an echo server on the port <Port> TCP\n");
        exit(1);
    }

    /* Creating a socket. It's like a file descriptor.
     * It is possible to do operations such as read, write and close. In this case the
     * created socket is an IPv4 socket (because of AF_INET), which will
     * use TCP (because of SOCK_STREAM), as MQTT works over
     * TCP, and will be used for a conventional application over the Internet
     * (because of the number 0) */
    if ((listenfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("socket :(\n");
        exit(2);
    }

    /* Now it is necessary to inform the addresses associated with this
     * socket. It is necessary to inform the address / interface and the port,
     * because later on the socket will be waiting for connections on this port
     * and at these addresses. For this you need to fill in the struct
     * servaddr. It is necessary to put the socket type there (In our
     * case AF_INET because it is IPv4), which address/interface will be
     * expected connections (In this case any -- INADDR_ANY) and
     * which port. In this case it will be the port that was passed as
     * shell argument (atoi(argv[1]))
     */
    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family      = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port        = htons(atoi(argv[1]));
    if (bind(listenfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) == -1) {
        perror("bind :(\n");
        exit(3);
    }

    /* As this code is server code, the socket will be a
     * passive socket. For this it is necessary to call the listen function
     * which defines that this is a server socket that will be waiting
     * by connections at the addresses defined in the bind function. */
    if (listen(listenfd, LISTENQ) == -1) {
        perror("listen :(\n");
        exit(4);
    }

    printf("[Server on the air. Waiting for connections on the port %s]\n",argv[1]);
    printf("[To finish, press CTRL+c or run a kill or killall]\n");
   
    /* The server is ultimately an infinite loop waiting for
     * connections and processing of each one individually */
	for (;;) {
        /* The initial socket that was created is the socket that will wait
         * by connecting to the specified port. But there may be
         * several clients connecting to the server. That's why you should
         * use the accept function. This function will remove a connection
         * from the queue of connections that were accepted on the listenfd socket and
         * will create a specific socket for this connection. the descriptor
         * from this new socket is the return of the accept function. */
        if ((connfd = accept(listenfd, (struct sockaddr *)&clientaddr, &clientaddr_size)) == -1) {
            perror("accept :(\n");
            exit(5);
        }
      
        /* Agora o servidor precisa tratar este cliente de forma
         * separada. Para isto é criado um processo filho usando a
         * função fork. O processo vai ser uma cópia deste. Depois da
         * função fork, os dois processos (pai e filho) estarão no mesmo
         * ponto do código, mas cada um terá um PID diferente. Assim é
         * possível diferenciar o que cada processo terá que fazer. O
         * filho tem que processar a requisição do cliente. O pai tem
         * que voltar no loop para continuar aceitando novas conexões.
         * Se o retorno da função fork for zero, é porque está no
         * processo filho. */
        if ( (childpid = fork()) == 0) {
            /**** PROCESSO FILHO ****/
            printf("[Uma conexão aberta]\n");
            /* Já que está no processo filho, não precisa mais do socket
             * listenfd. Só o processo pai precisa deste socket. */
            close(listenfd);
         
            /* Agora pode ler do socket e escrever no socket. Isto tem
             * que ser feito em sincronia com o cliente. Não faz sentido
             * ler sem ter o que ler. Ou seja, neste caso está sendo
             * considerado que o cliente vai enviar algo para o servidor.
             * O servidor vai processar o que tiver sido enviado e vai
             * enviar uma resposta para o cliente (Que precisará estar
             * esperando por esta resposta) 
             */

            /* ========================================================= */
            /* ========================================================= */
            /*                         EP1 INÍCIO                        */
            /* ========================================================= */
            /* ========================================================= */
            while((n = read(connfd, recvline, MAXLINE)) > 0) {
                enum operations currentOperation = (recvline[0] & 0xf0);
                
                switch (currentOperation) {
                case CONNECT:
                    sendConnack(connfd);
                    break;
                case PUBLISH:
                    printf("client PUBLISH\n");
                    char *topicNamePub = retrieveTopicFromPublish(recvline);
                    char *message = retrieveMessage(recvline);
                    publishToTopic(topicNamePub, message);
                    break;
                case SUBSCRIBE:
                    printf("********client SUBSCRIBE\n");
                    char *topicNameSub = retrieveTopicFromSubscribe(recvline);
                    char *topicPath = createTopic(topicNameSub);
                    sendSuback(connfd);
                    waitForMessages(connfd, topicPath);
                    break;
                case DISCONNECT:
                    printf("client DISCONNECT\n");
                    break;
                default:
                    printf("client unknown operation\n");
                    printf("Operation: %02x\n", (recvline[0] & 0xf0));
                }
            }

            /* ========================================================= */
            /* ========================================================= */
            /*                         EP1 FIM                           */
            /* ========================================================= */
            /* ========================================================= */

            /* Após ter feito toda a troca de informação com o cliente,
             * pode finalizar o processo filho */
            printf("[Uma conexão fechada]\n");
            exit(0);
        }
        else
            /**** PARENT PROCESS ****/
            /* If it's the parent, the only thing to do is close the socket
             * connfd (it is the specific client socket that will be handled
             * by the child process) */
            close(connfd);
    }
    exit(0);
}
