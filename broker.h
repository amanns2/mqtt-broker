#define LISTENQ 1
#define MAXDATASIZE 100
#define MAXLINE 4096
#define BASEPIPEPATH "/tmp/ep1/"

enum operations {
    CONNECT = 0x10,
    CONNACK = 0x20,
    PUBLISH = 0x30,
    PUBACK = 0x40,
    SUBSCRIBE = 0x80,
    SUBACK = 0x90,
    UNSUBSCRIBE = 0xa0,
    UNSUBACK = 0xb0,
    DISCONNECT = 0xe0,
    PINGREQ = 0xc0,
    PINGRESP = 0xd0
};

typedef struct {
    char *topicPath;
    int connfd;
} TopicArgs;