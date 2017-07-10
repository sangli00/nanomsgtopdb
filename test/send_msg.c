#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "nanomsg/nn.h"
#include "nanomsg/pipeline.h"

#define SOCKET_ADDRESS "tcp://127.0.0.1:9999"

int 
main(){
	int socket;
	int ret;
	char buf[64];	
	int optlen = sizeof(int);	
	int timeout = 10000;

	memset(buf,0,sizeof(buf));
	socket = nn_socket(AF_SP, NN_PUSH);	
	nn_setsockopt(socket, NN_SOL_SOCKET, NN_SNDTIMEO, &timeout, sizeof(timeout));
	ret = nn_connect(socket,SOCKET_ADDRESS);

	if(ret <0 ){
		printf("Can't connect nanomsg  %s\n",SOCKET_ADDRESS);
		return 0;
	}else 
		printf("connect to %s\n",SOCKET_ADDRESS);
		
	sprintf(buf,"%s","Hello ,from nanomsg msg.");

	int s = nn_send(socket,buf,strlen(buf),0);
	
	if(s < 0){
		printf("send msg is fault, address is: %s\n",SOCKET_ADDRESS);
		return 0;
	}	
	printf("Send msg size is %d\n",s);

	nn_close(socket);
	
	return 0;
}
