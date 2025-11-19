#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <stdbool.h>
#include <stdio.h>
#include <ezipcv2.h>

#define BUF_SIZE 10
#define NUM_PRODUCERS 3
#define NUM_CONSUMERS 3

struct SharedBuffer {
    char buffer_[BUF_SIZE];
    int in;
    int out;
};

int mutex, empty, full;
struct SharedBuffer *SM;
pid_t forks[NUM_PRODUCERS + NUM_CONSUMERS];

void producer(int num_proc, char input) {
    while (true) {
        P(empty);
        P(mutex);
        (*SM).buffer_[(*SM).in] = input;
        printf("[Producer %d] -> '%c'\n", num_proc, input);
        fflush(stdout);
        (*SM).in = ((*SM).in + 1) % BUF_SIZE;
        V(mutex);
        V(full);
    }
}
void consumer(int num_proc) {
    char buffer_char;
    while (true) {
        P(full);
        P(mutex);
        buffer_char = (*SM).buffer_[(*SM).out];
        printf("[Consumer %d] <- '%c'\n", num_proc, buffer_char);
        fflush(stdout);
        (*SM).out = ((*SM).out + 1) % BUF_SIZE;
        V(mutex);
        V(empty);
    }
}

void termination(int sig) {
    printf("Terminating\n");

    for (int i = 0; i < NUM_PRODUCERS + NUM_CONSUMERS; i++)
    kill(forks[i], SIGKILL);
}

int main() {
    int pid, status;

    SETUP();
    SM = (struct SharedBuffer *)SHARED_MEMORY(sizeof(struct SharedBuffer));
    (*SM).in = 0;
    (*SM).out = 0;

    mutex = SEMAPHORE(SEM_BIN, 1);
    empty = SEMAPHORE(SEM_CNT, BUF_SIZE);
    full = SEMAPHORE(SEM_CNT,0);

    signal(SIGINT, termination);

    for(int i = 0; i < NUM_PRODUCERS; i++) {
    pid = fork();
    if (pid == 0) {
        producer(i, 'A'+ i);
        exit(0);
        } else {
        forks[i] = pid;
    }
}

    for(int i = 0; i < NUM_CONSUMERS; i++) {
    pid = fork();
    if (pid == 0) {
        consumer(i);
        exit(0);
        } else {
        forks[NUM_PRODUCERS + i] = pid;
    }
}

while (wait(&status) != -1);

return 0;
}






