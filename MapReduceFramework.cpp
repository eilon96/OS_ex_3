//
// Created by אילון on 26/04/2022.
//

#include <pthread.h>
#include "MapReduceFramework.h"
#include <iostream>
#include <atomic>
#include <algorithm>

#define SYSTEM_ERROR "system error: "

using namespace std;
struct ThreadContext{
    IntermediateVec* intermediateVec;

    int* intermediaryElements;
    pthread_mutex_t* intermediaryElementsMutex;

    OutputVec* outputVec;

    int* outputElements;
    pthread_mutex_t* outputElementsMutex;
};

struct JobContext{
    JobState jobState; // the job state
    pthread_mutex_t jobStateMutex; // a mutex to be used when interested in changing the jobState

    const MapReduceClient* client; // the given client
    const InputVec* inputVec; // the input vector
    OutputVec* outputVec; // the output vector
    vector<IntermediateVec*>* intermediateVec;
    int multiThreadLevel; // the amount of needed thread (maybe useless)
    int fullIntermediaryElements;
    pthread_t* threads; // pointer to an array of all existing threads
    ThreadContext** contexts;

    int map_counter; // a generic count to be used
    pthread_mutex_t mapMutex = PTHREAD_MUTEX_INITIALIZER;

    int intermediaryElements; // a count for the amount of intermediary elements
    pthread_mutex_t intermediaryElementsMutex = PTHREAD_MUTEX_INITIALIZER;

    int outputElements; // a count for the amount of output element
    pthread_mutex_t outputElementsMutex = PTHREAD_MUTEX_INITIALIZER;

    int atomic_barrier; // a counter to use to implement the barrier
    pthread_mutex_t atomic_barrierMutex = PTHREAD_MUTEX_INITIALIZER;

    bool is_waiting;

    atomic<int>* threadsId; // gives an id to each thread
    atomic<int>* fixed_threads;

    pthread_cond_t cvMapSortBarrier = PTHREAD_COND_INITIALIZER;
    pthread_cond_t cvShuffleBarrier = PTHREAD_COND_INITIALIZER;
};

void emit2 (K2* key, V2* value, void* context){

    ThreadContext* threadContext = (ThreadContext*) context;
    IntermediatePair kv2 = IntermediatePair(key, value);
    threadContext->intermediateVec->push_back(kv2);
    if(pthread_mutex_lock((threadContext->intermediaryElementsMutex)) != 0){cerr << SYSTEM_ERROR << "mutex_lock ";exit(1);}
    (*(threadContext->intermediaryElements))++;
    if(pthread_mutex_unlock((threadContext->intermediaryElementsMutex)) != 0){cerr << SYSTEM_ERROR << "mutex_unlock ";exit(1);}

}

void emit3 (K3* key, V3* value, void* context){

    ThreadContext* threadContext = (ThreadContext*) context;
    OutputPair kv3 = OutputPair(key, value);
    if(pthread_mutex_lock((threadContext->outputElementsMutex)) != 0){cerr << SYSTEM_ERROR << "mutex_lock ";exit(1);} ;
    threadContext->outputVec->push_back(kv3);
    (*(threadContext->outputElements))++;
    if(pthread_mutex_unlock((threadContext->outputElementsMutex)) != 0){cerr << SYSTEM_ERROR << "mutex_unlock ";exit(1);} ;

}

/*
 * updates the percentage of the job state
 * @param jobContext
 */
void updatePercentageMap(JobContext* jobContext) {

    if(pthread_mutex_lock(&(jobContext->jobStateMutex)) != 0 ){cerr << SYSTEM_ERROR << "mutex_lock ";exit(1);};
    jobContext->jobState.percentage = (float)(jobContext->map_counter) / (float)jobContext->inputVec->size() * 100;
    if(pthread_mutex_unlock(&(jobContext->jobStateMutex)) != 0 ){cerr << SYSTEM_ERROR << "mutex_unlock ";exit(1);};
}
void updatePercentageShuffle(JobContext *jobContext) {
    // the jobState is shared by all threads which makes changing it a critical code segment
    if(pthread_mutex_lock(&(jobContext->jobStateMutex)) != 0 ){cerr << SYSTEM_ERROR << "mutex_lock ";exit(1);};
    jobContext->jobState.percentage =
            ((float)jobContext->fullIntermediaryElements - (float)(jobContext->intermediaryElements)) /
            (float)jobContext->fullIntermediaryElements * 100;
    if(pthread_mutex_unlock(&(jobContext->jobStateMutex)) != 0 ){cerr << SYSTEM_ERROR << "mutex_unlock ";exit(1);};
}


void updatePercentageReduce(JobContext* jobContext, float numOfElements){
    // the jobState is shared by all threads which makes changing it a critical code segment
    if(pthread_mutex_lock(&(jobContext->jobStateMutex)) != 0 ){cerr << SYSTEM_ERROR << "mutex_lock ";exit(1);};
    jobContext->intermediaryElements += numOfElements;
    jobContext->jobState.percentage = 100 *
                                      (float)(jobContext->intermediaryElements) / (float)jobContext->fullIntermediaryElements;


    if(pthread_mutex_unlock(&(jobContext->jobStateMutex)) != 0 ){cerr << SYSTEM_ERROR << "mutex_unlock ";exit(1);};
}

/**
 * the map phase as it is suppose to be in all different threads (including the main thread)
 * @param arg the jobContext
 * @param context the context of the thread
 * @return Null
 */
void mapPhase(void* arg, void* context){


    JobContext* jc = (JobContext*) arg;
    int oldValue;
    if(pthread_mutex_lock(&(jc->mapMutex)) != 0 ){cerr << SYSTEM_ERROR << "mutex_lock ";exit(1);};
    if((jc->map_counter)  < int(jc->inputVec->size())){oldValue = (jc->map_counter)++;}
    else {
        if(pthread_mutex_unlock(&(jc->mapMutex)) != 0 ){cerr << SYSTEM_ERROR << "mutex_unlock ";exit(1);};
        return;
    }
    if(pthread_mutex_unlock(&(jc->mapMutex)) != 0 ){cerr << SYSTEM_ERROR << "mutex_unlock ";exit(1);};

    while((oldValue < int(jc->inputVec->size()))) {

        InputPair kv = (*(jc->inputVec))[oldValue];
        jc->client->map(kv.first, kv.second, context);

        updatePercentageMap(jc);

        if(pthread_mutex_lock(&(jc->mapMutex)) != 0 ){cerr << SYSTEM_ERROR << "mutex_lock ";exit(1);};
        if((jc->map_counter)  < int(jc->inputVec->size())){oldValue = (jc->map_counter)++;}
        else{
            if(pthread_mutex_unlock(&(jc->mapMutex)) != 0 ){cerr << SYSTEM_ERROR << "mutex_unlock ";exit(1);};
            return;
        }
        if(pthread_mutex_unlock(&(jc->mapMutex)) != 0 ){cerr << SYSTEM_ERROR << "mutex_unlock ";exit(1);};
    }
}

bool comp(IntermediatePair kv1, IntermediatePair kv2){return (kv1.first->operator<(*kv2.first));}

void sortPhase(void* context){
    ThreadContext* tc = (ThreadContext*) context;
    sort(tc->intermediateVec->begin(), tc->intermediateVec->end(), comp);
}


void shufflePhase(void* arg) {

    JobContext *jc = (JobContext *) arg;
    IntermediateVec* current_iv = new IntermediateVec();
    jc->intermediateVec->push_back(current_iv);

    ThreadContext** contextsOfThreads =  jc->contexts;
    bool is_first = true; // first vector to be created

    int firstNotEmptyIndex = 0;
    while(firstNotEmptyIndex < jc->multiThreadLevel
          and contextsOfThreads[firstNotEmptyIndex]->intermediateVec->empty())
    {firstNotEmptyIndex++;} // finds first not empty vector
    if(firstNotEmptyIndex == jc->multiThreadLevel){return;} // if all vectors are already empty


    while (true){

        while(firstNotEmptyIndex < jc->multiThreadLevel
              and contextsOfThreads[firstNotEmptyIndex]->intermediateVec->empty())
        {firstNotEmptyIndex++;} // finds first not empty vector
        if(firstNotEmptyIndex == jc->multiThreadLevel){return;} // if all vectors are already empty
        IntermediatePair max = (contextsOfThreads[firstNotEmptyIndex]->intermediateVec->back()); // sets the max
        int max_index = firstNotEmptyIndex;

        for(int j = firstNotEmptyIndex; j < jc->multiThreadLevel; j++){
            if(!(contextsOfThreads[j]->intermediateVec->empty())){
                IntermediatePair curKv = (contextsOfThreads[j]->intermediateVec->back());

                if(max.first->operator<(*curKv.first)) {
                    max = contextsOfThreads[j]->intermediateVec->back();
                    max_index = j;
                }

            }
        }

        // if it's the first vector
        if(is_first){
            current_iv->push_back(max);
            contextsOfThreads[max_index]->intermediateVec->pop_back();
            jc->intermediaryElements--;
            is_first  = false;
            updatePercentageShuffle(jc);
        }

            // if it's not the first vector
        else{

            // if we should create a new vector
            IntermediatePair prev_max = current_iv->back();

            if(max.first->operator<(*prev_max.first)){

                current_iv = new IntermediateVec();
                jc->intermediateVec->push_back(current_iv);
                current_iv->push_back(max);
                contextsOfThreads[max_index]->intermediateVec->pop_back();
                jc->intermediaryElements--;

            }

                // if we can keep use the current vector
            else{
                current_iv->push_back(max);
                contextsOfThreads[max_index]->intermediateVec->pop_back();
                jc->intermediaryElements--;
            }
        }
        updatePercentageShuffle(jc);
    }
}

void reducePhase(void* arg, void* context){

    JobContext* jc = (JobContext*) arg;

    if(pthread_mutex_lock(&(jc->mapMutex)) != 0 ){cerr << SYSTEM_ERROR << "mutex_lock ";exit(1);};
    int oldValue  = (jc->map_counter)++;
    if(pthread_mutex_unlock(&(jc->mapMutex)) != 0 ){cerr << SYSTEM_ERROR << "mutex_unlock ";exit(1);};

    while(oldValue < int(jc->intermediateVec->size())) {
        jc->client->reduce((*(jc->intermediateVec))[oldValue], context);
        updatePercentageReduce(jc, (*(jc->intermediateVec))[oldValue]->size());

        if(pthread_mutex_lock(&(jc->mapMutex)) != 0 ){cerr << SYSTEM_ERROR << "mutex_lock ";exit(1);};
        oldValue  = (jc->map_counter)++;
        if(pthread_mutex_unlock(&(jc->mapMutex)) != 0 ){cerr << SYSTEM_ERROR << "mutex_unlock ";exit(1);};
    }
}

/*
 * a thread - which is not the main one - this thread should:
 * map - sort - wait for shuffle - than reduce
 * @param arg a pointer to the jobContext
 * @return
 */
void* mapSortReduceThread(void* arg){

    JobContext* jc = (JobContext*) arg;
    int id = ++(*(jc->threadsId));
    ThreadContext* threadContext = new ThreadContext();
    jc->contexts[id] = threadContext;
    IntermediateVec* intermediateVec = new IntermediateVec();

    threadContext->intermediateVec = intermediateVec;
    threadContext->outputVec = jc->outputVec;
    threadContext->intermediaryElements = &(jc->intermediaryElements);
    threadContext->outputElements = &(jc->outputElements);
    threadContext->intermediaryElementsMutex = &(jc->intermediaryElementsMutex);
    threadContext->outputElementsMutex = &(jc->outputElementsMutex);

    // the map phase
    mapPhase(arg, threadContext);
    sortPhase(threadContext);

    if(pthread_mutex_lock(&(jc->atomic_barrierMutex)) != 0){cerr << SYSTEM_ERROR << "mutex_lock ";exit(1);};
    if(++(jc->atomic_barrier) == jc->multiThreadLevel) // indicates sort phase of this thread is over
    {
        // declares all threads finished the sort phase
        if (pthread_cond_broadcast(&(jc->cvMapSortBarrier)) != 0) {
            cerr << SYSTEM_ERROR << "pthread_cond_broadcast MapSort";
            exit(1);
        }
    }

    if(pthread_cond_wait(&(jc->cvShuffleBarrier), &(jc->atomic_barrierMutex)) != 0){
        cerr << SYSTEM_ERROR << "pthread_cond_wait shuffle";
        exit(1);
    }
    if(pthread_mutex_unlock(&(jc->atomic_barrierMutex)) != 0){cerr << SYSTEM_ERROR << "mutex_unlock ";exit(1);};
    reducePhase(arg, threadContext);
    (*(jc->fixed_threads))++;
    return NULL;
}


/*
 * init the job context of the current job
 * @param client
 * @param inputVec
 * @param outputVec
 * @param multiThreadLevel
 * @param jobContext
 */
void initJobContext(const MapReduceClient& client,
                    const InputVec& inputVec, OutputVec& outputVec,
                    int multiThreadLevel, JobContext* jobContext){

    jobContext->multiThreadLevel = multiThreadLevel;
    jobContext->client = &client;
    jobContext->inputVec = &inputVec;
    jobContext->outputVec = &outputVec;
    jobContext->jobStateMutex = PTHREAD_MUTEX_INITIALIZER;
    jobContext->threads  = new pthread_t[multiThreadLevel];
    jobContext->contexts = new ThreadContext*[multiThreadLevel];
    jobContext->intermediateVec = new vector<IntermediateVec*>;
    jobContext->is_waiting = false;

    jobContext->map_counter = 0;
    jobContext->atomic_barrier = 0;
    jobContext->intermediaryElements =0;
    jobContext->outputElements = 0;
    jobContext->threadsId = new atomic<int>(0);
    jobContext->fixed_threads = new atomic<int>(0);

}

void* MainThread(void* arg){

    JobContext* jc = (JobContext*) arg;
    ThreadContext* mainThread = new ThreadContext();
    jc->contexts[0] = mainThread;
    IntermediateVec* intermediateVec = new IntermediateVec();

    mainThread->intermediateVec = intermediateVec;
    mainThread->outputVec = jc->outputVec;
    mainThread->intermediaryElements = &(jc->intermediaryElements);
    mainThread->outputElements = &(jc->outputElements);
    mainThread->intermediaryElementsMutex = &(jc->intermediaryElementsMutex);
    mainThread->outputElementsMutex = &(jc->outputElementsMutex);

    if(pthread_mutex_lock(&(jc->jobStateMutex)) != 0){cerr << SYSTEM_ERROR << "mutex_lock ";exit(1);};
    jc->jobState.stage = MAP_STAGE;
    jc->jobState.percentage = 0;
    if(pthread_mutex_unlock(&(jc->jobStateMutex)) != 0){cerr << SYSTEM_ERROR << "mutex_unlock ";exit(1);};


    for (int i = 1; i < jc->multiThreadLevel; ++i) {
        if(pthread_create(&(jc->threads[i]), NULL, mapSortReduceThread, jc) !=  0){
            cerr << SYSTEM_ERROR << "pthread_create";
            exit(1);
        }
    }

    mapPhase(jc, mainThread);
    sortPhase(mainThread);

    if(pthread_mutex_lock(&(jc->atomic_barrierMutex)) != 0){cerr << SYSTEM_ERROR << "mutex_lock ";exit(1);};
    if(++(jc->atomic_barrier) < jc->multiThreadLevel)
    {
        if(pthread_cond_wait(&(jc->cvMapSortBarrier), &(jc->atomic_barrierMutex)) != 0) {
            cerr << SYSTEM_ERROR << "pthread_cond_wait mapSortBarrier main thread";
            exit(1);
        }

    }
    if(pthread_mutex_unlock(&(jc->atomic_barrierMutex)) != 0){cerr << SYSTEM_ERROR << "mutex_unlock ";exit(1);};

    if(pthread_mutex_lock(&(jc->intermediaryElementsMutex)) != 0){cerr << SYSTEM_ERROR << "mutex_lock ";exit(1);};
    jc->fullIntermediaryElements = jc->intermediaryElements;
    if(pthread_mutex_unlock(&(jc->intermediaryElementsMutex)) != 0){cerr << SYSTEM_ERROR << "mutex_unlock ";exit(1);};

    jc->map_counter = 0;

    if(pthread_mutex_lock(&(jc->jobStateMutex)) != 0){cerr << SYSTEM_ERROR << "mutex_lock ";exit(1);};
    jc->jobState.stage = SHUFFLE_STAGE;
    jc->jobState.percentage = 0;
    if(pthread_mutex_unlock(&(jc->jobStateMutex)) != 0){cerr << SYSTEM_ERROR << "mutex_unlock ";exit(1);};
    shufflePhase(jc);

    if(pthread_mutex_lock(&(jc->jobStateMutex)) != 0){cerr << SYSTEM_ERROR << "mutex_lock ";exit(1);};
    jc->jobState.stage = REDUCE_STAGE;
    jc->jobState.percentage = 0;
    if(pthread_mutex_unlock(&(jc->jobStateMutex)) != 0){cerr << SYSTEM_ERROR << "mutex_unlock ";exit(1);};

    if (pthread_cond_broadcast(&(jc->cvShuffleBarrier)) != 0) {
        cerr << SYSTEM_ERROR << "pthread_cond_broadcast ShuffleBarrier main thread";
        exit(1);
    }
    reducePhase(jc, mainThread);

    for(int i = 1; i < jc->multiThreadLevel; i++) {
        if (pthread_join((jc->threads[i]), NULL) != 0) {
            cerr << SYSTEM_ERROR << "pthread_join waitForJob ";
            exit(1);
        }
    }
    return NULL;
}


JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){

    auto* jobContext = new JobContext();
    initJobContext(client, inputVec, outputVec, multiThreadLevel, jobContext);


    if(pthread_create(jobContext->threads, NULL,
                      MainThread, jobContext) != 0) {
        cerr << SYSTEM_ERROR << "pthread_create";
        exit(1);
    }

    return (JobHandle)(jobContext);


}

void getJobState(JobHandle job, JobState* state){
    auto jc = (JobContext*) job;
    if(pthread_mutex_lock(&jc->jobStateMutex) != 0){cerr << SYSTEM_ERROR << "mutex_lock ";exit(1);};
    state->stage = jc->jobState.stage;
    state->percentage = jc->jobState.percentage;
    if(pthread_mutex_unlock(&jc->jobStateMutex) != 0){cerr << SYSTEM_ERROR << "mutex_unlock ";exit(1);};
}

void waitForJob(JobHandle job) {
    auto jc = (JobContext *) job;
    if (!jc->is_waiting) {
        jc->is_waiting = true;
        if (pthread_join((jc->threads[0]), NULL) != 0 ) {
            cerr << SYSTEM_ERROR << "pthread_join waitForJob ";
            exit(1);

        }

    }
}

void closeJobHandle(JobHandle job){
    waitForJob(job);

    auto jc = (JobContext*) job;
    for(auto & it : *jc->intermediateVec){
        delete it;
    }
    delete jc->intermediateVec;

    // realises memory from all threads:
    for(int i = 0;i < jc->multiThreadLevel; i++) {
        delete jc->contexts[i]->intermediateVec;
        delete jc->contexts[i];
    }

    pthread_mutex_destroy(&jc->jobStateMutex);
    pthread_mutex_destroy(&jc->atomic_barrierMutex);
    pthread_mutex_destroy(&jc->mapMutex);
    pthread_mutex_destroy(&jc->intermediaryElementsMutex);
    pthread_mutex_destroy(&jc->outputElementsMutex);
    pthread_cond_destroy(&jc->cvShuffleBarrier);
    pthread_cond_destroy(&jc->cvMapSortBarrier);

    delete jc->threadsId;
    delete jc->fixed_threads;
    delete [] jc->threads;
    delete [] jc->contexts;
    delete jc;


}