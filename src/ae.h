/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
 *
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __AE_H__
#define __AE_H__

#include <time.h>

#define AE_OK 0
#define AE_ERR -1

#define AE_NONE 0       /* No events registered. */
#define AE_READABLE 1   /* Fire when descriptor is readable. */
#define AE_WRITABLE 2   /* Fire when descriptor is writable. */
#define AE_BARRIER 4    /* With WRITABLE, never fire the event if the
                           READABLE event already fired in the same event
                           loop iteration. Useful when you want to persist
                           things to disk before sending replies, and want
                           to do that in a group fashion. */

#define AE_FILE_EVENTS 1
#define AE_TIME_EVENTS 2
#define AE_ALL_EVENTS (AE_FILE_EVENTS|AE_TIME_EVENTS)
#define AE_DONT_WAIT 4
#define AE_CALL_AFTER_SLEEP 8

#define AE_NOMORE -1
#define AE_DELETED_EVENT_ID -1

/* Macros */
#define AE_NOTUSED(V) ((void) V)

struct aeEventLoop;

/* Types and data structures */
// 回调函数处理文件事件发生
typedef void aeFileProc(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
typedef int aeTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData);
typedef void aeEventFinalizerProc(struct aeEventLoop *eventLoop, void *clientData);
typedef void aeBeforeSleepProc(struct aeEventLoop *eventLoop);

/* File event structure */
// 文件事件
typedef struct aeFileEvent {
    // 文件事件类型 可读 可写 先写后读(一般是先读后写, 一个处理)
    int mask; /* one of AE_(READABLE|WRITABLE|BARRIER) */
    // 读处理回调函数指针
    aeFileProc *rfileProc;
    // 写处理回调函数指针
    aeFileProc *wfileProc;
    // 客户端数据
    void *clientData;
} aeFileEvent;

/* Time event structure */
// 时间事件
typedef struct aeTimeEvent {
    // 时间id 自增
    long long id; /* time event identifier. */
    // 时间事件发生的秒
    long when_sec; /* seconds */
    // 时间事件发生的毫秒
    long when_ms; /* milliseconds */
    // 时间事件处理回调函数 serverCron
    aeTimeProc *timeProc;
    // 时间事件删除处理的回调函数
    aeEventFinalizerProc *finalizerProc;
    // 客户端数据
    void *clientData;
    // 上一个时间事件
    struct aeTimeEvent *prev;
    // 下一个时间事件
    struct aeTimeEvent *next;
} aeTimeEvent;

/* A fired event */
// 就绪的文件事件
typedef struct aeFiredEvent {
    // 就绪的文件对象的fd
    int fd;
    // 就绪的文件类型
    int mask;
} aeFiredEvent;

/* State of an event based program */
// 事件循环结构
// 存储 文件事件 就绪事件 时间事件 和 IO多路复用对象
typedef struct aeEventLoop {
    // 当前注册的最大fd setsize - 1
    int maxfd;   /* highest file descriptor currently registered */
    // 监听的fd的最大数量
    int setsize; /* max number of file descriptors tracked */
    // 下一个时间事件的id
    long long timeEventNextId;
    // 最后一次执行事件的时间
    time_t lastTime;     /* Used to detect system clock skew */
    // 注册的文件事件 是setsize大小的数组 从0开始到maxfd
    aeFileEvent *events; /* Registered events */
    // 就绪的文件事件 是setsize大小的数组
    aeFiredEvent *fired; /* Fired events */
    // 时间事件, 指向双向链表的头
    aeTimeEvent *timeEventHead;
    // 事件处理停止标识
    int stop;
    // 指向具体的io多路复用对象
    void *apidata; /* This is used for polling API specific data */
    // 事件处理开始前的回调函数 处理异步操作 aof刷盘, 客户端应答, 快速过期删除
    aeBeforeSleepProc *beforesleep;
    // 事件处理结束后的回调函数
    aeBeforeSleepProc *aftersleep;
} aeEventLoop;

/* Prototypes */
aeEventLoop *aeCreateEventLoop(int setsize);
void aeDeleteEventLoop(aeEventLoop *eventLoop);
void aeStop(aeEventLoop *eventLoop);
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
        aeFileProc *proc, void *clientData);
void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask);
int aeGetFileEvents(aeEventLoop *eventLoop, int fd);
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
        aeTimeProc *proc, void *clientData,
        aeEventFinalizerProc *finalizerProc);
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id);
int aeProcessEvents(aeEventLoop *eventLoop, int flags);
int aeWait(int fd, int mask, long long milliseconds);
void aeMain(aeEventLoop *eventLoop);
char *aeGetApiName(void);
void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep);
void aeSetAfterSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *aftersleep);
int aeGetSetSize(aeEventLoop *eventLoop);
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize);

#endif
