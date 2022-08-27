/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
 *
 * Copyright (c) 2006-2010, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <time.h>
#include <errno.h>

#include "ae.h"
#include "zmalloc.h"
#include "config.h"

/*
 * 根据操作系统来include具体的.c实现IO多路复用 是在编译阶段完成的 是按照实现性能的从高到低的顺序(evport>epoll>kqueue)
 */
/* Include the best multiplexing layer supported by this system.
 * The following should be ordered by performances, descending. */
#ifdef HAVE_EVPORT
#include "ae_evport.c"
#else
    #ifdef HAVE_EPOLL
    #include "ae_epoll.c"
    #else
        #ifdef HAVE_KQUEUE
        #include "ae_kqueue.c"
        #else
        #include "ae_select.c"
        #endif
    #endif
#endif

/**
 * 创建事件循环
 * setsize: 可以处理的fd数量, 最大支持的client数量(10000)+128
 */
aeEventLoop *aeCreateEventLoop(int setsize) {
    aeEventLoop *eventLoop;
    int i;

    // 创建eventloop 申请内存
    if ((eventLoop = zmalloc(sizeof(*eventLoop))) == NULL) goto err;
    // events数组申请内存
    eventLoop->events = zmalloc(sizeof(aeFileEvent)*setsize);
    // fired数组申请内存
    eventLoop->fired = zmalloc(sizeof(aeFiredEvent)*setsize);
    if (eventLoop->events == NULL || eventLoop->fired == NULL) goto err;
    // 初始化属性
    eventLoop->setsize = setsize;
    eventLoop->lastTime = time(NULL);
    eventLoop->timeEventHead = NULL;
    eventLoop->timeEventNextId = 0;
    eventLoop->stop = 0; // 启动
    eventLoop->maxfd = -1;
    eventLoop->beforesleep = NULL;
    eventLoop->aftersleep = NULL;
    // aeApiCreate调用的是具体的xxx.c文件的apApiCreate(linux下的ae_epoll.c)
    if (aeApiCreate(eventLoop) == -1) goto err;
    /* Events with mask == AE_NONE are not set. So let's initialize the
     * vector with it. */
    // 循环赋值
    for (i = 0; i < setsize; i++)
        // 每个文件事件类型是无事件
        eventLoop->events[i].mask = AE_NONE;
    return eventLoop;

err:
    if (eventLoop) {
        zfree(eventLoop->events);
        zfree(eventLoop->fired);
        zfree(eventLoop);
    }
    return NULL;
}

/* Return the current set size. */
int aeGetSetSize(aeEventLoop *eventLoop) {
    return eventLoop->setsize;
}

/* Resize the maximum set size of the event loop.
 * If the requested set size is smaller than the current set size, but
 * there is already a file descriptor in use that is >= the requested
 * set size minus one, AE_ERR is returned and the operation is not
 * performed at all.
 *
 * Otherwise AE_OK is returned and the operation is successful. */
// 重新申请内存
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize) {
    int i;

    // 要设置的size等于setsize
    if (setsize == eventLoop->setsize) return AE_OK;
    // 最大的fd大于等于要设置的size
    if (eventLoop->maxfd >= setsize) return AE_ERR;
    // IO多路复用resize
    if (aeApiResize(eventLoop,setsize) == -1) return AE_ERR;

    // events数组重新申请内存
    eventLoop->events = zrealloc(eventLoop->events,sizeof(aeFileEvent)*setsize);
    // fired数组重新申请内存
    eventLoop->fired = zrealloc(eventLoop->fired,sizeof(aeFiredEvent)*setsize);
    eventLoop->setsize = setsize;

    /* Make sure that if we created new slots, they are initialized with
     * an AE_NONE mask. */
    // 循环依次将新生成的event的事件类型设置为无事件
    for (i = eventLoop->maxfd+1; i < setsize; i++)
        eventLoop->events[i].mask = AE_NONE;
    return AE_OK;
}

/**
 * 删除事件循环
 * @param eventLoop
 */
void aeDeleteEventLoop(aeEventLoop *eventLoop) {
    // IO多路复用删除
    aeApiFree(eventLoop);
    // 释放events数组
    zfree(eventLoop->events);
    // 释放fired数组
    zfree(eventLoop->fired);
    // 释放eventLoop
    zfree(eventLoop);
}

/**
 * 停止事件循环
 * @param eventLoop
 */
void aeStop(aeEventLoop *eventLoop) {
    eventLoop->stop = 1;
}

// 创建文件事件
// eventloop 指向事件循环
// fd: 指定fd
// mask: 指定事件类型
// proc: 指向事件处理回调函数
// clientData: 客户端数据
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
        aeFileProc *proc, void *clientData)
{
    // fd 大于fd的最大数量
    if (fd >= eventLoop->setsize) {
        errno = ERANGE;
        // 返回错误
        return AE_ERR;
    }
    // 获取文件事件索引, 在文件事件数组中索引到文件事件(标识fd)
    aeFileEvent *fe = &eventLoop->events[fd];

    // io多路复用 添加要监听的fd
    if (aeApiAddEvent(eventLoop, fd, mask) == -1)
        return AE_ERR;
    // 文件事件的事件类型为mask
    fe->mask |= mask;
    // 可读 设置事件的读回调函数 acceptTcpHandler, readQueryFromClient
    if (mask & AE_READABLE) fe->rfileProc = proc;
    // 可写 设置事件的写回调函数 wfileProc:sendReplyToClient
    if (mask & AE_WRITABLE) fe->wfileProc = proc;
    // 设置客户端数据
    fe->clientData = clientData;
    // 当前fd大于最大fd
    if (fd > eventLoop->maxfd)
        // 将当前fd设置为最大fd
        eventLoop->maxfd = fd;
    return AE_OK;
}

/**
 * 删除fd上的事件
 * @param eventLoop 事件循环
 * @param fd 指定的fd
 * @param mask 事件类型
 */
void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask)
{
    // fd大于最大数量 则返回
    if (fd >= eventLoop->setsize) return;
    // 获得fd对应的文件
    aeFileEvent *fe = &eventLoop->events[fd];
    // 文件事件类型是无事件 则返回
    if (fe->mask == AE_NONE) return;

    /* We want to always remove AE_BARRIER if set when AE_WRITABLE
     * is removed. */
    if (mask & AE_WRITABLE) mask |= AE_BARRIER;

    // IO多路复用删除fd下的指定事件
    aeApiDelEvent(eventLoop, fd, mask);
    fe->mask = fe->mask & (~mask);
    // fd是最大fd并且无事件监听
    if (fd == eventLoop->maxfd && fe->mask == AE_NONE) {
        /* Update the max fd */
        int j;

        for (j = eventLoop->maxfd-1; j >= 0; j--)
            if (eventLoop->events[j].mask != AE_NONE) break;
        eventLoop->maxfd = j;
    }
}

int aeGetFileEvents(aeEventLoop *eventLoop, int fd) {
    if (fd >= eventLoop->setsize) return 0;
    aeFileEvent *fe = &eventLoop->events[fd];

    return fe->mask;
}

static void aeGetTime(long *seconds, long *milliseconds)
{
    struct timeval tv;

    gettimeofday(&tv, NULL);
    *seconds = tv.tv_sec;
    *milliseconds = tv.tv_usec/1000;
}

/**
 * 设置处理事件的时间
 * @param milliseconds 时间
 * @param sec 返回秒
 * @param ms  返回毫秒
 */
static void aeAddMillisecondsToNow(long long milliseconds, long *sec, long *ms) {
    long cur_sec, cur_ms, when_sec, when_ms;

    // 获得当前的秒和毫秒
    aeGetTime(&cur_sec, &cur_ms);
    // 当前的秒+时间间隔(s)
    when_sec = cur_sec + milliseconds/1000;
    // 当前的毫秒+时间间隔(ms)
    when_ms = cur_ms + milliseconds%1000;
    // 毫秒>1000
    if (when_ms >= 1000) {
        // 秒+1
        when_sec ++;
        // 毫秒-1000
        when_ms -= 1000;
    }
    // 返回执行时间
    *sec = when_sec;
    *ms = when_ms;
}

/**
 * 创建时间事件
 * @param eventLoop
 * @param milliseconds 时间间隔
 * @param proc 时间事件回调函数 serverCron
 * @param clientData 客户端数据
 * @param finalizerProc 删除事件时回调函数
 * @return
 */
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
        aeTimeProc *proc, void *clientData,
        aeEventFinalizerProc *finalizerProc)
{
    // 时间事件下一个id+1
    long long id = eventLoop->timeEventNextId++;
    aeTimeEvent *te;

    // 申请内存
    te = zmalloc(sizeof(*te));
    if (te == NULL) return AE_ERR;
    // 设置id
    te->id = id;
    // 设置处理事件的时间
    aeAddMillisecondsToNow(milliseconds,&te->when_sec,&te->when_ms);
    te->timeProc = proc;
    // 删除事件回调函数
    te->finalizerProc = finalizerProc;
    te->clientData = clientData;
    te->prev = NULL;
    te->next = eventLoop->timeEventHead;
    if (te->next)
        te->next->prev = te;
    eventLoop->timeEventHead = te;
    return id;
}

/**
 * 删除时间事件
 * @param eventLoop
 * @param id 事件id
 * @return
 */
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id)
{
    // 从事件循环中获得时间事件
    aeTimeEvent *te = eventLoop->timeEventHead;
    while(te) {
        if (te->id == id) {
            te->id = AE_DELETED_EVENT_ID;
            return AE_OK;
        }
        te = te->next;
    }
    return AE_ERR; /* NO event with the specified ID found */
}

/* Search the first timer to fire.
 * This operation is useful to know how many time the select can be
 * put in sleep without to delay any event.
 * If there are no timers NULL is returned.
 *
 * Note that's O(N) since time events are unsorted.
 * Possible optimizations (not needed by Redis so far, but...):
 * 1) Insert the event in order, so that the nearest is just the head.
 *    Much better but still insertion or deletion of timers is O(N).
 * 2) Use a skiplist to have this operation as O(1) and insertion as O(log(N)).
 */
// 查找最近的时间事件
static aeTimeEvent *aeSearchNearestTimer(aeEventLoop *eventLoop)
{
    aeTimeEvent *te = eventLoop->timeEventHead;
    aeTimeEvent *nearest = NULL;

    // 循环链表, 找最小的
    while(te) {
        if (!nearest || te->when_sec < nearest->when_sec ||
                (te->when_sec == nearest->when_sec &&
                 te->when_ms < nearest->when_ms))
            nearest = te;
        te = te->next;
    }
    return nearest;
}

/* Process time events */
static int processTimeEvents(aeEventLoop *eventLoop) {
    int processed = 0;
    aeTimeEvent *te;
    long long maxId;
    time_t now = time(NULL);

    /* If the system clock is moved to the future, and then set back to the
     * right value, time events may be delayed in a random way. Often this
     * means that scheduled operations will not be performed soon enough.
     *
     * Here we try to detect system clock skews, and force all the time
     * events to be processed ASAP when this happens: the idea is that
     * processing events earlier is less dangerous than delaying them
     * indefinitely, and practice suggests it is. */
    // 当前时间小于最后一次执行时间, 说明系统时间被调小了
    if (now < eventLoop->lastTime) {
        te = eventLoop->timeEventHead;
        // 循环时间事件链表
        while(te) {
            // 将触发时间设置为0, 一定执行
            te->when_sec = 0;
            te = te->next;
        }
    }
    // 设置最后一次执行时间为当前时间
    eventLoop->lastTime = now;

    te = eventLoop->timeEventHead;
    // 获得最大id
    maxId = eventLoop->timeEventNextId-1;
    while(te) {
        long now_sec, now_ms;
        long long id;

        /* Remove events scheduled for deletion. */
        // 时间事件id等于删除的id
        if (te->id == AE_DELETED_EVENT_ID) {
            // 删除链表中的节点
            aeTimeEvent *next = te->next;
            if (te->prev)
                te->prev->next = te->next;
            else
                eventLoop->timeEventHead = te->next;
            if (te->next)
                te->next->prev = te->prev;
            // 如果设置了finalizeProc, 则执行
            if (te->finalizerProc)
                te->finalizerProc(eventLoop, te->clientData);
            // 释放节点
            zfree(te);
            te = next;
            continue;
        }

        /* Make sure we don't process time events created by time events in
         * this iteration. Note that this check is currently useless: we always
         * add new timers on the head, however if we change the implementation
         * detail, this check may be useful again: we keep it here for future
         * defense. */
        // 时间事件id大于链表最大id, 当前时间事件是最新的事件, 则本次循环不处理
        if (te->id > maxId) {
            te = te->next;
            continue;
        }
        // 获取当前时间
        aeGetTime(&now_sec, &now_ms);
        // 当前的时间大于触发的秒或者当前时间的秒等于触发的秒并且当前时间的毫秒大于触发的毫秒
        if (now_sec > te->when_sec ||
            (now_sec == te->when_sec && now_ms >= te->when_ms))
        {
            int retval;

            id = te->id;
            // 触发时间事件回调函数
            retval = te->timeProc(eventLoop, id, te->clientData);
            // 计数+1
            processed++;
            // 执行多次
            if (retval != AE_NOMORE) {
                // 设置下一次触发时间
                aeAddMillisecondsToNow(retval,&te->when_sec,&te->when_ms);
            } else {
                // 执行一次, id设置为deleted, 下次执行删除该id
                te->id = AE_DELETED_EVENT_ID;
            }
        }
        // 指向下一个节点
        te = te->next;
    }
    // 返回执行次数
    return processed;
}

/* Process every pending time event, then every pending file event
 * (that may be registered by time event callbacks just processed).
 * Without special flags the function sleeps until some file event
 * fires, or when the next time event occurs (if any).
 *
 * If flags is 0, the function does nothing and returns.
 * if flags has AE_ALL_EVENTS set, all the kind of events are processed.
 * if flags has AE_FILE_EVENTS set, file events are processed.
 * if flags has AE_TIME_EVENTS set, time events are processed.
 * if flags has AE_DONT_WAIT set the function returns ASAP until all
 * if flags has AE_CALL_AFTER_SLEEP set, the aftersleep callback is called.
 * the events that's possible to process without to wait are processed.
 *
 * The function returns the number of events processed. */
/**
 * 事件处理
 * @param eventLoop
 * @param flags
 * @return
 */
int aeProcessEvents(aeEventLoop *eventLoop, int flags)
{
    int processed = 0, numevents;

    /* Nothing to do? return ASAP */
    // 既不是时间事件和文件事件, 则直接返回
    if (!(flags & AE_TIME_EVENTS) && !(flags & AE_FILE_EVENTS)) return 0;

    /* Note that we want call select() even if there are no
     * file events to process as long as we want to process time
     * events, in order to sleep until the next time event is ready
     * to fire. */
    // 有文件事件或者有时间事件, 并且不是DONT_WAIT, 则开始处理
    if (eventLoop->maxfd != -1 ||
        ((flags & AE_TIME_EVENTS) && !(flags & AE_DONT_WAIT))) {
        int j;
        // 最近的时间事件
        aeTimeEvent *shortest = NULL;
        struct timeval tv, *tvp;

        // 有时间事件并且不是DONT_WAIT
        if (flags & AE_TIME_EVENTS && !(flags & AE_DONT_WAIT))
            // 查找最近的时间事件
            shortest = aeSearchNearestTimer(eventLoop);
        // 有时间事件
        if (shortest) {
            long now_sec, now_ms;

            // 获得当前时间
            aeGetTime(&now_sec, &now_ms);
            tvp = &tv;

            /* How many milliseconds we need to wait for the next
             * time event to fire? */
            // 计算时间差 毫秒
            long long ms =
                (shortest->when_sec - now_sec)*1000 +
                shortest->when_ms - now_ms;

            // 时间差 > 0, 则设置超时时间的秒和毫秒
            if (ms > 0) {
                tvp->tv_sec = ms/1000;
                tvp->tv_usec = (ms % 1000)*1000;
            } else {
                // 时间差小于0, 则设置超时时间为0
                tvp->tv_sec = 0;
                tvp->tv_usec = 0;
            }
            // 无时间事件
        } else {
            /* If we have to check for events but need to return
             * ASAP because of AE_DONT_WAIT we need to set the timeout
             * to zero */
            // 则设置超时时间为0, 立即返回
            if (flags & AE_DONT_WAIT) {
                tv.tv_sec = tv.tv_usec = 0;
                tvp = &tv;
            } else {
                /* Otherwise we can block */
                // 设置超时时间为NULL, -1 一直等待
                tvp = NULL; /* wait forever */
            }
        }

        /* Call the multiplexing API, will return only on timeout or when
         * some event fires. */
        // io多路复用, 等待就绪的文件事件
        numevents = aeApiPoll(eventLoop, tvp);

        /* After sleep callback. */
        // 有afterSleep并且CALL_AFTER_SLEEP
        if (eventLoop->aftersleep != NULL && flags & AE_CALL_AFTER_SLEEP)
            // 调用afterSleepProc处理
            eventLoop->aftersleep(eventLoop);

        // 循环就绪的文件事件数组
        for (j = 0; j < numevents; j++) {
            // 获得文件事件
            aeFileEvent *fe = &eventLoop->events[eventLoop->fired[j].fd];
            int mask = eventLoop->fired[j].mask;
            int fd = eventLoop->fired[j].fd;
            int fired = 0; /* Number of events fired for current fd. */

            /* Normally we execute the readable event first, and the writable
             * event laster. This is useful as sometimes we may be able
             * to serve the reply of a query immediately after processing the
             * query.
             *
             * However if AE_BARRIER is set in the mask, our application is
             * asking us to do the reverse: never fire the writable event
             * after the readable. In such a case, we invert the calls.
             * This is useful when, for instance, we want to do things
             * in the beforeSleep() hook, like fsynching a file to disk,
             * before replying to a client. */
            // 反转标识
            int invert = fe->mask & AE_BARRIER;

            /* Note the "fe->mask & mask & ..." code: maybe an already
             * processed event removed an element that fired and we still
             * didn't processed, so we check if the event is still valid.
             *
             * Fire the readable event if the call sequence is not
             * inverted. */
            // 不是反转 并且是事件标识可读
            if (!invert && fe->mask & mask & AE_READABLE) {
                fe->rfileProc(eventLoop,fd,fe->clientData,mask);
                fired++;
            }

            /* Fire the writable event. */
            // 是事件标识可写
            if (fe->mask & mask & AE_WRITABLE) {
                // 防止重复处理
                if (!fired || fe->wfileProc != fe->rfileProc) {
                    // 触发写回调函数执行
                    fe->wfileProc(eventLoop,fd,fe->clientData,mask);
                    fired++;
                }
            }

            /* If we have to invert the call, fire the readable event now
             * after the writable one. */
            // 如果是反转, 则先处理写, 后处理读
            if (invert && fe->mask & mask & AE_READABLE) {
                if (!fired || fe->wfileProc != fe->rfileProc) {
                    fe->rfileProc(eventLoop,fd,fe->clientData,mask);
                    fired++;
                }
            }

            // 计数+1
            processed++;
        }
    }
    /* Check time events */
    // 处理时间事件
    if (flags & AE_TIME_EVENTS)
        processed += processTimeEvents(eventLoop);

    return processed; /* return the number of processed file/time events */
}

/* Wait for milliseconds until the given file descriptor becomes
 * writable/readable/exception */
int aeWait(int fd, int mask, long long milliseconds) {
    struct pollfd pfd;
    int retmask = 0, retval;

    memset(&pfd, 0, sizeof(pfd));
    pfd.fd = fd;
    if (mask & AE_READABLE) pfd.events |= POLLIN;
    if (mask & AE_WRITABLE) pfd.events |= POLLOUT;

    if ((retval = poll(&pfd, 1, milliseconds))== 1) {
        if (pfd.revents & POLLIN) retmask |= AE_READABLE;
        if (pfd.revents & POLLOUT) retmask |= AE_WRITABLE;
        if (pfd.revents & POLLERR) retmask |= AE_WRITABLE;
        if (pfd.revents & POLLHUP) retmask |= AE_WRITABLE;
        return retmask;
    } else {
        return retval;
    }
}

void aeMain(aeEventLoop *eventLoop) {
    eventLoop->stop = 0;
    while (!eventLoop->stop) {
        // beforeSleep有效
        if (eventLoop->beforesleep != NULL)
            // 执行beforeSleep
            eventLoop->beforesleep(eventLoop);
        aeProcessEvents(eventLoop, AE_ALL_EVENTS|AE_CALL_AFTER_SLEEP);
    }
}

char *aeGetApiName(void) {
    return aeApiName();
}

void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep) {
    eventLoop->beforesleep = beforesleep;
}

void aeSetAfterSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *aftersleep) {
    eventLoop->aftersleep = aftersleep;
}
