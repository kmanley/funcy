from twisted.internet import reactor

import time
import threading

from functools import wraps
class NoValue: pass

"""
Simple rules:
- break your long running function into chunks by yielding periodically
- yield <progress>, <value>
- if you have to value to yield, yield NoValue
- to do a simple return value, yield NoValue until the last yield. In the last
-  yield, specify the value you want to return
"""
def _longRunningReturnsList():
    yield 0, 1
    time.sleep(1.0)
    yield 33, 2
    time.sleep(1.0)
    yield 66, 3
    time.sleep(1.0)
    yield 100, 4
    
def _longRunningReturnsScalar():
    yield 0, NoValue
    time.sleep(1.0)
    yield 50, NoValue
    time.sleep(1.0)
    yield 100, 20
    
def _longRunningReturnsDict():
    yield 0, NoValue
    time.sleep(.5)
    yield 25, NoValue
    time.sleep(.5)
    yield 50, NoValue
    time.sleep(.5)
    yield 75, NoValue
    time.sleep(.5)
    yield 100, {"one":1, "two":2}
    
def iteratorFunction(func):
    @wraps(func)
    def _target(*args, **kwargs):
        progressCallback = kwargs.pop("progressCallback", lambda x : None)
        for progress, value in func(*args, **kwargs):
            progressCallback(_target, progress)
            yield value
    return _target
    
def blockingFunction(func):
    @wraps(func)
    def _target(*args, **kwargs):
        progressCallback = kwargs.pop("progressCallback", lambda x : None)
        retval = []
        for progress, value in func(*args, **kwargs):
            progressCallback(_target, progress)
            if value != NoValue:
                retval.append(value)
        return retval[0] if len(retval)==1 else retval
    return _target

def asyncFunctionMainThread(func, delay=0, eventLoopCallLater=reactor.callLater):
    @wraps(func)
    def _target(*args, **kwargs):
        progressCallback = kwargs.pop("progressCallback", lambda x : None)
        doneCallback = kwargs.pop("doneCallback", lambda x : None)
        retval = []
        it = func(*args, **kwargs)
        def _nextIteration(_it, _retval, _progressCallback, _doneCallback):
            try:
                progress, value = _it.next()
            except StopIteration:
                eventLoopCallLater(0, _doneCallback, _target, _retval[0] if len(_retval)==1 else retval)
            else:
                eventLoopCallLater(0, _progressCallback, _target, progress)
                if value != NoValue:
                    _retval.append(value)
                eventLoopCallLater(delay, _nextIteration, _it, _retval, _progressCallback, _doneCallback)
        eventLoopCallLater(delay, _nextIteration, it, retval, progressCallback, doneCallback)
    return _target

def asyncFunctionNewThread(func):
    @wraps(func)
    def _target(*args, **kwargs):
        doneCallback = kwargs.pop("doneCallback", lambda x : None)
        def threadfunc(*args, **kwargs):
            retval = blockingFunction(func)(*args, **kwargs)
            doneCallback(_target, retval)
        thread = threading.Thread(target=threadfunc, args=args, kwargs=kwargs)
        thread.start()
        return thread
    return _target
    
asyncReturnsList = asyncFunctionMainThread(_longRunningReturnsList)
asyncReturnsScalar = asyncFunctionMainThread(_longRunningReturnsScalar)
asyncReturnsDict = asyncFunctionMainThread(_longRunningReturnsDict)

asyncReturnsDictNewThread = asyncFunctionNewThread(_longRunningReturnsDict)
asyncReturnsListNewThread = asyncFunctionNewThread(_longRunningReturnsList)


def multiprogress(func, count):
    sources = {}
    lock = threading.RLock()
    @wraps(func)
    def _target(source, progress):
        with lock:
            sources[source] = progress
            combinedProgress = sum(sources.values()) / float(count)
            func(sources.keys(), combinedProgress)
    return _target

def multidone(func, count):
    sources = {}
    lock = threading.RLock()
    @wraps(func)
    def _target(source, retval):
        with lock:
            sources[source] = retval
            if len(sources) == count:
                func(sources.keys(), sources.values())
    return _target

#longRunning = blockingFunction(_longRunning)
#print list(iteratorFunction(_longRunningReturnsList)())  
    
#print blockingFunction(_longRunningReturnsList)()
#def cb(x):
#    print ">%s<"% x
#print blockingFunction(_longRunningReturnsScalar)(progressCallback=cb)

# TODO: does this work with main thread and new thread? It should TODO: except what about join...
def runAllAsync(funcsAndParams, progressCallback=None, doneCallback=None):
    progressCallback = multiprogress(progressCallback or (lambda *args, **kwargs: None), len(funcsAndParams))
    doneCallback = multidone(doneCallback or (lambda *args, **kwargs : None), len(funcsAndParams))
    retval = []
    for item in funcsAndParams:
        if callable(item):
            func = item
            retval.append(func(progressCallback=progressCallback, doneCallback=doneCallback))
        else:
            itemLen = len(item)
            if itemLen==2: # assume 2-element sequence (func, args)
                func, args = item
                retval.append(func(*args, progressCallback=progressCallback, doneCallback=doneCallback))
            elif itemLen==3: # assume 3-element sequence (func, args, kwargs)
                func, args, kwargs = item
                kwargs["progressCallback"] = progressCallback
                kwargs["doneCallback"] = doneCallback
                retval.append(func(*args, **kwargs))
            else:
                raise ValueError("expect 1,2 or 3-element sequence for each item in funcsAndParams")
    return retval

def done(f, x):
    print "done! %s value is %s" % (f, x)
    #reactor.callLater(0, reactor.stop)
def progress(f, x):
    print "progress: %s: %s%%" % (f, x)
            
            
def main():

    prog = multiprogress(progress, 3)
    alldone = multidone(done, 3)
    
    #asyncReturnsList(progressCallback=prog, doneCallback=alldone)
    #asyncReturnsScalar(progressCallback=prog, doneCallback=alldone)
    #asyncReturnsDict(progressCallback=prog, doneCallback=alldone)    
    #reactor.callLater(0, reactor.stop)
    
    runAllAsync((asyncReturnsList, asyncReturnsScalar, asyncReturnsDict), progress, done)

#print 'Hello ',
#reactor.callWhenRunning(main)
#reactor.run()

#t1 = asyncReturnsDictNewThread(progressCallback=progress, doneCallback=done)
#t2 = asyncReturnsListNewThread(progressCallback=progress, doneCallback=done)
#t1.join()
#t2.join()

if 0:
    progress = multiprogress(progress, 2)
    done = multidone(done, 2)
    
    t1 = asyncReturnsDictNewThread(progressCallback=progress, doneCallback=done)
    t2 = asyncReturnsListNewThread(progressCallback=progress, doneCallback=done)
    t1.join()
    t2.join()

if 1:
    #progress = multiprogress(progress, 2)
    #done = multidone(done, 2)
    
    tids = runAllAsync((asyncReturnsDictNewThread, asyncReturnsListNewThread), progress, done)
    for tid in tids:
        tid.join()
    #t1.join()
    #t2.join()


#print longRunning()
