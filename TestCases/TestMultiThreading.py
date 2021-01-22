# todo -- important note. threads are good for inbound. io operations. but it is not good for processing like heavy
#  processing related with CPU. for this we uses multiprocessing module of python.

import threading
import concurrent.futures
import time

start = time.perf_counter()

# initial start for the threading
def do_something():
    print("sleeping for 1 sec...")
    time.sleep(1)
    print("Done sleeping...")

# t1 = threading.Thread(target=do_something)
# t2 = threading.Thread(target=do_something)

# t1.start()
# t2.start()

#to complete the above before moving to next line
# t1.join()
# t2.join()

# finish = time.perf_counter()
# print(f'finished in...{round(finish-start,2)} second(s)')


#################################################################
# this is for testing more advance version

def do_something_2(seconds):
    print(f"sleeping for {seconds} sec...")
    time.sleep(seconds)
    print("Done sleeping...")

# threads = []
# for _ in range(10):
#     t = threading.Thread(target=do_something_2, args=[2])
#     t.start()
#     threads.append(t)
#
# #joining all the threads
# for thread in threads:
#     thread.join()




###################################################################
#in python 3 it has introduced thread pool executor.

def do_something_3(seconds):
    print(f"sleeping for {seconds} sec...")
    time.sleep(seconds)
    return "Done sleeping..."

# this will accept the return val
# with concurrent.futures.ThreadPoolExecutor() as executor:
#     # f1 = executor.submit(do_something_3, 1)
#     # print(f1.result())
#     results = [executor.submit(do_something_3, 1) for _ in range(10)]
#     for f in concurrent.futures.as_completed(results):
#         print(f.result())



#################################################################
# advance version

def do_something_4(seconds):
    print(f"sleeping for {seconds} sec...")
    time.sleep(seconds)
    return f"Done sleeping...{seconds}"

with concurrent.futures.ThreadPoolExecutor() as executor:
    secondsList = [5,4,3,2,1]
    results = executor.map(do_something_4, secondsList)


finish = time.perf_counter()
print(f'finished in...{round(finish-start,2)} second(s)')
