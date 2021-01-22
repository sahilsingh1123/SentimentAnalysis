# todo important --

import multiprocessing
import concurrent.futures
import time

start = time.perf_counter()
def anotherFun():
    print("other function called")

# initial start for the multiprocessing
def do_something():
    print("sleeping for 1 sec...")
    anotherFun()
    print("Done sleeping...")

# p1 = multiprocessing.Process(target= do_something)
# p2 = multiprocessing.Process(target= do_something)
#
# p1.start()
# p2.start()
#
# p1.join()
# p2.join()


###############################################################

def do_something_2():
    print("sleeping for 1 sec...")
    time.sleep(1)
    print("Done sleeping...")

# processes = []
# for _ in range(10):
#     p = multiprocessing.Process(target=do_something_2)
#     p.start()
#     processes.append(p)
#
# for process in processes:
#     process.join()


##########################################################################

def do_something_3(seconds):
    print(f"sleeping for {seconds} sec...")
    time.sleep(seconds)
    return f"Done sleeping...{seconds}"

# with concurrent.futures.ProcessPoolExecutor() as executor:
#     secondsList = [5,4,3,2,1]
#     results = executor.map(do_something_3, secondsList)
#
#     for result in results:
#         # if u want to handle exception. do it here.
#         print(result)



#################################################################


'''
Downloading the files and processing it using the multiprocessing module.
'''

import time
import concurrent.futures
# from PIL import Image, ImageFilter

img_names = [
    'photo-1516117172878-fd2c41f4a759.jpg',
    'photo-1532009324734-20a7a5813719.jpg',
    'photo-1524429656589-6633a470097c.jpg',
    'photo-1530224264768-7ff8c1789d79.jpg',
    'photo-1564135624576-c5c88640f235.jpg',
    'photo-1541698444083-023c97d3f4b6.jpg',
    'photo-1522364723953-452d3431c267.jpg',
    'photo-1513938709626-033611b8cc03.jpg',
    'photo-1507143550189-fed454f93097.jpg',
    'photo-1493976040374-85c8e12f0c0e.jpg',
    'photo-1504198453319-5ce911bafcde.jpg',
    'photo-1530122037265-a5f1f91d3b99.jpg',
    'photo-1516972810927-80185027ca84.jpg',
    'photo-1550439062-609e1531270e.jpg',
    'photo-1549692520-acc6669e2f0c.jpg'
]


size = (1200, 1200)


def process_image(img_name):
    # img = Image.open(img_name)
    # img = img.filter(ImageFilter.GaussianBlur(15))
    # img.thumbnail(size)
    # img.save(f'processed/{img_name}')
    #todo above code will contains the logic
    # related to cpu intensive as well IO intensive task.
    print(f'{img_name} was processed...')


with concurrent.futures.ProcessPoolExecutor() as executor:
    executor.map(process_image, img_names)


finish = time.perf_counter()
print(f'finished in...{round(finish-start,2)} second(s)')

