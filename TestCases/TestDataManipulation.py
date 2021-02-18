'''iterators and iterables'''
# list,string,tuples,dict are iterables(located in memory allocation)
# li = [1,2,3,4]
# liIter = iter(li) # incase of iters it is not allocated or initialized in memory. we need to call next then only first element inititalized in memory.
# print(next(liIter)) # every time next is called it will fetch the data and put into memory. when u got at the end then throws an error.


#set---------------------------------------------------
# set no duplicates
#myset = set("Hello")
#myset.remove("e") #- error when not present
#myset.discard("e") # no error if elemtn not presetn there.
#myset.pop() #default it will pop the first elemtn

# odds = {1,3,5}
# evens = {0,2,4}
# primes = {2,3,5,7}

# u = odds.union(evens)
# u = odds.intersection(evens)
#u = odds.difference(evens) # take the data from odds and not from evens

# String ---------------------------------------------------------
# greeting = "    hello   "
# functions like startWith find endwith
# if "el" in greeting:
#     print('found')

# greeting = greeting.strip()

# listtets = ['1','2','c']
# string = ''.join(listtets)
# print(string)

# lambda function -----------------------------------------------------
# lambda x: x + 1

# add10 = lambda x: x + 10

''' map function -------------------------------'''

# a = [1,2,3,4,5]
# b = map(lambda x: x*2, a)
# print(list(b))
# # also usig list comprehension
# c = [x*2 for x in a]
print('test')

'''filter function -------------------------------'''
# filter(fuc, seq) must return true or false.
# a = [1,2,3,4,5]
# b = filter(lambda x: x%2==0, a)
# print(list(b))
# c = [x for x in a if x%2==0]
# print(c)
'''reduce '''
'''
from functools import reduce

a = [1,2,3,4,5]
re = reduce(lambda x,y: x+y, a)
print(re)

'''
#--------------------shallow and deep copy
# shallow - one level deep only refrences of nested child objects
# deep - full independent copy

# shallow-
import copy
# org = [1,2,3,4,5]
# cpy = copy.copy(org)
# cpy = org.copy()
# cpy = list(org)
# cpy = org[:].

# deep copy
# org = [[1,2,3,4,5], [4,5,6]]
# #cpy = copy.copy(org)
# cpy = copy.deepcopy(org)
# cpy[0][1] = -9 # this will affect the org value also so to avoid this we need to use the deepcopy.
# print(org)
# print(cpy)


##-------------------------------------------------------------------
'''string or list reverse manual method'''
'''
####################33
to iterate a list in reverse order
for val in reversed(list):
    print(val)


##################333333
index = len(listStr)
for val in listStr:
    while index:
        index -= 1
        print(listStr[index])

'''

#################################################3
#taking input from the user
'''
if __name__ == '__main__':
    while True:
        val = input()
        if val.__eq__("quit"):
            break
        else:
            print('input Val == :', val)
'''

###################################
'''default dict functionality in the python'''
from collections import defaultdict
#
# Dic = defaultdict()
# print(Dic['test'])

'''sub string finding in a string'''

# testString = "sahil this is very bad"
# subString = "this"
# print(testString.find(subString))
# if subString in testString:
#     print('yes')

'''pass by value and refrence'''
# import copy
# def appendNum(arr):
#     arr.append(4)
#     return arr
#
# arr = [1,2,3]
# print(arr)
# t = appendNum(copy.deepcopy(arr))
# print(arr)
# print(t)

'''list pop method'''
# l = [1,2,3]
# l.pop(0)
# print(l)


'''namedtuple'''
# from collections import namedtuple
# Point = namedtuple('Point', ['a','b'])
# p = Point(2,3)
# p._asdict()
# t = [2,3]
# p._make(t)
# print(p.a, p.b)

'''counter'''
# from collections import Counter
# li = [1,1,2,2,2,3,3,3,3]
# print(Counter(li))

'''chainmap'''
# from collections import ChainMap
# dic1 = {'a':1, 'b':3}
# dic2 = {'a':2, 'b':3}
# dic3 = {'a':1, 'b':3}
# chainedDict = ChainMap(dic1, dic2, dic3)
# # print(chainedDict['a'])
# # chainedDict.new_child(dic1)
# print(chainedDict.values())

'''deque'''
# from collections import deque
# # initializing deque
# de = deque([1, 2, 3])
# # using append() to insert element at right end
# # inserts 4 at the end of deque
# de.append(4)
# # printing modified deque
# # using appendleft() to insert element at right end
# # inserts 6 at the beginning of deque
# de.appendleft(6)
# # printing modified deque
# print(de)

'''fibonacci series'''
# 0,1,1,2,3,5,8,13
# def fib(n):
#     if n < 2:
#         return n
#     else:
#         return  fib(n-2) + fib(n-1)
#
# print(fib(6))
