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

a = [1,2,3,4,5]
b = map(lambda x: x*2, a)
print(list(b))
# also usig list comprehension
c = [x*2 for x in a]
print(c)

# filter function -------------------------------
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