#generators --> it wont keep the data in memory so it is faster and memory efficient.
def square(nums):
    for i in nums:
        yield i*i

# if you want, u can loop through a for loop if needed. or directly convert that sqrNums into the list.
# sqrNums = square([2,3,4])
# print(next(sqrNums))

#----------------------------------------------------------------------------------------->

#DECORATORS.

#==========>>
''' closure concept needs to be clear here. '''
#below is the example of closure function.
def outerFunc(*args):
    def innerFunc():
        print(args[0])
    return innerFunc

# funcOne = outerFunc("funcOne")
# funcTwo = outerFunc("funcTwo")
# funcOne()
# funcTwo()

'''DECORATORS.
=> decorators is a function that takes another function as an arguments
=> without altering the source code of original function.
'''
from functools import wraps

def decoratorFunc(func):
    @wraps(func)
    def wrapperFunc(*args, **kwargs):
        print("from Method execute these above method when below func is used with decorator {}.".format(func.__name__))
        return func(*args, **kwargs)
    return wrapperFunc

def decoratorFuncTwo(func):
    @wraps(func)
    def wrapperFunc(*args, **kwargs):
        print("from Method (2) execute these above method when below func is used with decorator {}.".format(func.__name__))
        return func(*args, **kwargs)
    return wrapperFunc

#using decorator with class.
class decoratorClass(object):
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        print("from class execute these above method when below func is used with decorator {}.".format(self.func.__name__))
        return self.func(*args, **kwargs)

@decoratorFuncTwo
@decoratorFunc
def displayMessage():
    print("dislay function executed.")
'''note --<>
when we use more than one decorator on a function. we have to clear about the working behaviour of the decorator.
the above decorators works like->
displayMessage = decoratorClass(decoratorFunc(displayMessage)) => always remembers that the decorator class will get the 
instance of wrapperFunc not the instance of displayMessage. so beware while using multiple decorators in a methods.

--> you can use wraps function from functools which will handle this above issue.
'''
#displayMessage()
