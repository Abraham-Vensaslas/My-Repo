#Regular expression
k=lambda x,y:x+y
k(2,3)

#lambda inside function

def add(x):
    return (lambda y:x+y)
t=add(3)
print(t(4))

#filter/map/reduce
r=[25,23,6,49,78,96,20]
new_list=tuple(map(lambda a:(a/7==7),r ))
print(new_list)

r=[25,23,6,49,78,96,20]
new_list=tuple(filter(lambda a:(a/7==7),r ))
print(new_list)
