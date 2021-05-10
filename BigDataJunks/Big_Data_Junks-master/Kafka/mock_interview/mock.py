print("My First EIT Mock Interview")
i =0
j = -1
if (i > j):
    print ("true")


cur =-1234
next = -3345
a =[1,2,34,56,7]

for i in range(0, len(a)):
    if (a[i] > cur):
        next = cur
        cur = a[i]
    elif (a[i] > next and a[i] != cur):
        next = a[i]
print (next)


