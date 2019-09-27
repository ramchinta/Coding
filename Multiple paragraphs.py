import sys

'''print('enter text:')
t =input()
#t = sys.stdin.readlines()
#n = 20
print(t.replace('Lakshman',' '))
#print (h)
#n = (textwrap.fill(t.replace(\n,''),20))
#print(n)
a =('First Line \n Second Line \nThird Line')
print(a)'''

import textwrap
#data = 'Lakshman is in Virginia \n Virginia is Awesome \n He went to schol in Texas'
data = sys.stdin.readlines()
#t = (data.replace('\n','                  '),20)
string = ''
for i in data:
    string = string + str(i)
print (textwrap.fill(string,20))

'''Lakshm'''















