import array
import sys
import textwrap
import re

print('enter text:')
t = sys.stdin.readlines()
n = 20
#para_edge = re.compile(r"(\n\s*\n)", re.MULTILINE)
#paragraphs = para_edge.split(t)
#print(paragraphs)
n = textwrap.indent(t,prefix = '',predicate= lambda line : True)
#n = (textwrap.fill(t,n))
print(n)

def attempt2(t,n):
    l = []
    for j in range(0,len(t),n):
        l.append(t[j:j+5])

    h = (t.split())

    for i in h:
        #print(i)
        print()
        #if len(i)<10:
            #print(i,i+1)





#attempt2(t,n)

