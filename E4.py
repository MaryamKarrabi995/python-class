# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 15:02:29 2020

@author: maryam
"""

year = int(input('enter year: ' ))
month = int(input('enter month: '))
day = int(input('enter day: '))
print ("date:  = {}/{}/{}".format(year, month, day))


# to check if year is a leap year or not
if (year % 4) == 0:
   if (year % 100) == 0:
       if (year % 400) == 0:
           print("{} is a leap year".format(year))
           x = True
       else:
           print("{} is not a leap year".format(year))
           x = False
   else:
       print("{} is a leap year".format(year))
       x = True
else:
   print("{} is not a leap year".format(year))
   x = False
   
m = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
mo = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
if (x):
    if month > 2:
        result = 0
        for i in m[:month-1]:
            result = result  + i
        result = result + day
        print('Number of days past this year:', result)
    elif month == 2:
        result = 31 + day
        print('Number of days past this year:', result)
    else:
        print('Number of days past this year:', day)
        
else:
    if month > 2:
        result = 0
        for i in mo[:month-1]:
            result = result  + i
        result = result + day
        print('Number of days past this year:', result)
    elif month == 2:
        result = 31 + day
        print('Number of days past this year:', result)
    else:
        print('Number of days past this year:', day)
    
    
    
    
    
    
    
    
    
    
    
    