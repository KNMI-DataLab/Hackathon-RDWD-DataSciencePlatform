import numpy as np
from scipy import interpolate
import pandas as pd
import matplotlib.pyplot as plt

import random





def interpolateAWS(data, newPoint):

    X = data["x"]
    Y = data["y"]
    value = data["value"]

    x,y = np.meshgrid(X,Y, sparse=True)

    #test = np.array([12,13,15,17,13,24])

    #f = interpolate.interp2d(x,y,value,kind='cubic')
    f = interpolate.bisplrep(x,y,value, s=0)

    # use linspace so your new range also goes from 0 to 3, with 8 intervals
    #Xnew = np.linspace(mymin,mymax,8)
    #Ynew = np.linspace(mymin,mymax,8)

    Xnew = newPoint[0]
    Ynew = newPoint[1]
    test = f(Xnew,Ynew)

    print(test)













np.random.seed(11)



X = np.random.rand(50)*200000+100000
Y = np.random.rand(50)*200000+100000
value = np.random.rand(50)*30


plt.scatter(X,Y,c=value,s=200)
plt.show()
print(X)
print(Y)

print(value)

d = {"x": X, "y": Y, "value": value}

df = pd.DataFrame(d)

newPoint = [120000,150000]

interpolateAWS(df, newPoint)

