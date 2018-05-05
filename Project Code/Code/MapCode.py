# Big Data Application Development Project - Spring 2018
# Optimal Strategies for PlayerUnknown's Battlegrounds
# Mathew Thomas - N15690387
#
# Code to generate Heat Maps from clustering data obtained from Spark analytic
#
# The code takes in a file containing cluster centers, weights it by the number
# of data points in each cluster, generates a region around each center with
# a decaying intensity, and superimposes these regions on the map image to
# generate a heat map.
#
# Thanks to Kaggle user skihikingkevin for the method of superimposing a
# heat map on an image in python using SciPy's gaussian_filter and matplotlib
#

import numpy as np
import matplotlib.pyplot as plt
from scipy.misc.pilutil import imread

from scipy.ndimage.filters import gaussian_filter
import matplotlib.cm as cm
from matplotlib.colors import Normalize
import random
import math
from random import randint

def genCircle(center,radius):

    points = []
    rads = np.arange(0,radius,0.5)
    numRads = len(rads)
    numPoints = []
    for i in range(numRads):
        numPoints.append(randint(1, 1000))
    numPoints = np.array(numPoints)
    numPoints = -np.sort(-numPoints)
    #print(numPoints)
    for ind in range(len(numPoints)):
        density = numPoints[ind]
        rad = rads[ind]
        for i in range(density):
            theta = math.sqrt(2*math.pi*random.random())
            #print(center[0],radius,theta)
            x = center[0] + rad * math.cos(theta)
            y = center[1] + rad * math.sin(theta)
            pt = [x,y]
            points.append(pt)
    return np.array(points)

def heatmap(x, y, s, bins=100):
    heatmap, xedges, yedges = np.histogram2d(x, y, bins=bins)
    heatmap = gaussian_filter(heatmap, sigma=s)

    extent = [xedges[0], xedges[-1], yedges[0], yedges[-1]]
    return heatmap.T, extent

def loadOutDisp(fname):
    f = open(fname)
    i = 0
    for line in f:
        weapon = line.split(',')[0]
        if weapon not in ['Down and Out','Falling','Bluezone', 'RedZone', 'killed_by']:
            print(str(i+1) + ". " + weapon + "\n")
            i = i + 1
        if i>9:
            break
def makeHeatMap(fname,wname,image,bins):
  f = open(fname)
  coords = np.array([[0,0]])
  g = open(wname)
  weights = []

  for weight in g:
      weight = weight.translate(None, '()[]')
      weight = weight.split(',')[1]
      weight = float(weight)
      weights.append(weight)
  i = 0
  #print(weights)
  for line in f:
    #print(i)
    line = line.translate(None, '[]')
    point = line.split(',')
    point = map(float, point)
    point = map(abs,point)
    #print(genCircle(point,100).shape)
    coords = np.append(coords,genCircle(point,int(weights[i]*100000)),axis=0)
    i = i+1
  coords = coords[1:]
  print(coords.shape)
  bg = imread(image)
  hmap, extent = heatmap(coords[:,0], coords[:,1], 1.5,bins)
  print(hmap.shape)
  #hmap = hmap[~np.all(hmap == 0, axis=1)]
  #hmap = hmap[hmap!=[0.0,0.0]]
  alphas = np.clip(Normalize(0, hmap.max(), clip=True)(hmap)*1.5, 0.0, 1.)
  colors = Normalize(0, hmap.max(), clip=True)(hmap)
  colors = cm.Reds(colors)
  colors[..., -1] = alphas

  fig, ax = plt.subplots(figsize=(24,24))
  if "erangel" in image:
    ax.set_xlim(0, 4096); ax.set_ylim(0, 4096)
  else:
    ax.set_xlim(0, 1000); ax.set_ylim(0, 1000)
  ax.imshow(bg)
  ax.imshow(colors, extent=extent, origin='lower', cmap=cm.Reds, alpha=1.0)
  #plt.scatter(coords[:,0], coords[:,1])
  plt.gca().invert_yaxis()
  plt.savefig("../Plots/" + fname[11:-3]+"jpg")

#genCircle((0.0,0.0),5.0)

print("Starting Early Game")
makeHeatMap("../Results/erangelClusterCentersEarly.txt","../Results/erangelClusterWeightsEarly.txt","../Images/erangel.jpg",375)
print("Erangel Early Game Done")
loadOutDisp("../Results/Loadouts/EarlyGameErangel.csv")

makeHeatMap("../Results/miramarClusterCentersEarly.txt","../Results/miramarClusterWeightsEarly.txt","../Images/miramar.jpg",1250)
print("Miramar Early Game Done")
loadOutDisp("../Results/Loadouts/EarlyGameMiramar.csv")

print("Starting Mid Game")
makeHeatMap("../Results/erangelClusterCentersMid.txt","../Results/erangelClusterWeightsMid.txt","../Images/erangel.jpg",200)
print("Erangel Mid Game Done")
loadOutDisp("../Results/Loadouts/MidGameErangel.csv")

makeHeatMap("../Results/miramarClusterCentersMid.txt","../Results/miramarClusterWeightsMid.txt","../Images/miramar.jpg",1000)
print("Miramar Mid Game Done")
loadOutDisp("../Results/Loadouts/MidGameMiramar.csv")

print("Starting Late Game")
makeHeatMap("../Results/erangelClusterCentersLate.txt","../Results/erangelClusterWeightsLate.txt","../Images/erangel.jpg",150)
print("Erangel Late Game Done")
loadOutDisp("../Results/Loadouts/LateGameErangel.csv")

makeHeatMap("../Results/miramarClusterCentersLate.txt","../Results/miramarClusterWeightsLate.txt","../Images/miramar.jpg",500)
print("Miramar Late Game Done")
loadOutDisp("../Results/Loadouts/LateGameMiramar.csv")
