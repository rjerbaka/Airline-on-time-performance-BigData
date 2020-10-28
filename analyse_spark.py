import sys
import textwrap
import itertools
import numpy as np
import matplotlib.pyplot as plt
import get_rdd as grdd
import scipy as sp
from scipy.stats import chi2_contingency
from scipy.stats import chi2




def Mean_Age(D):

    """ Returns average age of the planes in a dataset (using Map/Reduce). 

    Parameters
    ------------
        D:
            RDD of flight data to use to compute the age mean.

    """

    DN = (
        D.map(lambda flight : (1, 2005 - flight.MFRYear))
         .reduce(lambda x, y : x + y)
         .collect()
    )

    (count, total) = DN

    mean = total / float(count)

    return DN


def delay_group(x):
   
    """ Returns flight delay group number (1 to 5) for a delay value (to create classes). 

    Parameters
    ------------
        x:
            delay value.

    """

    if x <=0 :
       return 0
    if x<= 30 :
       return 1
    if x<= 60:
       return 2
    if x<= 120:
       return 3
    if x <= 180:
       return 4
    return 5

def age_group(x):

    """ Returns plane age group number (1 to 5) for a given plane age (to create classes). 

    Parameters
    ------------
        x:
            age.

    """

    if x <= 5 :
       return 0
    if x<= 10 :
       return 1
    if x<= 15 :
       return 2
    if x<= 20 :
       return 3
    if x <= 25 :
       return 4
    return 5

def count_age_del(D):

   """ Returns age/delay contingency table & Chi-square test results.

   Parameters
   ------------
      D:
            RDD of flight data to use to create contingency table.

   """

  count_agedel = (D.map(lambda flight : ((delay_group(flight.DepDelay), age_group(flight.Year - flight.MFRYear)), 1))
                             .reduceByKey(lambda x,y : x+y)
                             .collect()
                            )

  m_count_agedel = np.zeros((6,6), dtype = int)

  for (d,a), c in count_agedel:
      m_count_agedel[d, a] = c

  X2, pval, df, pred = chi2_contingency(m_count_agedel)

  fig, (ax_real, ax_pred) = plt.subplots(2,1)
  ax_real.imshow(m_count_agedel, cmap = "jet")
  ax_pred.imshow(pred, cmap = "jet")

  return m_count_agedel, fig, (X2, pval, df, pred)



