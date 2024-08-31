# **data.QC**

data.QC is an algorithm designed for robust quality control of hydrometeorological data. This algorithm consists of several tests that are performed on the hourly data, marking as “C” for data that meet all the tests, “D” for data that do not meet one or more of the rules considered.

### Installation 

------------------------------------------------------------------------

You can test the new features from the Github development channel:

```         
if (!require(devtools)) install.packages("devtools") 
library(devtools)
install_github("Jonnathan-Landi/data.QC")
```
