# **data.QC**

data.QC is an algorithm designed for robust quality control of hydrometeorological data. This algorithm consists of two stages, the first on a 5-minute scale and the second on an hourly scale.
The tests performed on the hourly data, mark as “C” the data that meet all the tests, “D” the data that do not meet one or more of the rules considered.
¡¡¡¡¡¡¡NOTE !!!!!!!
This algorithm is under development, some files have not yet been uploaded to the repository, as they are locally in the testing phase.

### Installation

------------------------------------------------------------------------

You can test the new features from the Github development channel:

```         
if (!require(devtools)) install.packages("devtools") 
library(devtools)
install_github("Jonnathan-Landi/data.QC")
```
