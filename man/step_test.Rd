\name{step_test}
\alias{step_test}
\title{
Step Change Test for a Data Table Variable
}
\description{
This function performs a step change test on a specified variable in a data table, calculating the difference between the current value and lagged values, and flags these differences based on specified thresholds.
}
\usage{
step_test(dt, variable_analyzed, lags, thresholds)
}
\arguments{
  \item{dt}{
    A \code{data.table} containing the data to be analyzed.
  }
  \item{variable_analyzed}{
    A \code{character} string representing the name of the variable to be analyzed within \code{dt}.
  }
  \item{lags}{
    A \code{numeric} vector specifying the lag values for which the differences should be calculated.
  }
  \item{thresholds}{
    A \code{numeric} vector of the same length as \code{lags}, specifying the thresholds for flagging the differences.
  }
}
\details{
This function creates new columns in the provided \code{data.table} corresponding to the absolute differences between the values of the specified variable and its lagged values. It then assigns flags based on these differences and the provided thresholds: "ND" for missing values, "C" if the difference is within the threshold, and "D" if the difference exceeds the threshold.
}
\value{
A \code{data.table} with additional columns for each calculated difference and corresponding flag, indicating whether each difference is "ND" (Not Determined if NA), "C" (Compliant within the threshold), or "D" (Non-compliant, outside the threshold).
}
\author{
Jonnathan Landi.
}
\note{
This function is designed for use with data tables in R and assumes that the length of \code{lags} and \code{thresholds} are the same.
}
\seealso{
\code{\link[data.table]{shift}}, \code{\link[data.table]{data.table}}
}
\examples{
library(data.table)
dt <- data.table(Var1 = c(NA, 5, 15, 25, 35))
lags <- c(1, 2)
thresholds <- c(5, 10)
result <- step_test(dt, "Var1", lags, thresholds)
print(result)
}
\keyword{data manipulation}
\keyword{time series analysis}
