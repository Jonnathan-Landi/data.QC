\name{hard_limits}
\alias{hard_limits}
\title{
Apply Hard Limits to a Data Table Variable
}
\description{
This function applies hard limits to a specified variable in a data table and flags the values based on whether they fall within or outside of the specified limits.
}
\usage{
hard_limits(dt, variable_analyzed, LimInf, LimSup)
}
\arguments{
  \item{dt}{
    A \code{data.table} containing the data to be analyzed.
  }
  \item{variable_analyzed}{
    A \code{character} string representing the name of the variable to be analyzed within \code{dt}.
  }
  \item{LimInf}{
    A \code{numeric} value specifying the lower limit for the variable.
  }
  \item{LimSup}{
    A \code{numeric} value specifying the upper limit for the variable.
  }
}
\details{
This function uses the \code{fifelse} function to create flags for each row in the specified variable, based on whether the value is \code{NA}, within the specified hard limits (\code{LimInf} and \code{LimSup}), or outside of them.
}
\value{
A \code{data.table} with an additional column \code{Flag_LimDuros} indicating whether each value is "ND" (Not Determined if NA), "C" (Compliant within the limits), or "M" (Non-compliant, outside the limits).
}
\references{
No references available.
}
\author{
Your Name or the Name of the Function's Author
}
\note{
This function is intended for use with data.tables in R.
}
\seealso{
\code{\link[data.table]{fifelse}}, \code{\link[data.table]{data.table}}
}
\examples{
library(data.table)
dt <- data.table(Var1 = c(NA, 5, 15, 25, 35))
result <- hard_limits(dt, "Var1", 10, 30)
print(result)
}
\keyword{data manipulation}
\keyword{limits}
