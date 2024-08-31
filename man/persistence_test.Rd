\name{persistence_test}
\alias{persistence_test}
\title{
Persistence Test for Hourly Meteorological Data
}
\description{
Performs a persistence test to identify dubious values in hourly meteorological data. If an hourly value is the same as the three consecutive previous hourly values, it is marked as dubious.
}
\usage{
persistence_test(dt, variable_analyzed, n_persistence)
}
\arguments{
  \item{dt}{
  A \code{data.table} object containing the hourly meteorological data.
  }
  \item{variable_analyzed}{
  A \code{character} string specifying the name of the meteorological variable to be analyzed (e.g., "TempAire_Avg", "RelativeHumidity", etc.).
  }
  \item{n_persistence}{
  An \code{integer} that defines the number of consecutive hours with the same value considered to determine persistence.
  }
}
\details{
The persistence test is useful for identifying meteorological data that have not changed over a specific period, which can indicate a stuck sensor or incorrect data. This function applies to various meteorological variables, including air temperature, relative humidity, wind direction, wind speed, atmospheric pressure, and solar radiation. The function checks if the current values are equal to the values of the previous hours specified by \code{n_persistence} and marks them as "Dubious" if persistence is detected.
}
\value{
Returns a modified \code{data.table} that includes an additional column, \code{Persistence_flag}, which indicates whether the values of the analyzed variable are "Dubious" ("D"), "Reliable" ("C"), or "Not Determined" ("ND") based on the persistence analysis performed.
}
\references{

}
\author{
Jonnathan Landi
}
\note{
This function is particularly useful for the quality analysis of meteorological data and for validating historical data.
}
\seealso{
\code{\link{shift}}, \code{\link{fifelse}}, \code{\link{data.table}}
}
\examples{
## Example of use
library(data.table)
library(lubridate)
library(data.QC)

# Create an example data.table
dt <- data.table(
  TIMESTAMP = seq.POSIXt(from = as.POSIXct("2024-01-01 00:00"), 
                         to = as.POSIXct("2024-01-01 12:00"), by = "hour"),
  TempAire_Avg = c(20, 20, 20, 20, 21, 22, 23, 24, 25, 25, 25, 25, 25)
)

# Apply the persistence test
result <- persistence_test(dt, "TempAire_Avg", 3)
print(result)
}

\keyword{data analysis}
\keyword{quality control}
\concept{persistence}
\concept{meteorological data}

