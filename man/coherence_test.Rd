\name{coherence_test}
\alias{coherence_test}
\title{
Coherence Test for Hourly Air Temperature and Relative Humidity Data
}
\description{
Performs a coherence test to determine the consistency of hourly air temperature or relative humidity data with respect to the values of the preceding hourly records. If the data value falls within a specified range based on the mean and standard deviation of the previous records, it is marked as coherent.
}
\usage{
coherence_test(dt, variable_analyzed, ventana)
}
\arguments{
  \item{dt}{
  A \code{data.table} object containing the hourly meteorological data.
  }
  \item{variable_analyzed}{
  A \code{character} string specifying the name of the meteorological variable to be analyzed (e.g., "TempAire_Avg", "RelativeHumidity").
  }
  \item{ventana}{
  An \code{integer} specifying the number of preceding hourly records to consider for calculating the mean (\eqn{\mu_1}) and standard deviation (\eqn{\sigma_1}).
  }
}
\details{
The coherence test assesses whether the value of a given hourly data point falls within a range defined by the mean (\eqn{\mu_1}) and standard deviation (\eqn{\sigma_1}) of the previous \code{ventana} hourly records. Specifically, it checks if the data value \eqn{Th} satisfies the condition \eqn{\mu_1 - 3\sigma_1 \leq Th \leq \mu_1 + 3\sigma_1}. Data points meeting this condition are flagged as "Coherent" ("C"), while those that do not are flagged as "Dubious" ("D"). Missing data is flagged as "Not Determined" ("ND").
}
\value{
Returns a modified \code{data.table} that includes an additional column, \code{flag_coherence}, which indicates whether the values of the analyzed variable are "Coherent" ("C"), "Dubious" ("D"), or "Not Determined" ("ND") based on the coherence analysis performed.
}
\references{
}
\author{
Jonnathan Landi
}
\note{
This function is particularly useful for the quality analysis of meteorological data and for validating the consistency of historical data.
}
\seealso{
\code{\link{frollmean}}, \code{\link{frollapply}}, \code{\link{data.table}}
}
\examples{
## Example of use
library(data.table)

# Create an example data.table
dt <- data.table(
  TIMESTAMP = seq.POSIXt(from = as.POSIXct("2024-01-01 00:00"), 
                         to = as.POSIXct("2024-01-01 12:00"), by = "hour"),
  TempAire_Avg = c(20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32)
)

# Apply the coherence test
result <- coherence_test(dt, "TempAire_Avg", 5)
print(result)
}
\keyword{data analysis}
\keyword{quality control}
\concept{coherence}
\concept{meteorological data}
