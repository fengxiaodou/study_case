
library(DBI)
library(RMySQL)
library(dplyr)
library(magrittr)
library(xgboost)
mycon <- DBI::dbConnect(RMySQL::MySQL(), 
                        host = "172.21.100.3",
                        user = rstudioapi::askForPassword("Database user_name"),
                        password = rstudioapi::askForPassword("Database password")
)