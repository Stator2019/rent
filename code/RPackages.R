  options(scipen = 999)
  options(xlsx.date.format = 'yyyy/mm/dd')
  
  pakgs <- c("DBI", "RMySQL", "devtools", "tidyverse", "dplyr", "ggplot2", "rvest",
             "tmcn", "writexl", "readxl", "openxlsx", "Lahman", "stringr", "fasttime",
             "magrittr", "ff", "zoo", "gapminder", "jiebaR", "data.table", 
             "xlsx", "Hmisc", "gmodels", "forecast", "TSA", "lubridate",
             "lattice", "modelr", "purrr", "knitr", "rmarkdown", "lobstr",
             "rlang", "shiny", "deSolve", "showtext", "reticulate")
  
  for(i in pakgs){
    try({library(i, character.only = T)})
  }
  