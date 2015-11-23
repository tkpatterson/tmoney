#SolarCity R Tool Kit #Version 1.5 (11.08.15)
rm(list=ls())
#hello


#Part I: Get Set Up
#Load Packages
library(RODBC)
library(RForcecom)
library(reshape2)
library(plyr)
library(maps)
library(mapdata)
library(mapproj)
library(RCurl)
library(RJSONIO)
library(zipcode)
library(lubridate)
library(data.table)
library(muRL)
library(maptools)
library(sqldf)
library(zipcode)
library(date)
library(ggplot2)
library(shiny)

#library(ggmap)

# Get Connected ----------------------------------------------------------
#Connect to Master Server
master <- odbcDriverConnect('driver={SQL Server};server=ReportingDB;database=master;trusted_connection=true')

#Connect to Solar Works Server
SolarWorks <- odbcDriverConnect('driver={SQL Server};server=ReportingDB;database=SolarWorks;trusted_connection=true')

#Connect to SalesForce Server  
SalesForce <- odbcDriverConnect('driver={SQL Server};server=ReportingDB;database=SalesForce;trusted_connection=true')

#Connect to Analysis Server 
Analysis <- odbcDriverConnect('driver={SQL Server};server=ReportingDB;database=Analysis;trusted_connection=true')
#Connect to Soleo Reference
SoleoReference <- odbcDriverConnect('driver={SQL Server};server=ReportingDB;database=SoleoReference;trusted_connection=true')

#Connect to Soleo Customer
SoleoCustomer <- odbcDriverConnect('driver={SQL Server};server=ReportingDB;database=SoleoCustomer;trusted_connection=true')

PartnerReporting <- odbcDriverConnect('driver={SQL Server};server=ReportingDB;database=PartnerReporting;trusted_connection=true')


# Part III: Party Tricks! ------------------------------------------------
#GeoCode Functions
url <- function(address, return.call = "json", sensor = "false") {
  root <- "http://maps.google.com/maps/api/geocode/"
  u <- paste(root, return.call, "?address=", address, "&sensor=", sensor, sep = "")
  return(URLencode(u))
}

geoCode <- function(address,verbose=FALSE) {
  if(verbose) cat(address,"\n")
  u <- url(address)
  doc <- getURL(u)
  x <- fromJSON(doc,simplify = FALSE)
  if(x$status=="OK") {
    lat <- x$results[[1]]$geometry$location$lat
    lng <- x$results[[1]]$geometry$location$lng
    location_type <- x$results[[1]]$geometry$location_type
    formatted_address <- x$results[[1]]$formatted_address
    return(c(formatted_address, lat, lng))
  } else {
    return(c(NA,NA,NA))
  }
}


#Make Life Easier
GetColumns <- function(object,database){
  ColumnQuery <- paste0("SELECT * FROM information_schema.columns WHERE table_name = '",object,"'")
  Columns <- sqlQuery(database,ColumnQuery)
  Columns <- Columns[with(Columns,order(COLUMN_NAME)),c("COLUMN_NAME","DATA_TYPE")]
  return
  Columns
}

GetTables <- function(database){
  Tables <- sqlQuery(database,'SELECT * FROM information_schema.tables')
  Tables <- Tables[with(Tables, order(TABLE_NAME)),]
  return
  Tables
}

GetHelp <- function(object,database){
  HelpQuery <- paste0("sp_helptext ",object,"")
  HelpResults <- sqlQuery(database,HelpQuery)
  HelpResults <- HelpResults[HelpResults$Text != "\r\n",]
  return
  HelpResults
}

GetTop <- function(object,database){
  TopQuery <- paste0("select top 100 * from ",object,"")
  TopResults <- sqlQuery(database,TopQuery)
  return
  TopResults
}

QueryDate <- function(x){
  DateForQuery <- as.Date(x,"%Y-%m-%d")
  DateForQuery <- as.POSIXct(strptime(paste0(DateForQuery," 00:00"),"%Y-%m-%d %H:%M",tz="America/Los_Angeles")) 
  return
  DateForQuery
}

GetFC <- function(x){
  FCQuery <- paste0("Select FC_SalesRep, FinalContractDate from Analysis..DW_ActiveSalesRep where InstallationID = '",x,"'")
  FC <- sqlQuery(master,FCQuery)
  return
  FC
}

GetBooking <- function(x){
  BookingQuery <- paste0("Select BK_SalesRep, BookingDate from Analysis..DW_ActiveSalesRep where InstallationID = '",x,"'")
  Booking <- sqlQuery(master,BookingQuery)
  return
  Booking
}

GetRepChange <- function(x) {
  ChangeQuery <- paste0("Select h2.InstallationID, h2.Created as 'Date', um.UserName as 'ChangedBy', oldr.UserName as 'OldRep', newr.UserName as 'NewRep' from SolarWorks..InstallationHistory h1 join SolarWorks..InstallationHistory h2 on (h1.InstallationID = h2.InstallationID) join SolarWorks..Users um on (h2.ModifiedBy = um.DomainUserName) join SolarWorks..Users newr on (h2.SalesRepresentativeUserID = newr.UserID)  join SolarWorks..Users oldr on (h1.SalesRepresentativeUserID = oldr.UserID)  join SolarWorks..Installations i on (h2.InstallationID = i.InstallationID) where (h2.InstallationHistoryID - 1) = h1.InstallationHistoryID and h1.SalesRepresentativeUserID <> h2.SalesRepresentativeUserID and h1.InstallationID = '",x,"'")
  Changes <- sqlQuery(master,ChangeQuery)
  return
  Changes
}


substrRight <- function(x, n){
  substr(x, nchar(x)-n+1, nchar(x))
}

substrLeft <- function(x, n){
  substr(x, 1, n)
}

WriteCSV <- function(DropFile,df,y){
  TimeStamp <- format(Sys.time(), " %m-%d-%y") 
  FileName <- paste0(DropFile,y,TimeStamp,".csv")
  write.csv(file=FileName,x=df,row.names = FALSE)
  return
  FileName
}

GetSFColumns <- function(session,object){
  RawCols <- rforcecom.getObjectDescription(session,object)
  Cols <- RawCols[,c("label","name","type")]
  Cols <- Cols[order(Cols$label),]
  return 
  Cols
}