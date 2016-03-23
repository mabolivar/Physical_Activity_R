#==============================================================================
# Type:        Source
# Title:       Accelerometry data processing package (ADPP)
# Version:     1.0
# Date:        2013-11-27
# Author:      Manuel A. Bolívar <ma.bolivar643@uniandes.edu.co> 
#              Olga L. Sarmiento <osarmien@uniandes.edu.co>
#              (Universidad de Los Andes. Bogotá, Colombia)            
# Maintainer:  Olga L. Sarmiento <osarmien@uniandes.edu.co>
# Description: ADPP contains a collection of functions to process multi-day   
#              rawaccelerometry data from ActiGraph accelerometry devices 
#              (GT3X and GT3X+).
# Depends:     RSQLite, sqldf, xlsx, Hmisc
#
#==============================================================================

#==============================================================================
# Required libraries
#==============================================================================

library("RSQLite")
library("sqldf")
library(lubridate)
library("Hmisc")
library(dplyr)

#==============================================================================
# Functions
#==============================================================================

## connects to db and extract the raw data from the .agd file
readDatabase <- function(dbDir){
  # Connects to db and extract the raw data from the .agd file
  #
  # Args:
  #   dbDir: The name of the file which the data are to be read from. If it does not contain an absolute path, the file name is relative to the current working directory, getwd().
  #
  # Returns:
  #   A list of data frames. data
  #     data: Data frame which contains the accelerometry data according to the parameters fixed in ActiLife software (i.e. sample rate, # of axis, steps, light and inclinometer). 
  #     settings: Data frame which contains all the device information and settings fixed in ActiLife Software (device serial, device version, subject name, epoch length, startdatetime, stopdatetime, downloaddatetime, etc.)
  
  con <- dbConnect(RSQLite::SQLite(), dbname=dbDir)
  
  ## list all tables
  tables <- dbListTables(con)
  
  ## exclude sqlite_sequence (contains table information)
  tables <- tables[tables != "sqlite_sequence"]
  
  dataF <- vector("list")
  
  ## create a data.frame for settings
  dataF[["settings"]] <- dbGetQuery(conn=con, statement=paste("SELECT * FROM '", "settings", "'", sep=""))
  
  ## create a data.frame for data
  dataF[["data"]] <- dbGetQuery(conn=con, statement=paste("SELECT (cast(dataTimestamp as double))*1.0e-7 as datetime, * FROM '", "data", "'", sep=""))
  dataF$data<-dataF$data[,!names(dataF$data)%in%c("dataTimestamp")] #remove dateTimeStamp column
  
  dbDisconnect(con)
  
  ##Convert date to date format
  #Move the start data to 1970-01-01 (sec:62135596800 since 01-01-0001)
  
  dataF$data$datetime<-dataF$data$datetime-62135596800
  dataF$data$datetime <- as.POSIXct(dataF$data$datetime,origin="1970-01-01 00:00:00", "GMT")
  
  #Startdate and download date
  idx <- match(c("startdatetime","downloaddatetime"),dataF$settings$settingName)
  dataF$settings$settingValue[idx] <- as.numeric(dataF$settings$settingValue[idx])*1.0e-7 - 62135596800
  dataF$settings$settingValue[idx] <- as.character(as.POSIXct(as.numeric(dataF$settings$settingValue[idx]),origin="1970-01-01 00:00:00", "GMT"))
  
  return(dataF)
  
}

#Aggregates the data given a new epoch
aggregation <- function(newEpoch, data){
  # Aggregates the data given a new epoch
  #
  # Args:
  #   newEpoch: New epoch value to aggregate the data.
  #   data: Data frame containing the accelerometry data
  #
  # Returns:
  #   A data frame aggregate in the new epoch. 
  
  currentEpoch <- as.numeric(data$datetime[2]-data$datetime[1],units="secs")
  epochRatio <- newEpoch/currentEpoch
  
  if(epochRatio == 1){
    return(data)
  }else if(epochRatio < 1){
    cat("\n Invalid request: ")
    cat(paste("\nCurrent epoch: ", currentEpoch," sec"))
    cat(paste("\nNew epoch: ", newEpoch," sec"))
  }
  else{
    
    #Create the groups
    grColumn <- as.vector(sapply(seq(1,ceiling(nrow(data)/epochRatio)), rep, epochRatio))
    grColumn <- grColumn[1:nrow(data)]
    data$groups <- grColumn
    
    #Extract the datetime for the aggregate data
    dtindex <- seq(1,nrow(data),epochRatio)
    datetime <- as.data.frame(data[dtindex,"datetime"])
    names(datetime) <- "datetime"
    
    if(length(data$incline)==0){
      data$incline <- -1
      if(!is.null(data$inclineOff)){
        data$incline <- data$inclineOff*0+ data$inclineStanding*1 + data$inclineLying*2 + data$inclineSitting*3
      }
    }
    
    if(length(data$lux)==0){
      data$lux <- -1
    }
    
    #Complete query (add activity and incline)
    notInclude <- c("datetime","inclineOff","inclineStanding","inclineSitting","inclineLying", "groups" )
    fcolumns <- names(data)[!(names(data)%in% notInclude)]
    fcolumns <- paste(fcolumns, collapse =", ")
    
    data4 <- data %>% tbl_df %>%
      select(one_of(fcolumns),groups) %>%
      group_by(groups) %>%
      summarise(axis1 = sum(axis1),
                axis2 = sum(axis2),
                axis3 = sum(axis3),
                steps = sum(steps),
                lux = mean(lux))
    
    dataInc <- data %>% tbl_df %>%
      select(one_of(fcolumns),groups) %>%
      group_by(groups,incline) %>%
      summarise(count = n()) %>% ungroup %>%
      group_by(groups) %>%
      filter(count == max(count)) %>%
      select(-count) %>%
      distinct() %>% ungroup
    
    data <- cbind(datetime, data4, dataInc)
    data$vm <- sqrt(data[,2]^2+data[,3]^2+data[,4]^2) #Vector magnitude
    data$day_m2m <- as.Date(data$datetime) #Day midnight-to-midnight (12:00am-11:59pm)
    data$day_n2n <- as.Date(data$datetime-60*60*12) #Day noon-to-noon (12:00pm-11:59am)
    
    return (data)
  }
}

#Identifies the non-wear periods 
nonWearPeriods <- function(data, innactivity = 60, tolerance = 2, scanParam = c("axis1","axis2","axis3","vm")){
  # Identifies the non-wear periods in the acceleromtry data given a scan parameter.
  # 
  # Args:
  #   data: Accelerometry data frame.
  #   innactivity: Minimum number of zero counts minutes to identify a non-wear period.
  #   tolerance: Number of minutes with counts per minute higher than zero within the 
  #               non-wear period.
  #   scanParam: Scan parameter to identify non-wear periods (eg. axis1, axis2, axis3, vm)
  #
  # Returns:
  #   A numeric matrix with a non-wear period per row. The first column is 
  #   the index of the first observation of the non-wear period and the second 
  #   column is the last observation of the non-wear period.
  #
  
  epoch <- as.numeric(data$datetime[2]-data$datetime[1],units="secs")
  inn <- innactivity*60 #min to sec
  tol <- tolerance *60 #min to sec
  nonWear <- matrix(nrow=0,ncol=2) #Matrix to store the start and end of non-wear periods
  
  cnonWear <- 0 #Counter
  i <- 1 #actual row
  
  #Column to identify non wear periods (ActiLife uses mv)
  if(scanParam=="axis1"){
    counts <- data$axis1 
  }else if(scanParam =="axis2"){
    counts <- data$axis2 
  }else if(scanParam == "axis3"){
    counts <- data$axis3
  }else if(scanParam == "vm"){
    counts <- sqrt((data$axis1)^2+(data$axis2)^2+(data$axis3)^2)
  }
  
  while(i <= nrow(data)){
    
    if(counts[i]==0){
      c<-0
      cnonWear<-0
      lstz <- 0 #lastzero
      newi <- 0 #new index i
      j<-i
      #While exists at least tol/epoch time units of activity (tolerance) -> counts
      #non-wear valid periods must be more than inn/epoch
      while(c<=tol/epoch && j <= nrow(data)){
        if(counts[j]==0){
          cnonWear<- cnonWear+1
          lstz <- j
        }else{
          cnonWear<- cnonWear+1
          c <- c+1
          newi <- newi + j*(c==1)
        }
        j <- j+1
      }
      if(lstz-i+1>=inn/epoch){
        nonWear <- rbind(nonWear, c(i,lstz)) #Save non-wear periods
        i <- lstz
      }
      else{
        if(newi != 0){
          i <- newi
        }
      }
      
    }
    i <- i+1
  }
  
  return(nonWear)
  
}


#Identify the sleep periods
sleepPeriods <- function(sleepOnsetHours= c(19,5), bedTimemin = 5,
                         minSleepTime = 160,
                         tolBsleepMatrix = matrix(c(0,24,10),1,3,byrow=T),
                         tolMatrix = matrix(c(0,5,20,
                                              5,19,10,
                                              19,24,20),3,3,byrow=T),
                         scanParam = c("axis1","axis2","axis3","vm"), data,
                         nonWearPSleep = F, nonWearInact = 90, nonWearTol = 2,
                         nonWearscanParam = c("axis1","axis2","axis3","vm"),
                         overlap_frac = 0.9){
  # Identifies the sleep periods in the acceleromtry data given a scan parameter for
  # the Sadeh et al. (1994) algorithm.
  # 
  # Args:
  #   sleepOnsetHours: Hour interval to start a valid sleep period.
  #   bedTimemin: Number of continous minutes of zero activity counts to start
  #                a sleep period.
  #   minSleepTime: Number of sleep minutes to identify a valid sleep period.
  #   tolBsleepMatrix: Matrix which contain the schedule of the allowed minutes   
  #                of non-zero activity before identifying a sleep period (<160).
  #   tolMatrix: Matrix which contain the schedule of the allowed minutes   
  #                of non-zero activity after identifying a sleep period(>=160).
  #   scanParam: Scan parameter to identify non-wear periods (eg. axis1, axis2, axis3, vm).
  #   data: Acelerometry data frame.
  #   nonWearPSleep: Logical. T, if the non-wear periods within sleep must be identified.
  #   nonWearInact: Minimum number of zero counts minutes to identify a non-wear period 
  #                 within sleep periods.
  #   nonWearTol: Number of minutes with counts per minute higher than zero within the 
  #               non-wear period.
  #   nonWearscanParam: Scan parameter to identify non-wear periods (eg. axis1, axis2, axis3, vm).
  #   overlap_frac: Overlap fraction to declare a sleep period as a non-wear period. If the 
  #                 overlap_frac of a sleep period is also classified as non-wear period, the sleep
  #                 sleep period is redefiened as non-wear.
  #
  # Returns:
  #   A list of matrices. 
  #     sleep: A numeric matrix with a sleep period per row. The first column is 
  #             the index of the first observation of the sleep period and the second 
  #             column is the last observation of the sleep period.
  #     sleepnw: A numeric matrix with a non-wear period per row. The first column is 
  #              the index of the first observation of the non-wear period and the second 
  #              column is the last observation of the non-wear period.
  #
  
  epoch <- as.numeric(data$datetime[2]-data$datetime[1],units="secs")
  
  newepoch <- 60 # New epoch to aggregate the data (60sec epochs are requiered to perform Sadeh's algorithm)
  data <- aggregation(newepoch,data)
  
  data$hour <- hour(data$datetime)
  data$sleep <- sadehAlgrtm(data, scanParam)
  #If the inclinometer value == 0,the minuted is labeled as sleep
  for(i in seq(nrow(data))){
    if(data$sleep[i]==0 && data$incline[i]==0){
      data$sleep[i] <- 1
    }
  }
  
  
  sleep <- matrix(nrow=0,ncol=2)    #Matrix to store the start and end of non-wear periods
  sleepnw <- matrix(nrow=0,ncol=2)  #Matrix to store the nonwear periods within sleep
  
  Sidx <- 0 #index
  i <- 1 #actual row
  
  while(i <= nrow(data)-(bedTimemin-1)){
    #First 5 consecutives minutes of sleep && sleep predefined interval between 7pm and 5:59am
    if(sum(data$sleep[i:(i+(bedTimemin-1))])==bedTimemin && (data$hour[i]>=sleepOnsetHours[1] || data$hour[i] <= sleepOnsetHours[2])){
      Sidx<-i
      cwake<-0      #tolerance
      cSleep<-0     #Sleep counter
      lst1 <- i     #lastzero
      newi <- 0     #new index i
      j<-i
      while( j <= nrow(data) && cwake<=ifelse(lst1-i+1<minSleepTime,
                                              waketimebyhour(data$hour[j], tolBsleepMatrix),
                                              waketimebyhour(data$hour[j], tolMatrix))){
        if(data$sleep[j]==1){
          cSleep<- cSleep+1
          cwake <- 0
          lst1 <- j
        }else{
          cSleep<- cSleep+1
          cwake <- cwake+1
          newi <- j
        }
        j <- j+1
      }
      if(lst1-i+1>=minSleepTime){
        sleep <- rbind(sleep, c(Sidx,lst1)) #Save non-wear periods
        i <- lst1
      }else{
        if(newi != 0){
          i <- newi
        }
      }
      
    }
    i <- i+1
  }
  
  #non-wear periods for sleep periods
  if(nonWearPSleep){
    rowstoremove <- vector()
    nwearPS <- nonWearPeriods(data,innactivity=nonWearInact, tolerance=nonWearTol, scanParam = nonWearscanParam)
    if(nrow(nwearPS)>0){
      for(rs in seq(1:nrow(sleep))){
        overlap_ratio <- 0 #Percentage of sleep period marked as nonWear period
        for(rnW in seq(1:nrow(nwearPS))){
          overlap_low <- max(c(sleep[rs,1],nwearPS[rnW,1]))
          overlap_upp <- min(c(sleep[rs,2],nwearPS[rnW,2]))
          
          overlap_epochs <- max(c(0,(overlap_upp - overlap_low+1)))
          overlap_ratio <- overlap_ratio + overlap_epochs/(sleep[rs,2]-sleep[rs,1]+1)     
        }
        
        if(overlap_ratio >= overlap_frac){
          sleepnw <- rbind(sleepnw,sleep[rs,])
          rowstoremove <- c(rowstoremove,rs) 
        }
      }
    }
    if(length(rowstoremove)>0){
      sleep <- sleep[-rowstoremove,]
    }
  } 
  sleeplist <- list()
  sleeplist$sleep <- sleep
  sleeplist$sleepnw <- sleepnw
  
  return(sleeplist)
}

#Classifies the nigth minutes if sleep or not (axis1)
sadehAlgrtm <- function(data, scanParam = c("axis1","axis2","axis3","vm")){
  # Classifies each minute if it is a sleep (1) minute or not (0).
  # 
  # Args:
  #   data: Accelerometry data frame
  #   scanParam: Scan parameter to identify the sleep minutes (eg. axis1, axis2, axis3, vm)
  #
  # Results:
  #   A numeric vector which contains the a flag for each minute in the data set.
  #     1, if the minute is classified as sleep.
  #     0, otherwise.
  #
  
  wlength <- 11 #Window length
  s <- (wlength-1)/2 #half window length
  sleep <- rep(0,nrow(data)) #Initially, all minues are not sleep minutes
  
  #Column to identify sleep periods 
  if(scanParam=="axis1"){
    counts <- data$axis1 
  }else if(scanParam =="axis2"){
    counts <- data$axis2 
  }else if(scanParam == "axis3"){
    counts <- data$axis3
  }else if(scanParam == "vm"){
    counts <- sqrt((data$axis1)^2+(data$axis2)^2+(data$axis3)^2)
  }
  
  for(i in seq((s+1),nrow(data)-s)){ #starts in obs 6 because it is the first window
    meanW5min <- mean(counts[(i-s):(i+s)])
    NAT <- sum(counts[(i-s):(i+s)]>=50 & counts[(i-s):(i+s)]<100)
    SDlst6min <- sd(counts[(i-s):i])
    LogAct <- log(counts[i]+1)
    
    PS <- 7.601 - 0.065*meanW5min - 1.08*NAT - 0.056*SDlst6min - 0.703*LogAct
    
    if(PS>=0){
      sleep[i] <- 1
    }
    
  }
  return(sleep)
}

#Returns the waketime minutes to the corresponding hour
waketimebyhour <- function(hour, schedule){
  # Suplementary function for the sleep function. Given an hour an a
  # schedule matrix returns number of minutes to declare a the end of
  # a sleep period.   
  #
  # Args:
  #   hour: Numeric value.
  #   schedule: Matrix with the following format: 
  #          - col1: start hour 
  #          - col2: end hour (not included)
  #          - col3: number of minutes to declare the end of a sleep period.
  #
  # Returns:
  #   Number of minutes to declare the end of a sleep period according to the hour.
  #
  
  for(r in seq(1,nrow(schedule))){
    if(schedule[r,1]<=hour && schedule[r,2]>hour){
      return(schedule[r,3])
    }
  }
}

#Delete data (days,hours,minutes,seconds)
removeData <- function(data, toremove, units = c("days","hours","minutes","seconds")){
  # Remove specific data from the dataframe.
  # 
  # Args:
  #   data: Accelerometry data frame.
  #   toremove: Vector of days, hours, minutes or seconds to be remove from de data frame 
  #             (e.g. days: c(2013-10-02, 2013-10-03); hours: c(10,11,12,13), etc)
  #   units: Units of the vector "toremove"
  #   
  # Returns: 
  #   A subset of the original data frame.
  # 
  
  if(units == "days"){
    tmp <- as.Date(data$datetime)
  }else if(units == "hours"){
    tmp <- hours(data$datetime)
  }else if(units == "minutes"){
    tmp <- minutes(data$datetime)
  }else if(units == "seconds"){
    tmp <- seconds(data$datetime)
  }else{
    print("Wrong time units")
    print("Valid time units: 'days','hour','minutes','seconds'")
  }
  data <- subset(data, subset = !(tmp%in%toremove) )
  return(data)
}

#Classifies the counts according Evenson cut points for physical activity acording to the epoch
cut_points_evenson <- function(counts, epoch=15){
  # Classifies the counts according Evenson et al. (2008) cut points for physical activity 
  # acording to the epoch. Evenson proposed his thresholds in 15 sec intervals.
  # This function recives the epoch of the data and adjust the thresholds.
  # 
  # Args:
  #   counts: Number of counts to be classified
  #   epoch:  Accelerometry data epoch
  #
  # Retruns:
  #   A string, the physical activity intensity level for the counts value.
  #   (i.e. sedentary, light, moderate or vigorous)
  #
  
  if(counts <= 25*(epoch/15)){
    return("sedentary")  
  }else if(counts <= 573*(epoch/15)){
    return("light")
  }else if(counts <= 1002*(epoch/15)){
    return("moderate")
  }else{
    return("vigorous")
  }
}

#Classifies the counts according Evenson cut points for physical activity acording to the epoch
cut_points_freedsonAdults1998 <- function(counts, epoch=60){
  # Classifies the counts according Freedson et al. (1998) cut points for physical activity 
  # acording to the epoch for adults. Freedson proposed his thresholds in 60 sec intervals.
  # This function recives the epoch of the data and adjust the thresholds.
  # 
  # Args:
  #   counts: Number of counts to be classified
  #   epoch:  Accelerometry data epoch
  #
  # Retruns:
  #   A string, the physical activity intensity level for the counts value.
  #   (i.e. sedentary, light, moderate or vigorous)
  #
  
  if(counts <= 1953*(epoch/60)){
    return("sedentary/light")  
  }else if(counts <= 5725*(epoch/60)){
    return("moderate")
  }else if(counts <= 9499*(epoch/60)){
    return("vigorous")
  }else if(counts <= 20000*(epoch/60)){
    return("very vigorous")
  }else{
    return("invalid")
  }
}




