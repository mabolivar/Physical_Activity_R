#==============================================================================
# Working directory
#==============================================================================

setwd("C:/Users/Manuel/Dropbox/2. Uniandes Projects/Accelerometria/MARA")

#==============================================================================
# Required libraries
#==============================================================================

library("RSQLite")
library("sqldf")
library("xlsx")
library("Hmisc")
library("scales")
source(".\\Physical_Activity_R\\ADPP.R")


#==============================================================================
# Functions
#==============================================================================

##Compares the dates in the .agd file against the PACK
qualityControlCheck <- function(dbDir,data, settings, pack){
  #Compares the dates/serial in the .agd file against the PACK
  #
  # Args:
  #   dbDir: The name of the file which the data was read from.
  #   data: The data frame with accelerometry data (datetime, axis1, axis2, axis3, etc)
  #   settings: The data frame constaining the accelerometer settings parameters
  #   pack: The data frame containing the participant checklist to validate the information
  # Returns:
  #   Logical. True, if the info in the data.frame is consistent with th PACK
  
  #Auxiliar var. True, if the info in the data.frame is consistent with th PACK
  valid <- TRUE 
  
  #Extract the participant id, first or second use (A or B), accelerometer serial and download date
  m<-regexec("\\\\[[:print:]]+\\\\([[:digit:]]+)([A-Z])([[:digit:]]).([[:digit:]]+).([[:digit:]]+)",dbDir)
  use <- regmatches(dbDir, m)[[1]][3] #If it is the first time of use -> A, else B.
  id <- regmatches(dbDir, m)[[1]][2] #Participant ID
  acserial <- regmatches(dbDir, m)[[1]][5] #Accelerometer serial number
  downloadDay <- regmatches(dbDir, m)[[1]][6] #Download day
  
  row <- match(id,table=pack$ID.Participante,nomatch=-1) #row in the PACK
  days <- unique(as.Date(data$datetime)) 
  
  packserial <- ifelse(use == "A",pack[row, 1],pack[row, 9])
  epoch <- data[2,1]-data[1,1]
  
  if(acserial != packserial){
    cat("\nThe serial contained in the pack mismatch the serial in the .agd file")
    valid <- FALSE
  }
  
  if(row == -1){
    cat(paste("\nParticipant ",as.character(id)," doesn't have a correponding record in the PACK", sep=" "))
    valid = FALSE
  }else{
    #Compare the serial in the .agd file with the serial in the name file
    dsindx <- match("deviceserial",settings$settingName)
    if(acserial != substr(settings$settingValue[dsindx], 9,13)){
      cat("\nThe serial contained in the file name mismatch the serial in the .agd file")
      valid <- FALSE
    }
    
    if(min(days)!=ifelse(use == "A",pack[row, 2],pack[row, 10])){
      cat("\nInitialization day mismatch the initialization day in the pack")
      cat(paste("\n\tInitialization day .agd: ",min(days), sep=" "))    
      cat(paste("\n\tInitialization day PACK: ",(as.Date("1970-01-01")+ifelse(use == "A",pack[row, 2],pack[row, 10])), sep=" "))
      valid <- FALSE
    }
    if(max(days)<=ifelse(use == "A",pack[row, 2],pack[row, 10])+6){
      cat("\nRetrieval day is within the 7 days of data collection")
      cat(paste("\n\tRetrieval day .agd: ",max(days), sep=" "))
      cat(paste("\n\tRetrieval day PACK: ",(as.Date("1970-01-01")+ifelse(use == "A",pack[row, 6],pack[row, 14])), sep=" ")) 
      valid = FALSE
    }
  }
  return (valid) 
  
}

#Checks for unusual values in wear labeled observation 
checkWearPeriods <- function(data,maxc = 20000){
  # Checks for unusual values in wear labeled observation and
  # classifies the first minutes of the first day as sleep (maximum the first 20 minutes)
  #
  # Args:
  #   data: The data frame with accelerometry data (datetime, axis1, axis2, axis3, etc) plus the activity column.
  #   maxc: Maximum count limit per epoch.
  #
  # Returns:
  #   The data frame with all the accelerometry data plus the updated activity column.
  
  i <- 1
  while(data$activity[i]=="wear" && i <= 160 ){
    data$activity[i]<-"sleep"
    i <- i+1
  }
  for( i in seq(2:(nrow(data)-1))){
    if(data$axis1[i]>maxc){
      if(data$activity[i-1]=="sleep" || data$activity[i+1]=="sleep"){
        data$activity[i]="sleep"
      }else{
        data$activity[i]="non-wear"
      }
    }
  }
  
  return(data)
  
}

#Set activity in each epoch
setActivitySNW <- function(x,label,intv, minlength=20){
  # Labels the minutes within the intervals parameter (intv) in the activity column
  #
  # Args:
  #   x: Activity vector
  #   l: label to classify the epochs
  #   intv: Matrix in which each rows is an interval to be labeled 
  #   minlength: Minimum number of uninterrupted epochs labeled as "wear" to be relabeled with l value.
  #
  # Returns:
  #   The updated activity column.
  
  intv<-matrix(intv,ncol=2)
  if(nrow(intv) > 0){
    for(rs in seq(1:nrow(intv))){
      l <- intv[rs,1]
      u <- intv[rs,2]
      if(sum(x[l:u] =="wear")>=minlength){
        x[l:u] <- ifelse(x[l:u]=="wear",label, x[l:u])
      }
    }
  }
  return(x)
}

#Left joins the 15sec data frame with 60secc data frame (i.e. adds data15 data frame the activity column)
mergingActivity <- function(data15,data60){
  # Left joins the 15sec data frame with 60secc data frame (i.e. adds data15 data frame the activity column)
  #
  # Args:
  #   data15: Accelerometry data frame agreggated in 15 sec epochs
  #   data60: Accelerometry data frame agreggated in 60 sec epochs plus the activity column ("sleep","non-wear","wear")
  #
  # Returns:
  #   Accelerometry data frame agreggated in 15 plus the activity column ("sleep","non-wear","wear").
  
  options(warn=-1)
  data15$minute <- tryCatch(as.POSIXct(trunc.POSIXt(data15$datetime,units ="mins")))
  options(warn=0)
  
  q <- "SELECT d15.datetime, d15.axis1, d15.axis2, d15.axis3, d15.steps, 
        d15.lux, d15.incline, d60.activity,d15.day_n2n, d15.day_m2m
        FROM data15 as d15 LEFT JOIN data60 as d60
        ON d15.minute = d60.datetime"
  data15 <- (sqldf(q, method ="raw"))
  data15$datetime <- as.POSIXct(data15$datetime,origin="1970-01-01 00:00:00", tz = "GMT")
  data15$day_n2n <- as.Date(data15$day_n2n,origin="1970-01-01 00:00:00")
  data15$day_m2m <- as.Date(data15$day_m2m,origin="1970-01-01 00:00:00")
  return(data15)
  
}

#Extracts a substring from a string, starting from the left-most character.
substrRight <- function(x, n){
  # Extracts a substring from a string, starting from the left-most character.
  # Args:
  #   x:The string that you wish to extract from.
  #   n: The number of characters that you wish to extract starting from the left-most character.
  # 
  # Returns:
  #   Substring with n characters.
  
  substr(x, nchar(x)-n+1, nchar(x))
}

createQueryIntensity <- function(colName = "intensityEV", intensities = c("moderate","vigorous"), dayType = "all",tomin = 4, q_data){
  # Builds a SQL query to extract the average day minutes and counts for a given intensity from a given query "q_data".
  # Args:
  #   colName: Column which contains the physical intensity levels (i.e. vigorous, moderate, light and sedentary)
  #   intensities: Physical intensity levels to be taking into account for this query.
  #   dayType: Prefix which indicates what days are been used to build the query. This must match with the q_query. 
  #   tomin: The ratio between the desired output epoch and the actual epoch (e.g. If the actual epoch is 15sec and the desired epoch is 60sec then tomin=60/15=4)
  #   q_data: the data frame/query containing the information to be treated. (e.g. midweek acceleromtry data, weekend accelerometry data, alldays accelerometry data)
  # 
  # Returns:
  #   Query string.
  cutsname <- substrRight(colName,2)
  WHEREcond <- sapply(intensities, FUN = function(i) (paste("(intensity",cutsname," = '",i,"')",sep = "")))
  WHEREcond <- paste(WHEREcond, collapse = " OR ")
  
  ints_name <- sapply(intensities, substr,1,1)
  ints_name <- paste(ints_name, collapse = "", sep ="")
  
  q <- paste("SELECT avg(duration",cutsname,") as ",dayType,"mean_",ints_name,"_",cutsname,", avg(countsEV) as ",dayType,"mean_cnt",ints_name,"_",cutsname,"
                FROM
                (SELECT day_m2m, count(axis1)*1.0/",tomin," as duration",cutsname,", sum(axis1) as counts",cutsname," 
                FROM (",q_data,") 
                WHERE ",WHEREcond," 
                GROUP BY day_m2m)", sep ="")
  return(q)
}

# Builds a set of queries to extract physical activity related variables given a accelerometry data frame.
getobs <- function(dbDir, data, timeunit = "min"){
  # Builds a set of queries to extract physical activity related variables given a accelerometry data frame.
  # Args:
  #   dbDir: The name of the file which the data were read from.
  #   data: Accelerometry data frame plus activity and phisical intensity columns.
  #   timeunit: Time units for the time-derived variables. 
  # 
  # Returns:
  #   A data frame. 
  
  #Default time unit
  tu <- 60
  if(timeunit =="sec"){
    tu <- 1
  }else if(timeunit == "min"){
    tu <- 60
  }else if(timeunit == "hour"){
    tu <- 3600
  }else{
    print("Wrong time unit. Valid parameters: 'sec','min','hour'")
    print("default time unit:'min'")
  }
  
  epoch <- as.double(data[2,1])-as.double(data[1,1])
  tomin <- as.numeric(tu/epoch)*1.0
  data$weekday <- weekdays(data$day_m2m, abbreviate=T)
  
  saturday <- weekdays(as.Date(c("2013-07-13")),abbreviate=T)
  saturday <- as.data.frame(saturday)
  sunday <- weekdays(as.Date(c("2013-07-14")),abbreviate=T)
  sunday <- as.data.frame(sunday)
  weekend <- weekdays(as.Date(c("2013-07-13","2013-07-14")),abbreviate=T) #Saturday and sunday
  
  midweek <- unique(data$weekday)
  midweek <- subset(midweek, subset= !(midweek%in%weekend) )
  weekend <- as.data.frame(weekend)
  midweek <- as.data.frame(midweek)
  
  
  #Extract the participant id
  m<-regexec("\\\\[[:print:]]+\\\\([[:digit:]]+)([A-Z])([[:digit:]]).([[:digit:]]+).([[:digit:]]+)",dbDir)
  PID <- regmatches(dbDir, m)[[1]][2]
  PID <- as.data.frame(PID)
  Measure <- regmatches(dbDir, m)[[1]][4]
  Measure <- as.data.frame(Measure)
  use <- regmatches(dbDir, m)[[1]][3]
  use <- as.data.frame(use)
  
  #Query: Wearing time (min) per day
  q_wtpd <- paste("SELECT day_m2m, count(axis1)*1.0/",tomin," as wearTime, weekday 
                  FROM data WHERE activity = 'wear'
                  GROUP BY day_m2m", sep="")
  
  #Query: valid days
  q_vd <- paste("SELECT day_m2m, weekday FROM (",q_wtpd,") WHERE wearTime >= 600")
  
  #Query: number of valid days
  q_nvd <- paste("SELECT count(day_m2m) as valdays FROM (",q_vd,")")
  
  #Query: number of valid days (midweek)
  q_nvd_wk <- paste("SELECT count(day_m2m) as valwkdays FROM (",q_vd,"), midweek WHERE weekday = midweek")
  
  #Query: number of valid days (weekend)
  q_nvd_wd <- paste("SELECT count(day_m2m) as valwkend FROM (",q_vd,"), weekend WHERE weekday = weekend")
  
  
  #Extract only obs classified as wear and valid
  q_wearobs <- paste("SELECT * FROM data as d JOIN (",q_vd,") as vd 
                     WHERE d.activity = 'wear' AND d.day_m2m = vd.day_m2m ")
  q_sleepNWobs <- paste("SELECT * FROM data as d JOIN (",q_vd,") as vd 
                        WHERE (d.activity = 'sleep' OR d.activity = 'non-wear') AND d.day_m2m = vd.day_m2m ")
  
  dt <- "all"
  
  #Query: mean wake/wear time per day
  q_MeanWakeWear <- paste("SELECT avg(Time) as ", dt,"MeanWakeWear 
                          FROM
                          (SELECT day_m2m, count(axis1)*1.0/",tomin," as Time, weekday FROM
                          (",q_wearobs,") GROUP BY day_m2m)",sep = "")
  
  #Query: mean sleep and nonwear time per day
  q_MeanSleepNW <- paste("SELECT avg(Time) as ", dt,"MeanSleepNW 
                         FROM
                         (SELECT day_m2m, count(axis1)*1.0/",tomin," as Time, weekday FROM
                         (",q_sleepNWobs,") GROUP BY day_m2m)",sep = "")
  
  #Queries: physical activity intensities
  
  q_mv <- createQueryIntensity(colName = "intensityEV", intensities = c("moderate","vigorous"),dayType=dt,tomin, q_data=q_wearobs)
  
  q_v <- createQueryIntensity(colName = "intensityEV", intensities = c("vigorous"),dayType=dt,tomin, q_data=q_wearobs)
  
  q_m <- createQueryIntensity(colName = "intensityEV", intensities = c("moderate"),dayType=dt,tomin, q_data=q_wearobs)
  
  q_l <- createQueryIntensity(colName = "intensityEV", intensities = c("light"),dayType=dt,tomin, q_data=q_wearobs)
  
  q_s <- createQueryIntensity(colName = "intensityEV", intensities = c("sedentary"),dayType=dt,tomin, q_data=q_wearobs)
  
  #Query: Mean daily total intensity counts
  q_MeanActCounts <- paste("SELECT avg(Counts) as ", dt,"MeanActCounts 
                           FROM
                           (SELECT day_m2m, sum(axis1) as Counts, weekday FROM
                           (",q_wearobs,") GROUP BY day_m2m)",sep = "")
  
  #Query: Mean intensity count per minute
  q_MeanIntenPerMin <- paste("SELECT avg(axis1)*1.0*",tomin," as ", dt,"MeanIntenPerMin FROM
                             (",q_wearobs,")",sep = "")
  
  
  
  #Extract only obs classified as wear and valid (MIDWEEK)
  q_wearobs <- paste("SELECT * 
                     FROM midweek as w, data as d JOIN (",q_vd,") as vd 
                     WHERE d.activity = 'wear' AND d.day_m2m = vd.day_m2m AND (d.weekday = w.midweek)")
  q_sleepNWobs <- paste("SELECT * 
                        FROM midweek as w,data as d JOIN (",q_vd,") as vd 
                        WHERE (d.activity = 'sleep' OR d.activity = 'non-wear') 
                        AND (d.day_m2m = vd.day_m2m) AND (d.weekday = w.midweek) ")
  
  dt <- "WKDAY"
  
  #Query: mean wake/wear time per day
  q_MeanWakeWear_wk <- paste("SELECT avg(Time) as ", dt,"MeanWakeWear 
                             FROM
                             (SELECT day_m2m, count(axis1)*1.0/",tomin," as Time, weekday FROM
                             (",q_wearobs,") GROUP BY day_m2m)",sep = "")
  
  #Query: mean sleep and nonwear time per day
  q_MeanSleepNW_wk <- paste("SELECT avg(Time) as ", dt,"MeanSleepNW 
                            FROM
                            (SELECT day_m2m, count(axis1)*1.0/",tomin," as Time, weekday FROM
                            (",q_sleepNWobs,") GROUP BY day_m2m)",sep = "")
  
  
  #Queries: physical activity intensities (MIDWEEK)
  q_mv_wk <- createQueryIntensity(colName = "intensityEV", intensities = c("moderate","vigorous"),dayType=dt, tomin,q_data=q_wearobs)
  
  q_v_wk <- createQueryIntensity(colName = "intensityEV", intensities = c("vigorous"),dayType=dt, tomin,q_data=q_wearobs)
  
  q_m_wk <- createQueryIntensity(colName = "intensityEV", intensities = c("moderate"),dayType=dt, tomin,q_data=q_wearobs)
  
  q_l_wk <- createQueryIntensity(colName = "intensityEV", intensities = c("light"),dayType=dt, tomin,q_data=q_wearobs)
  
  q_s_wk <- createQueryIntensity(colName = "intensityEV", intensities = c("sedentary"),dayType=dt, tomin,q_data=q_wearobs)
  
  #Query: Mean daily total intensity counts (midweek)
  q_MeanActCounts_wk <- paste("SELECT avg(Counts) as ", dt,"MeanActCounts 
                              FROM
                              (SELECT day_m2m, sum(axis1) as Counts, weekday FROM
                              (",q_wearobs,") GROUP BY day_m2m)",sep = "")
  
  #Query: Mean intensity count per minute (midweek)
  q_MeanIntenPerMin_wk <- paste("SELECT avg(axis1)*1.0*",tomin," as ", dt,"MeanIntenPerMin FROM
                                (",q_wearobs,")",sep = "")
  
  
  #Extract only obs classified as wear and valid (WEEKEND)
  q_wearobs <- paste("SELECT * 
                     FROM weekend as w, data as d JOIN (",q_vd,") as vd 
                     WHERE d.activity = 'wear' AND d.day_m2m = vd.day_m2m AND (d.weekday = w.weekend)")
  
  q_sleepNWobs <- paste("SELECT * FROM weekend as w, data as d JOIN (",q_vd,") as vd 
                        WHERE (d.activity = 'sleep' OR d.activity = 'non-wear') 
                        AND (d.day_m2m = vd.day_m2m) AND (d.weekday = w.weekend) ")
  
  dt <- "WKEND"
  #Query: mean wake/wear time per day
  q_MeanWakeWear_wd <- paste("SELECT avg(Time) as ", dt,"MeanWakeWear 
                             FROM
                             (SELECT day_m2m, count(axis1)*1.0/",tomin," as Time, weekday FROM
                             (",q_wearobs,") GROUP BY day_m2m)",sep = "")
  
  #Query: mean sleep and nonwear time per day
  q_MeanSleepNW_wd <- paste("SELECT avg(Time) as ", dt,"MeanSleepNW 
                            FROM
                            (SELECT day_m2m, count(axis1)*1.0/",tomin," as Time, weekday FROM
                            (",q_sleepNWobs,") GROUP BY day_m2m)",sep = "")
  
  #Queries: physical activity intensities (WEEKEND)
  q_mv_wd <- createQueryIntensity(colName = "intensityEV", intensities = c("moderate","vigorous"),dayType=dt, tomin,q_data=q_wearobs)
  
  q_v_wd <- createQueryIntensity(colName = "intensityEV", intensities = c("vigorous"),dayType=dt, tomin,q_data=q_wearobs)
  
  q_m_wd <- createQueryIntensity(colName = "intensityEV", intensities = c("moderate"),dayType=dt, tomin,q_data=q_wearobs)
  
  q_l_wd <- createQueryIntensity(colName = "intensityEV", intensities = c("light"),dayType=dt, tomin,q_data=q_wearobs)
  
  q_s_wd <- createQueryIntensity(colName = "intensityEV", intensities = c("sedentary"),dayType=dt, tomin,q_data=q_wearobs)
  
  #Query: Mean daily total intensity counts (weekend)
  q_MeanActCounts_wd <- paste("SELECT avg(Counts) as ", dt,"MeanActCounts 
                              FROM
                              (SELECT day_m2m, sum(axis1) as Counts, weekday FROM
                              (",q_wearobs,") GROUP BY day_m2m)",sep = "")
  
  #Query: Mean intensity count per minute (weekend)
  q_MeanIntenPerMin_wd <- paste("SELECT avg(axis1)*1.0*",tomin," as ", dt,"MeanIntenPerMin FROM
                                (",q_wearobs,")",sep = "")
  
  
  
  #Extract only obs classified as wear and valid (SUNDAY)
  q_wearobs <- paste("SELECT * 
                     FROM sunday as s, data as d JOIN (",q_vd,") as vd 
                     WHERE d.activity = 'wear' AND d.day_m2m = vd.day_m2m AND (d.weekday = s.sunday)")
  
  q_sleepNWobs <- paste("SELECT * FROM sunday as s, data as d JOIN (",q_vd,") as vd 
                        WHERE (d.activity = 'sleep' OR d.activity = 'non-wear') 
                        AND (d.day_m2m = vd.day_m2m) AND (d.weekday = s.sunday) ")
  dt <- "SUN"
  #Query: mean wake/wear time per day
  q_MeanWakeWear_s <- paste("SELECT avg(Time) as ", dt,"MeanWakeWear 
                             FROM
                             (SELECT day_m2m, count(axis1)*1.0/",tomin," as Time, weekday FROM
                             (",q_wearobs,") GROUP BY day_m2m)",sep = "")
  
  #Query: mean sleep and nonwear time per day
  q_MeanSleepNW_s <- paste("SELECT avg(Time) as ", dt,"MeanSleepNW 
                            FROM
                            (SELECT day_m2m, count(axis1)*1.0/",tomin," as Time, weekday FROM
                            (",q_sleepNWobs,") GROUP BY day_m2m)",sep = "")
  
  #Queries: physical activity intensities (WEEKEND)
  q_mv_s <- createQueryIntensity(colName = "intensityEV", intensities = c("moderate","vigorous"),dayType=dt, tomin,q_data=q_wearobs)
  
  q_v_s <- createQueryIntensity(colName = "intensityEV", intensities = c("vigorous"),dayType=dt, tomin,q_data=q_wearobs)
  
  q_m_s <- createQueryIntensity(colName = "intensityEV", intensities = c("moderate"),dayType=dt, tomin,q_data=q_wearobs)
  
  q_l_s <- createQueryIntensity(colName = "intensityEV", intensities = c("light"),dayType=dt, tomin,q_data=q_wearobs)
  
  q_s_s <- createQueryIntensity(colName = "intensityEV", intensities = c("sedentary"),dayType=dt, tomin,q_data=q_wearobs)
  
  #Query: Mean daily total intensity counts (weekend)
  q_MeanActCounts_s <- paste("SELECT avg(Counts) as ", dt,"MeanActCounts 
                              FROM
                              (SELECT day_m2m, sum(axis1) as Counts, weekday FROM
                              (",q_wearobs,") GROUP BY day_m2m)",sep = "")
  
  #Query: Mean intensity count per minute (weekend)
  q_MeanIntenPerMin_s <- paste("SELECT avg(axis1)*1.0*",tomin," as ", dt,"MeanIntenPerMin FROM
                                (",q_wearobs,")",sep = "")
  
  
  
  allqueries <- c(q_nvd,q_nvd_wk,q_nvd_wd,
                  q_MeanWakeWear,q_MeanSleepNW,q_mv,q_v,q_m,q_l,q_s,q_MeanActCounts,q_MeanIntenPerMin,
                  q_MeanWakeWear_wk,q_MeanSleepNW_wk,q_mv_wk,q_v_wk,q_m_wk,q_l_wk,q_s_wk,q_MeanActCounts_wk,q_MeanIntenPerMin_wk,
                  q_MeanWakeWear_wd,q_MeanSleepNW_wd,q_mv_wd,q_v_wd,q_m_wd,q_l_wd,q_s_wd,q_MeanActCounts_wd,q_MeanIntenPerMin_wd,
                  q_MeanWakeWear_s,q_MeanSleepNW_s,q_mv_s,q_v_s,q_m_s,q_l_s,q_s_s,q_MeanActCounts_s,q_MeanIntenPerMin_s)
  
  allqueries <- sapply(allqueries, FUN = function(i) (paste("(",i,")", sep ="")))
  allqueries <- paste(allqueries, collapse=" , ", sep=" , ")
  q_full <- paste("SELECT *  FROM", allqueries)
  
  obs <- sqldf(q_full)
  valid <- ifelse(obs$valwkdays>=3 && obs$valwkend>=1,1,0)
  valid <- as.data.frame(valid)
  obs <- cbind(Measure,PID,use, valid, obs)
  
  return(obs)
  
  q<- "SELECT day_m2m, day_n2n, count(axis1) as MeanSleepNW FROM data 
  WHERE activity = 'sleep' GROUP BY day_n2n"
  q<- "SELECT day_m2m, day_n2n, count(axis1) as MeanSleepNW FROM data 
  WHERE activity = 'non-wear' GROUP BY day_n2n"
  
  
}


#Read PACK
readPack <- function(name,sheets){
  # Read the xlsx file of the participant checklist (PACK)
  # Args:
  #   name: The name of the file which the data is read from (Complete file path).
  #   sheets: Workbook sheets to be readed.
  # 
  # Returns:
  #   A data frame with the pack information.
  
  pack <- data.frame()
  
  for(sheet in sheets){
    pack <- rbind(pack, (read.xlsx(name,sheetName=sheet,colClasses= "character",
                                   startRow=2,as.data.frame=T,
                                   stringsAsFactors=F)))
  }
  
  #Extract the last digits of the device serial
  pack[,1]<- substr(pack[,1], 1, 5) 
  return(pack)
}  

#=============================================================================
# Main routine
#=============================================================================

inputdir <- ".\\data_MARA"
outputdir <- ".\\output"
outputfile <- paste("COL PA MARA var_",as.Date(Sys.time()),".csv", sep="")
packdir <- "PACK-MARA.xlsx"
sheets <- c("COLEGIO 20 DE JULIO", "COLEGIO MANUELITA SAENZ",
            "COLEGIO MONTEBELLO", "ISCOLE")

d <- "2103A3_10503_011120131sec.agd"
d <- "2210A3_10500_051120131sec.agd"
d <- "2228A3_10459_121120131sec.agd"
d <- "2227A3_10476_051120131sec.agd"
d <- "1117A3_10463_231020131sec.agd"

main <- function(){
  
  #Time
  tnow <- Sys.time()   
  #Final data set
  adata <- data.frame()
  #Get datafiles names
  dataFiles <- dir(inputdir)
  
  #Read the PACK
  pack <- readPack(packdir,sheets)  
  
  #Open Log output
  sink(paste(outputdir,"\\log.txt",collapse="",sep=""))
  cat("Log output:\n")  
  
  for (d in dataFiles){
    #Database location
    dbDir <- paste(inputdir,"\\",d,sep="")
    cat(d)
    
    #Checks file size
    if(file.info(dbDir)$size/1000<8000){
      cat("...........Wrong file size\n")
    }else{
      #0. Read data form the .agd files (SQLite database)
      db <- readDatabase(dbDir)
      data <- db$data
      settings <- db$settings
      
      #1. Quality control checks
      valid <- qualityControlCheck(dbDir,data, settings, pack)
      epoch <- as.numeric(data[2,1]-data[1,1], units="secs")
      
      if(epoch == 1){
        #2 Data aggregation
        data15 <- aggregation(15, data)
        data60 <- aggregation(60, data)
        data15$activity <- rep("wear",nrow(data15)) #Activity (sleep, non-wear, wear/wake)
        data60$activity <- rep("wear",nrow(data60)) #Activity (sleep, non-wear, wear/wake)
        
        #2.5. Cleaning (Remove last day of data)
        udays <- unique(as.Date(data60$datetime))
        lastday <- max(udays)
        data15 <- removeData(data15,toremove=c(lastday),units="days") 
        data60 <- removeData(data60,toremove=c(lastday),units="days") 
        
        #3. Sleep period
        sleep <- sleepPeriods(data=data60,sleepOnsetHours= c(19,5), bedTimemin = 5,
                              tolBsleepMatrix = matrix(c(0,24,10),1,3,byrow=T),
                              tolMatrix = matrix(c(0,5,20,
                                                   5,19,10,
                                                   19,24,20),3,3,byrow=T),
                              minSleepTime = 160, scanParam = "axis1",
                              nonWearPSleep = T, nonWearInact = 90, nonWearTol = 2,
                              nonWearscanParam = "axis1",
                              overlap_frac = 0.9)
        
        data60$activity <- setActivitySNW(data60$activity,label="sleep",intv=sleep$sleep,minlength = 20)
        data60$activity  <- setActivitySNW(data60$activity,label="non-wear",intv=sleep$sleepnw, minlength = 20)
        
        #4. Non-wear period
        nWP <- nonWearPeriods(data60, scanParam="axis1",innactivity=20,tolerance=0) #nonWearPeriods. Innactivity and tolerance in minutes
        data60$activity  <- setActivitySNW(data60$activity,label="non-wear",nWP, minlength = 20)
        
        #5. Wear periods
        data60 <- checkWearPeriods(data60,maxc = 20000)
        
        #6. Cleaning (Remove last day of data and more than 7 days of data)
        udays <- unique(as.Date(data60$datetime))
        firstday <- min(udays)
        lastday <- max(udays)
        validdays <- seq(firstday,firstday+6,by=1) 
        daystoremove <- udays[!(udays%in%validdays)]
        data15 <- removeData(data15,toremove=c(daystoremove),units="days") 
        data60 <- removeData(data60,toremove=c(daystoremove),units="days") 
        
        #7. Add intensity physical activity
        data15 <- mergingActivity(data15,data60)
        data15$intensityEV <- mapply(cut_points_evenson,epoch=15, data15$axis1)
        data60$intensityEV <- mapply(cut_points_evenson,epoch=60, data60$axis1)
        
        #8. Get the datafile observation for the final data frame(only wear periods)
        ob <- getobs(dbDir,data15, timeunit="min")
        
  		  #Copies the final data frame in the clipboard
        #write.csv(ob,file="clipboard", row.names=F)
        adata<-rbind(adata,ob)
        write.csv(adata,file=paste(outputdir,"\\",outputfile, sep="",collapse=""), row.names=F)
        
        if(valid==T){
          cat("........OK\n")
        }else{
          cat("........INVALID\n")
        }
        
      }else{
        cat("........SKIPPED (Wrong epoch) \n")
      }
    }
  }
  
  
  sink()
  write.csv(adata,file=paste(outputdir,"\\",outputfile, sep="",collapse=""), row.names=F)
  
  print(paste("Total time:", as.numeric(Sys.time()-tnow,units="mins")," mins"))
}

#=============================================================================

tryCatch(main(),finally=sink())

#=============================================================================

data60$activity <- as.factor(data60$activity)
data60$intensityEV <- as.factor(data60$intensityEV) 
data60$weekday <- weekdays(data60$datetime)
data60$hour <- hours(data60$datetime)
data60$time <- times(substr(as.character(as.chron(data60$datetime)),11,18))


data.frame(unique(data60$day_m2m),weekdays(unique(data60$day_m2m)))

table(data60$activity, weekdays(data60$day_m2m))

par(mfrow=c(1,1))
n <- 1
smoothed <- c()
for(i in 1:nrow(data60)){
  smoothed <- c(smoothed,mean(data60$axis1[(i-n):(i+n)]))
}
data60$smooth <- smoothed

stime <- times("7:00:00")
etime <- times("13:00:00")
day1 <- "jueves"
day2 <- "viernes"
dd <- subset(data60, data60$hour >= hours(stime) & data60$hour<hours(etime) & data60$weekday == day1)
dd2 <- subset(data60, data60$hour >=hours(stime) & data60$hour<hours(etime) & data60$weekday == day2)
plot(dd$time,dd$smooth, type="l", lwd=3,col=rgb(166/250,34/255,30/255),
     xlab="Time", ylab="Counts per minute (cpm)",
     xlim = c(stime,etime),ylim=c(0,6000), axes = F)
abline(h=c(100,573*4,1002*4), col = "gray", lty = "dotted",lwd = 3)
text(x=rep(stime-times("00:10:00"),3),y=c(573*4,1002*4,6150)-150,adj = c(0,1),
     labels=c("Light","Moderate","Vigorous"))
lines(dd$time,dd$smooth,lwd=3,col=rgb(166/250,34/255,30/255))
lines(dd2$time,dd2$smooth,lwd=3,col=rgb(49/250,76/255,117/255))
axis(side=1,at=seq(stime,etime,times("01:00:00")),
     labels= paste(hours(stime):hours(etime),":00",sep=""))
axis(2)
legend(x= c(etime-times("01:15:00")*etime/times("14:00:00"),etime-times("00:30:00")*etime/times("14:00:00")),y=c(4800,6000),
       legend=c("Thursday","Friday"),lty=1, lwd=3,
       col=c(rgb(166/250,34/255,30/255),rgb(49/250,76/255,117/255)),seg.len = 0.5,
       cex=1, x.intersp=0.1, xjust=0, yjust=0, bty="n")

box()
#===========================================================================

#===========================================================================

stime <- times("9:00:00")
etime <- times("10:00:00")
day1 <- "jueves"
day2 <- "viernes"
dd <- subset(data60, data60$hour >= hours(stime) & data60$hour<hours(etime) & data60$weekday == day1)
dd2 <- subset(data60, data60$hour >=hours(stime) & data60$hour<hours(etime) & data60$weekday == day2)
plot(dd$time,dd$smooth, type="l", lwd=3,col=rgb(166/250,34/255,30/255),
     xlab="Time", ylab="Counts per minute (cpm)",
     xlim = c(stime,etime),ylim=c(0,6000), axes = F)
abline(h=c(100,573*4,1002*4), col = "gray", lty = "dotted",lwd = 3)
text(x=rep(stime-times("00:02:00"),3),y=c(573*4,1002*4,6150)-150,adj = c(0,1),
     labels=c("Light","Moderate","Vigorous"))
lines(dd$time,dd$smooth,lwd=3,col=rgb(166/250,34/255,30/255))
lines(dd2$time,dd2$smooth,lwd=3,col=rgb(49/250,76/255,117/255))
axis(side=1,at=seq(stime,etime,times("00:15:00")),
     labels= seq(stime,etime,times("00:15:00")))
axis(2)
legend(x= c(etime-times("00:00:00"),etime-times("00:15:00")*etime/times("14:00:00")),y=c(4800,6000),
       legend=c("Thursday","Friday"),lty=1, lwd=3,
       col=c(rgb(166/250,34/255,30/255),rgb(49/250,76/255,117/255)),seg.len = 0.5,
       cex=1, x.intersp=0.1, xjust=0, yjust=0, bty="n")

box()

#=====================================================================00

n <- 1
smoothed <- c()
for(i in 1:nrow(data)){
  cat(i, "\n")
  smoothed <- c(smoothed,mean(data$axis1[(i-n):(i+n)]))
}
data$smooth <- smoothed

dd<- data
plot(dd$datetime,dd$smooth, type="l", lwd=1,col=rgb(166/250,34/255,30/255),
     xlab="Date/Time", ylab="Counts per second (cps)")
