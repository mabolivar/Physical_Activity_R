#==============================================================================
# Working directory
#==============================================================================

setwd("C:\\Users\\Manuel\\Documents\\MARA")
setwd("C:\\Users\\ma.bolivar643\\Documents\\MARA")

#==============================================================================
# Required libraries
#==============================================================================

library("RSQLite")
library("sqldf")
library("xlsx")
library("Hmisc")
library("chron")
library("plyr")
source(".\\R\\ADPP.R")


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
  
  #Counts per day
  data[data$activity =="non-wear","intensityEV"] <- "non-wear"
  counts <- as.data.frame(tapply(data$axis1,INDEX=list(data$day_m2m,data$intensityEV),FUN=sum))
  names(counts) <- paste("cnts",names(counts),sep="_")
  minutes <- as.data.frame.ts(table(data$day_m2m,data$intensityEV))/tomin
  names(minutes) <- paste("min",names(minutes),sep="_")
  date <- row.names(counts)
  
  palvls <- c("vigorous","moderate","light","sedentary")
  
  obs <- data.frame(PID,date,Measure,use,counts,minutes)  
  row.names(obs) <- NULL 
  return(obs)
    
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

checkNames <- function(folders = dir(".\\data")){
  
  for(fold in folders){
    
    inputdir <- paste(".\\data\\",fold,sep="")
    
    #Get datafiles names
    dataFiles <- dir(inputdir, pattern = "(.+)agd")
    #print(dataFiles)
    ok <- grepl(pattern = "^[0-9]{4}[AB][0-9][_][0-9]{5}[_][0-9]{8}1sec[.]agd",
                x = dataFiles)
    cat(fold,"\n")
    print(dataFiles[!ok])
    cat("\n")
  }
}

checkDates <- function(folders = dir(".\\data")){
  sink(paste("dates_",as.Date(Sys.time()),".txt",collapse="",sep=""))
  for(fold in folders[9:10]){
    
    inputdir <- paste(".\\data\\",fold,sep="")
    
    #Get datafiles names
    dataFiles <- dir(inputdir, pattern = "(.+)agd")
    
    cat(fold,"\n")
    for (d in dataFiles){
      #Garbage collector
      gc()
      #Database location
      dbDir <- paste(inputdir,"\\",d,sep="")
      cat(d)
      m <- regexec(pattern = "^[0-9]{4}[AB][0-9][_][0-9]{5}[_]([0-9]{8})1sec[.]agd", d)
      date <-as.Date(regmatches(dataFiles[1],m)[[1]][2],format = "%Y%m%d")
      date2 <-as.Date(regmatches(dataFiles[1],m)[[1]][2],format = "%d%m%Y")
      
      #Checks file size
      if(file.info(dbDir)$size/1000<8000){
        cat(d, "...........Wrong file size\n")
      }else{
        #0. Read data form the .agd files (SQLite database)
        db <- readDatabase(dbDir)
        data <- db$data
        settings <- db$settings
        
        cat(d,"nameFileDate1:",as.character(date), "nameFileDate2:",as.character(date2),
            " .agdDate:",as.character(as.Date(settings[18,3])),
            "Equals1: ",as.Date(settings[18,3]) == date, "Equals2: ",as.Date(settings[18,3]) == date2)
        cat("\n")
        
        as.character(as.POSIXct(as.numeric(635107112200000000*1.0e-7 - 62135596800),origin="1970-01-01 00:00:00", "GMT"))
      }
      
    }
    
    cat("\n")
  }
  sink()
}

#=============================================================================
# Main routine
#=============================================================================

main <- function(){
  folders <- dir(".\\data")
  
  outputdir <- ".\\output"
  outputfile <- paste("COL PA MARA RECREO_",as.Date(Sys.time()),".csv", sep="")
  packdir <- "PACK-MARA.xlsx"
  sheets <- c("COLEGIO 20 DE JULIO", "COLEGIO MANUELITA SAENZ",
              "COLEGIO MONTEBELLO", "ISCOLE")
  #Time
  tnow <- Sys.time()   
  #Final data set
  adata <- data.frame()
  
  #Open Log output
  sink(paste(outputdir,"\\log_",as.Date(Sys.time()),".txt",collapse="",sep=""))
  cat("Log output:\n") 
  
  for(fold in folders){
    
    inputdir <- paste(".\\data\\",fold,sep="")
    
    #Get datafiles names
    dataFiles <- dir(inputdir, pattern = "(.+)agd")
    
    cat(fold,"\n")
    #Read the PACK
    #pack <- readPack(packdir,sheets)  
    
    for (d in dataFiles){
      #Garbage collector
      gc()
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
        
        #0.5 Remove unrequired hours
        hourstoremove <- c(0:6,12:23)
        data <- removeData(data,toremove=c(hourstoremove),units="hours") 
        
        
        
        #1. Quality control checks
        #valid <- qualityControlCheck(dbDir,data, settings, pack)
        valid <- T
        epoch <- as.numeric(data[2,1]-data[1,1], units="secs")
        
        if(epoch == 1){
          #2 Data aggregation
          data15 <- aggregation(15, data)
          data60 <- aggregation(60, data)
          
          
          data15$activity <- rep("wear",nrow(data15)) #Activity (sleep, non-wear, wear/wake)
          data60$activity <- rep("wear",nrow(data60)) #Activity (sleep, non-wear, wear/wake)
          
          #4. Non-wear period
          nWP <- nonWearPeriods(data60, scanParam="axis1",innactivity=20,tolerance=0) #nonWearPeriods. Innactivity and tolerance in minutes
          data60$activity  <- setActivitySNW(data60$activity,label="non-wear",nWP, minlength = 20)
          
          #5. Wear periods
          data60 <- checkWearPeriods(data60,maxc = 20000)
          
          #6. Cleaning (Remove more than 7 days of data)
          udays <- unique(as.Date(data60$datetime))
          firstday <- min(udays)
          lastday <- max(udays)
          validdays <- seq(firstday,firstday+6,by=1) 
          daystoremove <- udays[!(udays%in%validdays)]
          data15 <- removeData(data15,toremove=c(daystoremove),units="days") 
          data60 <- removeData(data60,toremove=c(daystoremove),units="days") 
          
          #6.5 Extract Recreo hours
          hourstoremove <- c(0:8,10:23)
          data15 <- removeData(data15,toremove=c(hourstoremove),units="hours") 
          data60 <- removeData(data60,toremove=c(hourstoremove),units="hours")
          #6.6 Extract Recreo minutes
          minutestoremove <- c(0:14,45:59)
          data15 <- removeData(data15,toremove=c(minutestoremove),units="minutes") 
          data60 <- removeData(data60,toremove=c(minutestoremove),units="minutes")
          
          #7. Add intensity physical activity
          data15 <- mergingActivity(data15,data60)
          data15$intensityEV <- mapply(cut_points_evenson,epoch=15, data15$axis1)
          data60$intensityEV <- mapply(cut_points_evenson,epoch=60, data60$axis1)
          
          #8. Get the datafile observation for the final data frame(only wear periods)
          ob <- getobs(dbDir,data15, timeunit="min")
          
          #Copies the final data frame in the clipboard
          #write.csv(ob,file="clipboard", row.names=F)
          
          adata<-rbind.fill(adata,ob)
          write.csv(adata,file=paste(outputdir,"\\",outputfile, sep="",collapse=""), row.names=F)
          
          if(valid==T){
            cat("........OK\n")
          }else{
            cat("........INVALID\n")
          }
          
        }else{
          cat("........SKIPPED (Wrong epoch) \n")
        }
        
        #delete objects
        rm(data)
        rm(data15)
        rm(data60)
      }
    }
    
  }
  
  sink()
  write.csv(adata,file=paste(outputdir,"\\",outputfile, sep="",collapse=""), row.names=F)
  
  print(paste("Total time:", as.numeric(Sys.time()-tnow,units="mins")," mins"))
}

#=================================================================================

tryCatch(main(),finally=sink())

