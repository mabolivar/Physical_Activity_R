#===================================================================
#DESCRIPTION
#===================================================================
#
#
#
#===================================================================


#==============================================================================
# Working directory
#==============================================================================

setwd("C:\\Users\\Manuel\\Desktop\\Bkup Olga Lucia")
setwd("C:/Users/ma.bolivar643/Dropbox/2. Uniandes Projects/Accelerometria/MARA")
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
  m<-regexec("\\\\[[:print:]]+\\\\([[:digit:]]{5})([[:digit:]]{2}).([[:digit:]]+)([A-Z])([[:digit:]]+)",dbDir)
  use <- regmatches(dbDir, m)[[1]][5] #If it is the first time of use -> A, else B.
  id <- regmatches(dbDir, m)[[1]][2] #Participant ID
  acserial <- regmatches(dbDir, m)[[1]][4] #Accelerometer serial number
  
  
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

inputdir <- ".\\ISCOLE"
outputdir <- ".\\output"

#d <- "1145508_10462A1sec.agd"
main <- function(){
  
  #Time
  tnow <- Sys.time()   
  #Get datafiles names
  dataFiles <- dir(inputdir) 
  
  #Open Log output
  sink(paste(outputdir,"\\log.txt",collapse="",sep=""))
  cat("Log output:\n")  
  
  for (d in dataFiles){
    #Database location
    dbDir <- paste(inputdir,"\\",d,sep="")
    cat(d)
    if(file.info(dbDir)$size/1000<8000){
      cat("...........Wrong file size\n")
    }else{
      #0. Read data form the .agd files (SQLite database)
      db <- readDatabase(dbDir)
      data <- db$data
      settings <- db$settings
      
      #1. Quality control checks
      #valid <- qualityControlCheck(dbDir,data, settings, pack)
      epoch <- as.numeric(data[2,1]-data[1,1], units="secs")
      valid <- T
      
      if(epoch == 1){
        #2 Data aggregation
        data60 <- aggregation(60, data)
        data60$activity <- rep("wear",nrow(data60)) #Activity (sleep, non-wear, wear/wake)
        
        #2.5. Cleaning (Remove last day of data)
        udays <- unique(as.Date(data60$datetime))
        lastday <- max(udays)
        data60 <- removeData(data60,toremove=c(lastday),units="days") 
                     
        #4. Non-wear period
        nWP <- nonWearPeriods(data60, scanParam="axis1",innactivity=20,tolerance=0) #nonWearPeriods. Innactivity and tolerance in minutes
        data60$activity  <- setActivitySNW(data60$activity,label="non-wear",nWP, minlength = 20)
        
                
        #6. Cleaning (Remove last day of data and more than 7 days of data)
        udays <- unique(as.Date(data60$datetime))
        firstday <- min(udays)
        lastday <- max(udays)
        validdays <- seq(firstday,firstday+6,by=1) 
        daystoremove <- udays[!(udays%in%validdays)]
        data60 <- removeData(data60,toremove=c(daystoremove),units="days") 
        
        #7. Add intensity physical activity
        data60$intensityEV <- mapply(cut_points_evenson,epoch=60, data60$axis1)
        data60<-data60[,!(names(data60)%in%c("day_m2m","day_n2n"))] #Remove type of days colunms
        #Copies the final data frame in the clipboard
		    #write.csv(ob,file="clipboard", row.names=F)
        write.csv(data60,file=paste(outputdir,"\\",substr(d,start = 1,14),"60sec.csv", sep="",collapse=""), row.names=F)
        
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
    
  print(paste("Total time:", as.numeric(Sys.time()-tnow,units="mins")," mins"))
}

#=============================================================================

tryCatch(main(),finally=sink())
