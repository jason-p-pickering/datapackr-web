require(datapackr)
require(scales)
require(futile.logger)
require(DT)
require(config)
require(paws)

#Set the maximum file size for the upload file
options(shiny.maxRequestSize = 100 * 1024 ^ 2)

#Initiate logging
logger <- flog.logger()
#Load the local config file
config <- config::get()
Sys.setenv(AWS_REGION = config$aws_region)

options("baseurl" = config$baseurl)
options("support_files_directory" = config$deploy_location)
flog.appender(appender.file(config$log_path), name="datapack")

DHISLogin <- function(baseurl, username, password) {
  httr::set_config(httr::config(http_version = 0))
  url <- URLencode(URL = paste0(config$baseurl, "api/me"))
  #Logging in here will give us a cookie to reuse
  r <- httr::GET(url,
                 httr::authenticate(username, password),
                 httr::timeout(60))
  if (r$status != 200L) {
    return(FALSE)
  } else {
    me <- jsonlite::fromJSON(httr::content(r, as = "text"))
    options("organisationUnit" = me$organisationUnits$id)
    return(TRUE)
  }
}

validatePSNUData <- function(d) {
  
  vr_data<-d$datim$MER
  if (is.null(vr_data) | NROW(vr_data) == 0 ) {return(d)}
  
  # We need ALL mechanisms to be in DATIM before remapping....TODO
  vr_data$attributeOptionCombo <-
    datimvalidation::remapMechs(vr_data$attributeOptionCombo,
                                getOption("organisationUnit"),
                                "code",
                                "id")
  datasets_uid <- c("Pmc0yYAIi1t", "s1sxJuqXsvV")
  if ( Sys.info()["sysname"] == "Linux") {
    ncores <- parallel::detectCores() - 1
    doMC::registerDoMC( cores = ncores )
    is_parallel <- TRUE
  } else {
    is_parallel <- FALSE
  }
  
  vr_violations <- datimvalidation::validateData(vr_data,
                                                 datasets = datasets_uid,
                                                 parallel = is_parallel,
                                                 return_violations_only = TRUE)
  
  # rules_to_keep <- c(
  #   "L76D9NGEPRS",
  #   "rVVZmdG1KTb",
  #   "zEOFo6X436M",
  #   "oVtpQHVVeCV",
  #   "r0CC6MQW5zc",
  #   "vrS3kAtlJ4F",
  #   "WB338HNucS7",
  #   "tiagZGzSh6G",
  #   "vkFHYHgfqCf",
  #   "coODsuNsoXu",
  #   "qOnTyseQXv8",
  #   "Ry93Kc34Zwg",
  #   "g0XwMGLB5XP",
  #   "eb02xBNx7bD",
  #   "SNzoIyNuanF",
  #   "Ry93Kc34Zwg",
  #   "WiRJutVpAq4"
  # )
  
  rules_to_ignore<-c("RLXOqAeHN04","KdqWm8ZvWoO")
  
  vr_violations<-vr_violations[!(vr_violations$id %in% rules_to_ignore),]
  
  if ( NROW(vr_violations) > 0 ) {
    
    # vr_violations <-
    #   vr_violations[vr_violations$id %in% rules_to_keep,] } else {
    #     d$datim$vr_rules_check <- NULL
    #     return(d)
    #   }
    
    diff <- gsub(" [<>]= ", "/", vr_violations$formula)
    vr_violations$diff <- sapply( diff, function(x) { round( ( eval( parse( text = x ) ) - 1) * 100, 2 ) })
    vr_violations$diff <-ifelse(vr_violations$rightSide.expression == 0 | vr_violations$leftSide.expression == 0,
                                NA,
                                vr_violations$diff)
    
    #vr_violations %<>% dplyr::filter(diff >= 5)
    
    diff <- gsub(" [<>]= ", "-", vr_violations$formula)
    vr_violations$abs_diff <- sapply( diff, function(x) { abs( eval( parse( text = x ) ) ) })
    
    
    if (NROW(vr_violations) > 0) {
      
      d$tests$vr_rules_check <- vr_violations  %>%
        dplyr::select(name, ou_name, mech_code, formula, diff,abs_diff) %>%
        dplyr::rename("Validation rule" = name,
                      "PSNU" = ou_name,
                      "Mechanism" = mech_code,
                      "Formula" = formula,
                      "Diff (%)" = diff,
                      "Diff (Absolute)" = abs_diff)
      
      flog.info(
        paste0(
          NROW(vr_violations),
          " validation rule issues found in ",
          d$info$datapack_name,
          " DataPack."
        ),
        name = "datapack"
      )
    } else {
      d$tests$vr_rules_check <- NULL
      flog.info(
        paste0(
          "No validation rule issues found in ",
          d$info$datapack_name,
          " DataPack."
        ),
        name = "datapack"
      )
      return(d)
    }
  }
  
  d
  
}

#TODO: Move this back to the DataPackr....
validateMechanisms<-function(d) {
  
  
  if (is.null(d$data$distributedMER)) {return(d)}
  vr_data <- d$data$distributedMER %>%
    dplyr::pull(mechanism_code) %>%
    unique()
  
  #TODO: Removve hard coding of time periods and 
  #filter for the OU as well
  mechs<-datapackr::getMechanismView() %>%
    dplyr::filter(!is.na(startdate)) %>%
    dplyr::filter(!is.na(enddate)) %>%
    dplyr::filter(startdate <= as.Date('2020-10-01')) %>%
    dplyr::filter(enddate >= as.Date('2021-09-30')) %>%
    dplyr::pull(mechanism_code)
  
  #Allow for the pseudo dedupe mechanism
  mechs <- append("99999",mechs)
  
  bad_mechs<-vr_data[!(vr_data %in% mechs)]
  
  if (length(bad_mechs) > 0 ) {
    
    msg <- paste0("ERROR!: Invalid mechanisms found in the PSNUxIM tab. 
                  These MUST be reallocated to a valid mechanism
                  ",paste(bad_mechs,sep="",collapse=","))
    d$tests$bad_mechs<-bad_mechs
    d$info$warning_msg<-append(msg,d$info$warning_msg)
    d$info$had_error<-TRUE
  }
  
  d
  
}

getCountryNameFromUID<-function(uid) {
  
  
  paste0(getOption("baseurl"),"api/organisationUnits/",uid,"?fields=shortName") %>%
    URLencode(.) %>%
    httr::GET(.) %>%
    httr::content(.,"text") %>%
    jsonlite::fromJSON(.) %>% 
    purrr::pluck(.,"shortName")
}

archiveDataPacktoS3<-function(d,datapath,config) {
  
  d$info$country_uids<-substr(paste0(d$info$country_uids,sep="",collapse="_"),0,25)
  
  #Write an archived copy of the file
  s3<-paws::s3()
  tags<-c("tool","country_uids","cop_year","has_error","sane_name")
  object_tags<-d$info[names(d$info) %in% tags] 
  object_tags<-URLencode(paste(names(object_tags),object_tags,sep="=",collapse="&"))
  object_name<-paste0("datapack_archives/",gsub(" ","_",d$info$sane_name),"_",format(Sys.time(),"%Y%m%d_%H%m%s"),".xlsx")
  # Load the file as a raw binary
  read_file <- file(datapath, "rb")
  raw_file <- readBin(read_file, "raw", n = file.size(datapath))
  close(read_file)
  
  r<-tryCatch({
    foo<-s3$put_object(Bucket = config$s3_bucket,
                       Body = raw_file,
                       Key = object_name,
                       Tagging = object_tags,
                       ContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    flog.info("Datapack Archive sent to S3", name = "datapack")
    TRUE
  },
  error = function(err) {
    flog.info("Datapack could not be archived",name = "datapack")
    flog.info(err, name = "datapack")
    FALSE
  })
  
  return(r)
  
}

archiveDataPackErrorUI <- function(r) {
  if (!r) {
    showModal(modalDialog(title = "Error",
                          "The DataPack could not be archived."))
  }
}

saveTimeStampLogToS3<-function(d) {
  
  d$info$country_uids<-substr(paste0(d$info$country_uids,sep="",collapse="_"),0,25)
  
  #Write an archived copy of the file
  s3<-paws::s3()
  tags<-c("tool","country_uids","cop_year","has_error","sane_name")
  object_tags<-d$info[names(d$info) %in% tags] 
  object_tags<-URLencode(paste(names(object_tags),object_tags,sep="=",collapse="&"))
  object_name<-paste0("processed/",d$info$sane_name,".csv")
  
  #Save a timestamp of the upload
  timestamp_info<-list(
    ou=d$info$datapack_name,
    ou_id=d$info$country_uids,
    country_name=d$info$datapack_name,
    country_uid=d$info$country_uids,
    upload_timestamp=strftime(as.POSIXlt(Sys.time(), "UTC") , "%Y-%m-%d %H:%M:%S"),
    filename=object_name
  )
  
  tmp<-tempfile()
  write.table(
    as.data.frame(timestamp_info),
    file = tmp,
    quote = FALSE,
    sep = "|",
    row.names = FALSE,
    na = "",
    fileEncoding = "UTF-8"
  )
  # Load the file as a raw binary
  read_file <- file(tmp, "rb")
  raw_file <- readBin(read_file, "raw", n = file.size(tmp))
  close(read_file)
  object_name<-paste0("upload_timestamp/",d$info$sane_name,".csv")
  
  r<-tryCatch({
    foo<-s3$put_object(Bucket = config$s3_bucket,
                       Body = raw_file,
                       Key = object_name,
                       Tagging = object_tags,
                       ContentType = "text/csv")
    flog.info("Timestamp log sent to S3", name = "datapack")
    TRUE
  },
  error = function(err) {
    flog.error("Timestamp log could not be saved to S3",name = "datapack")
    FALSE
  })
  unlink(tmp)
  return(r)
}

timestampUploadUI<-function(r) {
  if (!r) {
    showModal(modalDialog(title = "Error",
                          "Timestamp log could not be saved to S3."))
  }
  
}

prepareFlatMERExport<-function(d) {
  
  d$data$analytics <-  d$data$analytics %>% 
    dplyr::mutate(upload_timestamp = format(Sys.time(),"%Y-%m-%d %H:%M:%S"),
                  fiscal_year = "FY21") %>% 
    dplyr::select( ou,
                   ou_id,
                   country_name,
                   country_uid,
                   snu1,
                   snu1_id,
                   psnu,
                   psnuid,
                   prioritization,
                   mechanism_code,
                   mechanism_desc,
                   partner_id,
                   partner_desc,
                   funding_agency,
                   fiscal_year,
                   dataelement_id ,
                   dataelement_name,
                   indicator,
                   numerator_denominator ,
                   support_type ,
                   hts_modality ,
                   categoryoptioncombo_id ,
                   categoryoptioncombo_name ,
                   age,
                   sex, 
                   key_population ,
                   resultstatus_specific ,
                   upload_timestamp,
                   disagg_type,
                   resultstatus_inclusive,
                   top_level,
                   target_value)
  
  d
}

sendMERDataToPAW<-function(d,config) {
  
  d$info$country_uids<-substr(paste0(d$info$country_uids,sep="",collapse="_"),0,25)
  
  #Write the flatpacked output
  tmp <- tempfile()
  mer_data<-d$data$analytics
  
  #Need better error checking here I think. 
  write.table(
    mer_data,
    file = tmp,
    quote = FALSE,
    sep = "|",
    row.names = FALSE,
    na = "",
    fileEncoding = "UTF-8"
  )
  
  # Load the file as a raw binary
  read_file <- file(tmp, "rb")
  raw_file <- readBin(read_file, "raw", n = file.size(tmp))
  close(read_file)
  
  
  tags<-c("tool","country_uids","cop_year","has_error","sane_name")
  object_tags<-d$info[names(d$info) %in% tags] 
  object_tags<-URLencode(paste(names(object_tags),object_tags,sep="=",collapse="&"))
  object_name<-paste0("processed/",d$info$sane_name,".csv")
  s3<-paws::s3()
  
  r<-tryCatch({
    foo<-s3$put_object(Bucket = config$s3_bucket,
                       Body = raw_file,
                       Key = object_name,
                       Tagging = object_tags,
                       ContentType = "text/csv")
    flog.info("Flatpack sent to AP", name = "datapack")
    TRUE
  },
  error = function(err) {
    flog.info("Flatpack cannot be sent to AP",name = "datapack")
    flog.info(err, name = "datapack")
    FALSE
  })
  
  unlink(tmp)
  
  return(r)
}

sendValidationSummary<-function(vr,config) {
  
  vr$info$country_uids<-substr(paste0(vr$info$country_uids,sep="",collapse="_"),0,25)
  validation_summary<-validationSummary(vr)
  
  tmp <- tempfile()
  #Need better error checking here I think. 
  write.table(
    validation_summary,
    file = tmp,
    quote = FALSE,
    sep = "|",
    row.names = FALSE,
    na = "",
    fileEncoding = "UTF-8"
  )
  # Load the file as a raw binary
  read_file <- file(tmp, "rb")
  raw_file <- readBin(read_file, "raw", n = file.size(tmp))
  close(read_file)
  
  tags<-c("tool","country_uids","cop_year","has_error","sane_name")
  object_tags<-vr$info[names(vr$info) %in% tags] 
  object_tags<-URLencode(paste(names(object_tags),object_tags,sep="=",collapse="&"))
  
  object_name<-paste0("validation_error/",vr$info$sane_name,".csv")
  s3<-paws::s3()
  
  r<-tryCatch({
    foo<-s3$put_object(Bucket = config$s3_bucket,
                       Body = raw_file,
                       Key = object_name,
                       Tagging = object_tags,
                       ContentType = "text/csv")
    
    TRUE
  },
  error = function(err) {
    flog.info("Validation summary could not be sent to AP",name = "datapack")
    flog.info(err, name = "datapack")
    FALSE
  })
  
  unlink(tmp)
  
  return(r)
  
}

validationSummaryUI<-function(r) {
  if (!r) {
    showModal(modalDialog(title = "Error","Validation summary could not be sent to AP."))
  } 
}

saveDATIMExportToS3<-function(d) {
  #Write the flatpacked output
  tmp <- tempfile()
  d$datim$MER$value<-as.character(d$datim$MER$value)
  d$datim$subnat_impatt$value<-as.character(d$datim$subnat_impatt$value)
  datim_export<-dplyr::bind_rows(d$datim$MER,d$datim$subnat_impatt)
  
  #Need better error checking here I think. 
  write.table(
    datim_export,
    file = tmp,
    quote = FALSE,
    sep = "|",
    row.names = FALSE,
    na = "",
    fileEncoding = "UTF-8"
  )
  
  # Load the file as a raw binary
  read_file <- file(tmp, "rb")
  raw_file <- readBin(read_file, "raw", n = file.size(tmp))
  close(read_file)
  
  
  tags<-c("tool","country_uids","cop_year","has_error","sane_name")
  object_tags<-d$info[names(d$info) %in% tags] 
  object_tags<-URLencode(paste(names(object_tags),object_tags,sep="=",collapse="&"))
  object_name<-paste0("datim_export/",d$info$sane_name,".csv")
  s3<-paws::s3()
  
  r<-tryCatch({
    foo<-s3$put_object(Bucket = config$s3_bucket,
                       Body = raw_file,
                       Key = object_name,
                       Tagging = object_tags,
                       ContentType = "text/csv")
    flog.info("DATIM Export sent to S3", name = "datapack")
    TRUE
  },
  error = function(err) {
    flog.info("DATIM Export could not be sent to  S3",name = "datapack")
    flog.info(err, name = "datapack")
    FALSE
  })
  
  unlink(tmp)
  
  return(r)
  
}

datimExportUI<-function(r) {
  if (!r) {
    showModal(modalDialog(title = "Error",
                          "DATIM Export could not be sent to S3"))
  } else {
    showModal(modalDialog(title = "Congrats!",
                          "Export to PAW was successful."))
  }
}
