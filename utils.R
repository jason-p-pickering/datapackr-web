require(datapackr)
options(shiny.maxRequestSize=70*1024^2)
options("baseurl" = "http://127.0.0.1:8080/")

DHISLogin<-function(baseurl, username, password) {
  httr::set_config(httr::config(http_version = 0))
  url <- URLencode(URL = paste0(getOption("baseurl"), "api/me"))
  #Logging in here will give us a cookie to reuse
  r <- httr::GET(url ,
                 httr::authenticate(username, password),
                 httr::timeout(60))
  if(r$status != 200L){
    return(FALSE)
  } else {
    me <- jsonlite::fromJSON(httr::content(r,as = "text"))
    options("organisationUnit" = me$organisationUnits$id)
    return(TRUE)
  }
}

filterZeros<-function(d) {
  
  #Filter any zeros
  d$data$MER %<>% filter(.,value != 0)
  d$data$SUBNAT_IMPATT %<>% filter(.,value != 0)
  d$data$SNUxIM %<>% filter(., distribution != 0)
  d$data$distributedMER %<>% filter(.,value != 0)
  
  d
}


validatePSNUData<-function(d) {
  
  #Validation rule checking
  vr_data <- d$datim$PSNUxIM
    
  names(vr_data) <- c("dataElement",
                      "period",
                      "orgUnit",
                      "categoryOptionCombo",
                      "attributeOptionCombo",
                      "value")
  
  vr_data$attributeOptionCombo <-
    datimvalidation::remapMechs(vr_data$attributeOptionCombo,
                                getOption("organisationUnit"),
                                "code",
                                "id")
  
  datasets_uid<-c("nIHNMxuPUOR","sBv1dj90IX6")
  
  if ( Sys.info()['sysname'] == "Linux") {
    
    ncores <- parallel::detectCores() - 1
    doMC::registerDoMC(cores=ncores)
    is_parallel <- TRUE
    
  } else {
    is_parallel <- FALSE
  } 
  vr_violations <- datimvalidation::validateData(vr_data,
                                                 datasets = datasets_uid,
                                                 parallel = is_parallel)
  
  rules_to_keep <- c(
    "ZuX9Ck27Bb2",
    "L76D9NGEPRS",
    "DztKZSt84yx",
    "rVVZmdG1KTb",
    "zEOFo6X436M",
    "oVtpQHVVeCV",
    "r0CC6MQW5zc",
    "vrS3kAtlJ4F",
    "WB338HNucS7",
    "tiagZGzSh6G",
    "vkFHYHgfqCf",
    "coODsuNsoXu",
    "qOnTyseQXv8",
    "Ry93Kc34Zwg",
    "g0XwMGLB5XP",
    "eb02xBNx7bD",
    "SNzoIyNuanF"
  )
  
  vr_violations<-vr_violations[ vr_violations$id %in% rules_to_keep,]
  
  diff<-gsub(" <= ","/",vr_violations$formula)
  
  vr_violations$diff<-sapply(diff,function(x) { round( ( eval(parse(text=x)) -1 ) * 100 , 2) })
  vr_violations %<>% dplyr::filter(diff >= 5)
  d$datim$vr_rules_check <-vr_violations[,c("name","ou_name","mech_code","formula","diff")]
  
  d
  
}


adornMechanisms <- function(d) {
  
  cached_mechs <- "/srv/shiny-server/apps/datapack/mechs.rds"
  
  if (file.access(cached_mechs,4)) {
    
    mechs <-readRDS(cached_mechs)

  } else {
    
    mechs <- paste0(getOption("baseurl"),"api/sqlViews/fgUtV6e9YIX/data.csv") %>% 
      httr::GET() %>% 
      httr::content(., "text") %>% 
      readr::read_csv(col_names = TRUE) %>% 
      dplyr::select(mechanismCode="code",partner,agency,ou)
    
  }
  
  dplyr::left_join( d , mechs, by = "mechanismCode" )

  }