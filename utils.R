require(datapackr)
options(shiny.maxRequestSize=30*1024^2)
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
    is_parallel<-TRUE
    
  } else {
    is_parallel<-FALSE
  } 
  vr_violations <- datimvalidation::validateData(vr_data,
                                                 datasets = datasets_uid,
                                                 parallel = is_parallel)
  vr_rules<-getValidationRules()
  cop_19_des<-getValidDataElements(datasets=datasets_uid)
  match<-paste(unique(cop_19_des$dataelementuid),sep="",collapse="|")
  vr_filter<-vr_rules[grepl(match,vr_rules$leftSide.expression) & grepl(match,vr_rules$rightSide.expression),"id"]
  vr_violations<-vr_violations[ vr_violations$id %in% vr_filter,]
  diff<-gsub(" <= ","/",vr_violations$formula)
  vr_violations$diff<-sapply(diff,function(x) { round( ( eval(parse(text=x)) -1 ) * 100 , 2) })
  
  d$datim$vr_rules_check <-vr_violations[,c("name","ou_name","mech_code","formula","diff")]
  
  d
  
}


adornMechanisms<-function(d) {
  
  url <- paste0(getOption("baseurl"),"api/sqlViews/fgUtV6e9YIX/data.csv")
  mechs <- readr::read_csv(url) %>% 
    dplyr::select(mechanism,partner,agency,ou)
  
  d %>% dplyr::left_join(.,mechs, by=c("mechanismCode" = "mechanism"))
}