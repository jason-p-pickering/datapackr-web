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

filterZeros <- function(d) {
  
  d$data <- lapply(d$data,function(x) dplyr::filter(x, value != 0 ))
  d
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
  datasets_uid <- c("nIHNMxuPUOR", "sBv1dj90IX6")
  if ( Sys.info()["sysname"] == "Linux") {
    ncores <- parallel::detectCores() - 1
    doMC::registerDoMC( cores = ncores )
    is_parallel <- TRUE
  } else {
    is_parallel <- FALSE
  } 
  vr_violations <- datimvalidation::validateData(d$datim$MER,
                                                 datasets = datasets_uid,
                                                 parallel = is_parallel)
  
  rules_to_keep <- c(
    "L76D9NGEPRS",
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
  
  if ( NROW(vr_violations) > 0 ) {
    
    vr_violations <-
      vr_violations[vr_violations$id %in% rules_to_keep,] } else {
        d$datim$vr_rules_check <- NULL
        return(d)
      }
  
  diff <- gsub(" <= ", "/", vr_violations$formula)
  vr_violations$diff <-
    sapply( diff, function(x) {
      round( ( eval( parse( text = x) ) - 1) * 100, 2 )
    })
  
  vr_violations %<>% dplyr::filter(diff >= 5) 
  
  if (NROW(vr_violations) > 0) {
    d$datim$vr_rules_check <- vr_violations  %>%
      dplyr::select(name, ou_name, mech_code, formula, diff) %>%
      dplyr::mutate(name = gsub(pattern = " DSD,", "", name))
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
    d$datim$vr_rules_check <- NULL
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
  mechs<-getMechanismView() %>%
    dplyr::filter(!is.na(startdate)) %>%
    dplyr::filter(!is.na(enddate)) %>%
    dplyr::filter(startdate <= as.Date('2020-10-01')) %>%
    dplyr::filter(enddate >= as.Date('2021-09-30')) %>%
    dplyr::pull(mechanism_code)
  
  #Allow for dedupe
  #mechs <- append("00000",mechs)
  
  bad_mechs<-vr_data[!(vr_data %in% mechs)]
  
  if (length(bad_mechs) > 0 ) {
    
    msg <- paste0("ERROR!: Invalid mechanisms found in the PSNUxIM tab. 
                  These MUST be reallocated to a valid mechanism
                  ",paste(bad_mechs,sep="",collapse=","))
    d$tests$bad_mechs<-bad_mechs
    d$info$warningMsg<-append(msg,d$info$warningMsg)
    d$info$had_error<-TRUE
  }
  
  d
  
}

getMechanismView<-function() {
  
  cached_mechs <- paste0(config$deploy_location,"mechs.rds")
  
  if ( file.access(cached_mechs,4) == 0 ) {
    
    readRDS(cached_mechs)
    
  } else {
    
    # mechs<- paste0(getOption("baseurl"),"api/categoryOptionCombos?filter=categoryCombo.id:eq:wUpfppgjEza&fields=code,name,id,categoryOptions[startDate,endDate,organisationUnits[id,name]]&paging=false") %>%
    #   URLencode(.) %>%
    #   httr::GET(., httr::timeout(60)) %>%
    #   httr::content(.,"text") %>%
    #   jsonlite::fromJSON(., flatten = TRUE) %>%
    #   purrr::pluck(.,"categoryOptionCombos")
    # 
    # mech_has_ou <- mechs %>%
    #   purrr::pluck(.,"categoryOptions") %>%
    #   purrr::map(.,"organisationUnits") %>%
    #   purrr::flatten(.) %>%
    #   purrr::map(., function(x) { class(x) == "data.frame" }) %>%
    #   unlist(.)
    # 
    # mechs %<>% dplyr::filter(mech_has_ou)
    # 
    # mechs$startdate <-
    #   as.Date(sapply(mechs$categoryOptions, function(x)
    #     ifelse(is.null(x$startDate), "1900-01-01", x$startDate)),
    #     "%Y-%m-%d")
    # mechs$enddate <-
    #   as.Date(sapply(mechs$categoryOptions, function(x)
    #     ifelse(is.null(x$endDate), "1900-01-01", x$endDate)),
    #     "%Y-%m-%d")
    # 
    # 
    # mechs_ous<- mechs %>%
    #   purrr::pluck(.,"categoryOptions") %>%
    #   purrr::map(.,"organisationUnits") %>%
    #   purrr::flatten_dfr(.,.id="foo") %>%
    #   dplyr::select("operating_unit"=name,
    #                 "operating_unit_uid"=id)
    # 
    # mechs %<>% dplyr::select(-categoryOptions) %>%
    #   dplyr::bind_cols(.,mechs_ous) %>%
    #   dplyr::rename(mechanism_code = code)
    
    paste0(getOption("baseurl"), "api/sqlViews/fgUtV6e9YIX/data.csv") %>%
      httr::GET() %>%
      httr::content(., "text") %>%
      readr::read_csv(col_names = TRUE) %>%
      dplyr::rename(mechanism_desc = mechanism,
                    attributeOptionCombo = uid,
                    mechanism_code = code,
                    partner_desc = partner,
                    partner_id = primeid)
    # mechs %<>% dplyr::left_join(mechs_partners,by="mechanism_code")
  }
  
  
}

adornMechanisms <- function(d) {
  
  mechs<-getMechanismView() %>% 
    dplyr::select(-ou,-startdate,-enddate)
  
  dplyr::left_join( d , mechs, by = "mechanism_code") %>% 
    dplyr::mutate(mechanism_desc = dplyr::case_when(mechanism_code == "99999" ~ 'Dedupe approximation',
                                                    TRUE ~ mechanism_desc),
                  partner_desc = dplyr::case_when(mechanism_code == "99999" ~ 'Dedupe approximation',
                                                  TRUE ~ partner_desc),
                  partner_id = dplyr::case_when(mechanism_code == "99999" ~ '99999',
                                                TRUE ~ partner_id),
                  agency = dplyr::case_when(mechanism_code == "99999" ~ 'Dedupe approximation',
                                            TRUE ~ agency))
  
  
}

getCOGSMap<-function(uid) {
  
  r<-paste0(getOption("baseurl"),"api/categoryOptionGroupSets/",uid,
            "?fields=id,name,categoryOptionGroups[id,name,categoryOptions[id,name,categoryOptionCombos[id,name]]") %>%
    URLencode(.) %>%
    httr::GET(.) %>%
    httr::content(.,"text") %>%
    jsonlite::fromJSON(.,flatten = TRUE) 
  
  dim_name<- r$name
  dim_id<- r$id
  
  cogs <- r %>% purrr::pluck(.,"categoryOptionGroups") %>% dplyr::select(id,name)
  
  cogs_cocs_map<-list()
  
  for (i in 1:NROW(cogs) ) {
    
    cos_cocs <- r %>%
      purrr::pluck(.,"categoryOptionGroups") %>% 
      purrr::pluck(.,"categoryOptions") %>%
      purrr::pluck(., i) %>% 
      purrr::pluck(.,"categoryOptionCombos") %>%
      do.call(rbind.data.frame,.) %>%
      dplyr::distinct() %>%
      dplyr::select("category_option_combo"=name,"coc_uid"=id)
    
    cos_cocs$category_option_group_name<-cogs[i,"name"]
    cos_cocs$category_option_group_uid<-cogs[i,"id"]
    cogs_cocs_map<-rlist::list.append(cogs_cocs_map,cos_cocs)
  }
  
  cogs_cocs_map %<>% do.call(rbind.data.frame,.)
  
  return(list(dimension_name=r$name,
              dimension_id=r$id,
              dimension_map= cogs_cocs_map))
  
}

getDEGSMap <- function(uid) {
  
  r <- paste0(getOption("baseurl"),"api/dataElementGroupSets/",uid,"?fields=id,name,dataElementGroups[name,dataElements[id]]&paging=false") %>%
    URLencode(.) %>%
    httr::GET(.) %>%
    httr::content(.,"text") %>%
    jsonlite::fromJSON(.,flatten = TRUE) 
  
  r %>%
    purrr::pluck(.,"dataElementGroups") %>% 
    dplyr::mutate_if(is.list, purrr::simplify_all) %>% 
    tidyr::unnest(cols = c(dataElements)) %>%
    dplyr::distinct() %>%
    dplyr::mutate(type=make.names(r$name))
  
}

#Used with permission from @achafetz  
#https://github.com/USAID-OHA-SI/tameDP/blob/master/R/clean_indicators.R

adornMERData <- function(d){
  
  if ( is.null(d) ) { return(NULL) }
  
  hiv_specific<-getCOGSMap("bDWsPYyXgWP") %>% #HIV Test Status (Specific)
    purrr::pluck("dimension_map") %>%
    dplyr::select("categoryoptioncombouid"=coc_uid,
                  "resultstatus"=category_option_group_name) %>%
    dplyr::mutate(resultstatus = stringr::str_replace(resultstatus,"\\(Specific\\)","")) %>%
    dplyr::mutate(resultstatus = stringr::str_replace(resultstatus,"HIV","")) %>%
    dplyr::mutate(resultstatus = stringr::str_trim(resultstatus))
  
  
  hiv_inclusive<-getCOGSMap("ipBFu42t2sJ") %>% # HIV Test Status (Inclusive)
    purrr::pluck("dimension_map") %>%
    dplyr::select("categoryoptioncombouid"=coc_uid,
                  "resultstatus_inclusive"=category_option_group_name) %>%
    dplyr::mutate(resultstatus_inclusive = stringr::str_replace(resultstatus_inclusive,"\\(Inclusive\\)","")) %>%
    dplyr::mutate(resultstatus_inclusive = stringr::str_replace(resultstatus_inclusive,"HIV","")) %>%
    dplyr::mutate(resultstatus_inclusive = stringr::str_replace(resultstatus_inclusive,"Status","")) %>%
    dplyr::mutate(resultstatus_inclusive = stringr::str_trim(resultstatus_inclusive))
  
  #Classify all dedupe as DSD
  d$data$distributedMER %<>% dplyr::mutate(support_type = dplyr::case_when(mechanism_code == "99999" ~ 'DSD',
                                                                           TRUE ~ support_type))
  
  
  #Append the distributed MER data and subnat data together
  df <- dplyr::bind_rows(d$data$distributedMER,
                         dplyr::mutate(d$data$SUBNAT_IMPATT,
                                       mechanism_code = "HllvX50cXC0",
                                       support_type="DSD"))
  
  
  df %<>%  dplyr::left_join(., ( datapackr::map_DataPack_DATIM_DEs_COCs %>% 
                                   dplyr::rename(Age = valid_ages.name,
                                                 Sex = valid_sexes.name,
                                                 KeyPop = valid_kps.name) ), by=c("Age","Sex","KeyPop","indicator_code","support_type"))
  #Check for any data elements which do not have UIDs
  na_dataelement_uids<-dplyr::filter(df,is.na(dataelement)) %>% 
    dplyr::pull(indicator_code) %>% unique()
  if ( length(na_dataelement_uids) > 0 ) {
    flog.warn(paste0("The following indicator codes did not have a data element uid:",paste(na_dataelement_uids,sep="",collapse=",")),name="datapack")
  }
  
  # %>% dplyr::filter(!is.na(dataelement) & !is.na(categoryoptioncombo))
  #Join category option group sets
  df  <- df %>% dplyr::left_join(hiv_inclusive,by="categoryoptioncombouid") %>%
    dplyr::left_join(hiv_specific,by="categoryoptioncombouid")
  
  #Data element group set dimension adornment  
  cached_degs<-paste0(config$deploy_location,"degs_map.rds")
  
  if ( file.access(cached_degs,4) == 0 ) {
    degs_map <-readRDS(cached_degs)
  } else {
    
    data_element_dims <-
      c("HWPJnUTMjEq",
        "LxhLO68FcXm")
    
    degs_map <- purrr::map_dfr(data_element_dims,getDEGSMap) %>% 
      tidyr::spread(type,name,fill=NA) 
    #Remapping of column names
    from<-c("dataElements",
            "Disaggregation.Type", 
            "Technical.Area")
    
    to<-c("dataelement",
          "disagg_type",
          "technical_area")
    
    names(degs_map) <- plyr::mapvalues(names(degs_map),from,to)
  }
  
  d$data$distributedMER <-df %>% 
    dplyr::left_join( degs_map, by = "dataelement") %>%
    dplyr::mutate( hts_modality=stringr::str_replace(hts_modality," FY20R/FY21T$",""))
  
  return(d)
  
}

modalitySummaryChart <- function(df) {
  
  df %>% 
    dplyr::filter(!is.na(hts_modality)) %>%
    dplyr::group_by(resultstatus_inclusive, hts_modality) %>%
    dplyr::summarise(value = sum(value)) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(resultstatus_inclusive, desc(resultstatus_inclusive)) %>% 
    dplyr::mutate(resultstatus_inclusive = factor(resultstatus_inclusive, c("Unknown","Negative", "Positive"))) %>%
    ggplot(aes(
      y = value,
      x = reorder(hts_modality, value, sum),
      fill = resultstatus_inclusive
    )) +
    geom_col() +
    scale_y_continuous(labels = scales::comma) +
    coord_flip() +
    scale_fill_manual(values = c(	"#948d79", "#548dc0", "#59BFB3")) +
    labs(y = "", x = "",
         title = "COP20/FY21 Testing Targets",
         subtitle = "modalities ordered by total tests") +
    theme(legend.position = "bottom",
          legend.title = element_blank(),
          text = element_text(color = "#595959", size = 14),
          plot.title = element_text(face = "bold"),
          axis.ticks = element_blank(),
          panel.background = element_blank(),
          panel.grid.major.x = element_line(color = "#595959"),
          panel.grid.minor.y = element_blank())
  
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
  
  tryCatch({
    foo<-s3$put_object(Bucket = config$s3_bucket,
                       Body = raw_file,
                       Key = object_name,
                       Tagging = object_tags,
                       ContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    flog.info("Datapack Archive sent to S3", name = "datapack")
    
  },
  error = function(err) {
    flog.info("Datapack could not be archived",name = "datapack")
    flog.info(err, name = "datapack")
    showModal(modalDialog(title = "Error",
                          "The DataPack could not be archived."))
  })
  
 
}


saveTimeStampLogToS3<-function(d) {
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

getPSNUList<-function() {
  datapackr::valid_PSNUs %>%
    dplyr::mutate(
      ou_id = purrr::map_chr(ancestors, list("id", 3), .default = NA),
      ou = purrr::map_chr(ancestors, list("name", 3), .default = NA),
      snu1_id = dplyr::if_else(
        condition = is.na(purrr::map_chr(ancestors, list("id",4), .default = NA)),
        true = psnu_uid,
        false = purrr::map_chr(ancestors, list("id",4), .default = NA)),
      snu1 = dplyr::if_else(
        condition = is.na(purrr::map_chr(ancestors, list("name",4), .default = NA)),
        true = psnu,
        false = purrr::map_chr(ancestors, list("name",4), .default = NA))
    ) %>%
    dplyr::select(ou, ou_id, country_name, country_uid, snu1, snu1_id, psnu, psnu_uid)
}

adornPSNUs<-function(d) {
  
  d$data$distributedMER %<>% dplyr::left_join(getPSNUList(), by = c("psnuid" = "psnu_uid"))
  
  prio_defined<-tibble::tribble(
    ~value,~prioritization,
    1,"Scale-Up: Saturation",
    4 ,"Sustained",
    0 ,"No Prioritization",
    6, "Sustained: Commodities" ,
    2, "Scale Up: Aggressive"  ,   
    5, "Centrally Supported",
    7,  "Attained" ,
    8, "Not PEPFAR Supported" )
  
  
  #We need to add the prioritization as a dimension here
  prio <- d$data$SUBNAT_IMPATT %>% 
    dplyr::filter(indicator_code == "IMPATT.PRIORITY_SNU.T") %>% 
    dplyr::select(psnuid,value) %>% 
    dplyr::left_join(prio_defined,by="value") %>% 
    dplyr::select(-value)
  
  d$data$distributedMER %<>% dplyr::left_join(prio,by="psnuid") %>% 
    dplyr::mutate(prioritization = case_when(is.na(prioritization) ~ "No Prioritization",
                                             TRUE ~ prioritization ))
  
  d
  
}

prepareFlatMERExport<-function(vr) {
  
  vr$data$distributedMER %>% 
    dplyr::mutate(timestamp = format(Sys.time(),"%Y-%m-%d %H:%M:%S"),
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
                   funding_agency  = agency,
                   fiscal_year,
                   dataelement_id  = dataelement,
                   dataelement_name = dataelement.y,
                   indicator = technical_area,
                   numerator_denominator ,
                   support_type ,
                   hts_modality ,
                   categoryoptioncombo_id = categoryoptioncombouid,
                   categoryoptioncombo_name = categoryoptioncombo,
                   age = Age,
                   sex = Sex, 
                   key_population = KeyPop,
                   result_value = resultstatus, 
                   target_value = value,
                   timestamp,
                   disagg_type,
                   resultstatus_inclusive)
}

sendMERDataToPAW<-function(vr,config) {
  #Write the flatpacked output
  tmp <- tempfile()
  mer_data<-prepareFlatMERExport(vr)

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
  object_tags<-vr$info[names(vr$info) %in% tags] 
  object_tags<-URLencode(paste(names(object_tags),object_tags,sep="=",collapse="&"))
  object_name<-paste0("processed/",vr$info$sane_name,".csv")
  s3<-paws::s3()
  
  tryCatch({
    foo<-s3$put_object(Bucket = config$s3_bucket,
                       Body = raw_file,
                       Key = object_name,
                       Tagging = object_tags,
                       ContentType = "text/csv")
    flog.info("Flatpack sent to AP", name = "datapack")
    showModal(
      modalDialog(
        "The DataPack has been delivered to PAW.",
        easyClose = TRUE,
        footer = NULL
      )
    )
    shinyjs::disable("send_paw")
  },
  error = function(err) {
    flog.info("Flatpack cannot be sent to AP",name = "datapack")
    flog.info(err, name = "datapack")
    showModal(modalDialog(title = "Error",
                          "The DataPack cannot be delivered to PAW."))
  })
  
  unlink(tmp)
}

validationSummary<-function(vr,config) {
  
  
  sheets_out_of_order<-sum(vr$tests$sheets_check == FALSE)
  
  columns_out_of_order<-purrr::map(vr$tests$col_check,function(x) purrr::pluck(x,"order_check")) %>% 
    purrr::map(.,function(x) !x) %>% 
    purrr::map(.,function(x) sum(x)) %>% 
    tibble::enframe() %>% 
    tidyr::unnest(cols=c(value)) %>% 
    dplyr::filter(value != 0) %>% 
    NROW(.)
  
  non_numeric<-length(vr$tests$non_numeric)
  
  validation_rule_issues<-NROW(vr$datim$vr_rules_check)
  
  invalid_dedupes<-NROW(vr$tests$invalid_dedupes)
  
  invalid_DSDTA<-NROW(vr$tests$invalid_DSDTA)
  negative_distributed_targets<-NROW(vr$tests$negative_distributed_targets)
  imbalancedDistribution<-NROW(vr$tests$imbalancedDistribution)
  no_targets<-NROW(vr$tests$noTargets)
  undistributed<-NROW(vr$tests$undistributed)
  
  validation_summary<-tibble::tribble(~validation_issue_category, ~count,
                                      "Non-numeric values", non_numeric,
                                      "Sheets out of order", sheets_out_of_order,
                                      "Sheets with columns out of order", columns_out_of_order,
                                      "Validation rule issues",validation_rule_issues,
                                      "Invalid DSD/TA", invalid_DSDTA,
                                      "Negative distributed targets",negative_distributed_targets,
                                      "Imbalanced distribution",imbalancedDistribution,
                                      "Targets with missing distribution",no_targets,
                                      "Invalid dedupes",invalid_dedupes,
                                      "Undistributed targets",undistributed)
  validation_summary %<>%
    mutate(ou = vr$info$datapack_name,
           ou_id = vr$info$country_uids,
           country_name = vr$info$datapack_name,
           country_uid = vr$info$country_uids )
  
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
  
  object_name<-paste0("Validation_Error/",vr$info$sane_name,".csv")
  s3<-paws::s3()
  
  r<-tryCatch({
    foo<-s3$put_object(Bucket = config$s3_bucket,
                       Body = raw_file,
                       Key = object_name,
                       Tagging = object_tags,
                       ContentType = "text/csv")
    
    return(TRUE)
  },
  error = function(err) {
    flog.info("Validation summary could not be sent to AP",name = "datapack")
    flog.info(err, name = "datapack")
    return(FALSE)
  })
  
  unlink(tmp)
  
  return(r)
  
}

validationSummaryUI<-function(r) {
  if (!r) {
    flog.error("Validation summary could not be sent to AP",name = "datapack")
    showModal(modalDialog(title = "Error",
                          "Validation summary could not be sent to AP."))
  } else {
    flog.info("Validation summary  sent to AP",name = "datapack")
  }
}