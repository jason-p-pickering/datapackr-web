require(datapackr)
require(scales)
options(shiny.maxRequestSize = 100 * 1024 ^ 2)
options("baseurl" = "http://127.0.0.1:8080/")

DHISLogin <- function(baseurl, username, password) {
  httr::set_config(httr::config(http_version = 0))
  url <- URLencode(URL = paste0(getOption("baseurl"), "api/me"))
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
  #Filter any zeros
  d$data$MER %<>% dplyr::filter( value != 0 )
  d$data$SUBNAT_IMPATT %<>% dplyr::filter( value != 0 )
  d$data$SNUxIM %<>% dplyr::filter( distribution != 0 )
  d$data$distributedMER %<>% dplyr::filter( value != 0 )
  
  d
}

validatePSNUData <- function(d) {
  #Validation rule checking
  vr_data <- d$datim$PSNUxIM %>%
  dplyr::mutate(value = datapackr::round_trunc(value)) %>%
  dplyr::filter(value != 0)
  
  
  round_trunc <- function(x) {
    trunc(abs(x) + 0.5) * sign(x)
  }
  
  names(vr_data) <- c("dataElement",
                      "period",
                      "orgUnit",
                      "categoryOptionCombo",
                      "attributeOptionCombo",
                      "value")
  
  # We need ALL mechanisms to be in DATIM before remapping....TODO
  # vr_data$attributeOptionCombo <-
  #   datimvalidation::remapMechs(vr_data$attributeOptionCombo,
  #                               getOption("organisationUnit"),
  #                               "code",
  #                               "id")
  datasets_uid <- c("nIHNMxuPUOR", "sBv1dj90IX6")
  if ( Sys.info()["sysname"] == "Linux") {
    ncores <- parallel::detectCores() - 1
    doMC::registerDoMC( cores = ncores )
    is_parallel <- TRUE
  } else {
    is_parallel <- FALSE
  } 
  vr_violations <- datimvalidation::validateData(vr_data,
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
   
  if ( NROW(vr_violations) > 0 ) {
    d$datim$vr_rules_check <- vr_violations  %>%
      dplyr::select(name,ou_name,mech_code,formula,diff) %>%
      dplyr::mutate(name = gsub(pattern = " DSD,","",name))     
  } else {
    d$datim$vr_rules_check <- NULL
    return(d)
  }
 
  d
  
}

#TODO: Move this back to the DataPackr....
validateMechanisms<-function(d) {
  
  vr_data <- d$datim$PSNUxIM %>%
    dplyr::pull(mechanismCode) %>%
    unique()
  
  #TODO: Removve hard coding of time periods and 
  #filter for the OU as well
  mechs<-getMechanismView() %>%
    dplyr::filter(!is.na(startdate)) %>%
    dplyr::filter(!is.na(enddate)) %>%
    dplyr::filter(startdate <= as.Date('2019-10-01')) %>%
    dplyr::filter(enddate >= as.Date('2020-09-30')) %>%
    dplyr::pull(mechanismCode)
  
  #Allow for dedupe
  mechs <- append("00000",mechs)
  
  bad_mechs<-vr_data[!(vr_data %in% mechs)]
  
  if (length(bad_mechs) > 0 ) {
    
    msg <- paste0("ERROR!: Invalid mechanisms found in the PSNUxIM tab. 
                  These MUST be reallocated to a valid mechanism
                  ",paste(bad_mechs,sep="",collapse=","))
    d$info$warningMsg<-append(msg,d$info$warningMsg)
    d$info$had_error<-TRUE
  }
  
  d
  
}

getMechanismView<-function() {
  cached_mechs <- "/srv/shiny-server/apps/datapack/mechs.rds"
  
  if ( file.access(cached_mechs,4) == 0 ) {
    
    mechs <-readRDS(cached_mechs)
    
  } else {
    
    mechs <- paste0(getOption("baseurl"), "api/sqlViews/fgUtV6e9YIX/data.csv") %>%
      httr::GET() %>%
      httr::content(., "text") %>%
      readr::read_csv(col_names = TRUE) %>%
      dplyr::select(mechanismCode = "code", partner, agency, ou,startdate,enddate)
  }
  
  mechs
}

adornMechanisms <- function(d) {
  
  mechs<-getMechanismView()
  dplyr::left_join( d , mechs, by = "mechanismCode" )

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
    tidyr::unnest() %>%
    dplyr::distinct() %>%
    dplyr::mutate(type=make.names(r$name))

}

generateMechanismMap<-function() {
  
  
  mechs<- paste0(getOption("baseurl"),"api/categoryOptionCombos?filter=categoryCombo.id:eq:wUpfppgjEza&fields=code,name,id,categoryOptions[startDate,endDate,organisationUnits[id,name]]&paging=false") %>% 
    URLencode(.) %>%
    httr::GET(., httr::timeout(60)) %>%
    httr::content(.,"text") %>%
    jsonlite::fromJSON(., flatten = TRUE) %>%
    purrr::pluck(.,"categoryOptionCombos")
  
  mech_has_ou <- mechs %>% 
    purrr::pluck(.,"categoryOptions") %>%
    purrr::map(.,"organisationUnits") %>%
    purrr::flatten(.) %>% 
    purrr::map(., function(x) { class(x) == "data.frame" }) %>%
    unlist(.)
  
  mechs %<>% dplyr::filter(mech_has_ou) 
    
  mechs$startDate <-
    as.Date(sapply(mechs$categoryOptions, function(x)
      ifelse(is.null(x$startDate), "1900-01-01", x$startDate)),
      "%Y-%m-%d")
  mechs$endDate <-
    as.Date(sapply(mechs$categoryOptions, function(x)
      ifelse(is.null(x$endDate), "1900-01-01", x$endDate)),
      "%Y-%m-%d")
  

  mechs_ous<- mechs %>% 
    purrr::pluck(.,"categoryOptions") %>%
    purrr::map(.,"organisationUnits") %>%
    purrr::flatten_dfr(.,.id="foo") %>%
    dplyr::select("operating_unit"=name,
                  "operating_unit_uid"=id)
  
   mechs %>% dplyr::select(-categoryOptions) %>%
    dplyr::bind_cols(.,mechs_ous)
  
  
}

#Used with permission from @achafetz  
#https://github.com/USAID-OHA-SI/tameDP/blob/master/R/clean_indicators.R

adornMERData <- function(df) {
  
  
  hiv_specific<-getCOGSMap("bDWsPYyXgWP") %>%
    purrr::pluck("dimension_map") %>%
    dplyr::select("categoryoptioncombouid"=coc_uid,
                  "resultstatus"=category_option_group_name) %>%
    dplyr::mutate(resultstatus = stringr::str_replace(resultstatus,"\\(Specific\\)","")) %>%
    dplyr::mutate(resultstatus = stringr::str_replace(resultstatus,"HIV","")) %>%
    dplyr::mutate(resultstatus = stringr::str_trim(resultstatus))
    
  
  hiv_inclusive<-getCOGSMap("ipBFu42t2sJ") %>%
  purrr::pluck("dimension_map") %>%
    dplyr::select("categoryoptioncombouid"=coc_uid,
                  "resultstatus_inclusive"=category_option_group_name) %>%
  dplyr::mutate(resultstatus_inclusive = stringr::str_replace(resultstatus_inclusive,"\\(Inclusive\\)","")) %>%
    dplyr::mutate(resultstatus_inclusive = stringr::str_replace(resultstatus_inclusive,"HIV","")) %>%
    dplyr::mutate(resultstatus_inclusive = stringr::str_replace(resultstatus_inclusive,"Status","")) %>%
    dplyr::mutate(resultstatus_inclusive = stringr::str_trim(resultstatus_inclusive))

   df %<>% dplyr::left_join(datapackr::PSNUxIM_to_DATIM %>%
                     dplyr::filter(dataset == "MER") %>%
                     dplyr::select(-sheet_name, -typeOptions, -dataset),
                   by = c("indicatorCode" = "indicatorCode",
                          "Age" = "validAges",
                          "Sex" = "validSexes",
                          "KeyPop" = "validKPs")) %>%
     dplyr::filter(!is.na(dataelementuid) & !is.na(categoryoptioncombouid))
 
   #Join category option group sets
   df  <- df %>% dplyr::left_join(hiv_inclusive,by="categoryoptioncombouid") %>%
     dplyr::left_join(hiv_specific,by="categoryoptioncombouid")
   
   #Data element group set dimension adornment  
  cached_degs<-"/srv/shiny-server/apps/datapack/degs_map.rds"
  
   if ( file.access(cached_degs,4) == 0 ) {
    degs_map <-readRDS(cached_degs)
  } else {
    
    data_element_dims <-
      c("HWPJnUTMjEq",
        "lD2x0c8kywj",
        "LxhLO68FcXm",
        "TWXpUVE2MqL",
        "Jm6OwL9IqEa")
  
      degs_map <- purrr::map_dfr(data_element_dims,getDEGSMap) %>% 
      tidyr::spread(type,name,fill=NA) 
      #Remapping of column names
      from<-c("dataElements",
              "Disaggregation.Type", 
              "HTS.Modality..USE.ONLY.for.FY19.Results.FY20.Targets.",
              "Numerator...Denominator",
              "Support.Type",
              "Technical.Area")
      
      to<-c("dataElements",
            "disagg_type",
            "hts_modality",
            "numerator_denominator",
            "support_type",
            "technical_area")
      
      names(degs_map) <- plyr::mapvalues(names(degs_map),from,to)
  }
  
  df %>% 
    dplyr::left_join( degs_map, by = c("dataelementuid" = "dataElements")) %>%
    dplyr::mutate( hts_modality=stringr::str_replace(hts_modality," FY19R/FY20T$",""))


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
         title = "COP19/FY20 Testing Targets",
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