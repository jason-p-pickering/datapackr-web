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
  
  vr_violations<-vr_violations[ vr_violations$id %in% rules_to_keep, ]
  
  diff <- gsub(" <= ", "/", vr_violations$formula)
  
  vr_violations$diff <-
    sapply( diff, function(x) {
      round( ( eval( parse( text = x) ) - 1) * 100, 2 )
    })
  

  d$datim$vr_rules_check <- vr_violations %>% dplyr::filter(diff >= 5) %>%
    dplyr::select(name,ou_name,mech_code,formula,diff) %>%
    dplyr::mutate(name = gsub(pattern = " DSD,","",name)) 

 d
  
}

adornMechanisms <- function(d) {
  
  cached_mechs <- "/srv/shiny-server/apps/datapack/mechs.rds"
  
  if ( file.access(cached_mechs,4) == 0 ) {
    
    mechs <-readRDS(cached_mechs)

  } else {
    
  mechs <- paste0(getOption("baseurl"), "api/sqlViews/fgUtV6e9YIX/data.csv") %>%
      httr::GET() %>%
      httr::content(., "text") %>%
      readr::read_csv(col_names = TRUE) %>%
      dplyr::select(mechanismCode = "code", partner, agency, ou)
  }
  
  dplyr::left_join( d , mechs, by = "mechanismCode" )

}

getDEGSMap <- function(uid) {
  
    r <- paste0(getOption("baseurl"),"api/dataElementGroupSets/",uid,"?fields=id,name,dataElementGroups[name,dataElements[id]]") %>%
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

#Used with permission from @achafetz  
#https://github.com/USAID-OHA-SI/tameDP/blob/master/R/clean_indicators.R

adornMERData <- function(df) {
  
  suppressWarnings( df <- df  %>% tidyr::separate(
    indicatorCode,
    into = c(
      "indicator",
      "numeratordenom",
      "disaggregate",
      NA,
      "otherdisaggregate"
    ),
    sep = "\\.", 
    remove = FALSE ) %>%    dplyr::mutate(
      resultstatus = dplyr::case_when(
        otherdisaggregate %in% c("NewPos", "KnownPos", "Positive") ~ "Positive",
        otherdisaggregate %in% c("NewNeg", "Negative")             ~ "Negative",
        otherdisaggregate == "Unknown"                             ~ "Unknown"
      )
    ))
  
   df <- df %>%  dplyr::left_join(datapackr::PSNUxIM_to_DATIM %>%
                     dplyr::filter(dataset == "MER") %>%
                     dplyr::select(-sheet_name, -typeOptions, -dataset),
                   by = c("indicatorCode" = "indicatorCode",
                          "Age" = "validAges",
                          "Sex" = "validSexes",
                          "KeyPop" = "validKPs"))
   
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
  
      degs_map <- purrr::map_dfr(data_element_dims,dimensionMap) %>% 
      tidyr::spread(type,name,fill=NA) 
  }
  
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
  
  df <- df %>% 
    dplyr::left_join( df, degs_map, by = c("dataelementuid" = "dataElements")) %>%
    dplyr::mutate(operating_unit=d$info$datapack_name,
                  hts_modality=stringr::str_replace(hts_modality," FY19R/FY20T$",""))
  
  warning(names(df))
  return(df) 

}
  
modalitySummaryChart <- function(df) {

   df %>% 
    dplyr::filter(!is.na(hts_modality)) %>%
    dplyr::group_by(hts_modality, resultstatus) %>%
    dplyr::summarise(value = sum(value)) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(hts_modality, desc(resultstatus)) %>% 
    dplyr::mutate(resultstatus = factor(resultstatus, c("Unknown","Negative", "Positive"))) %>%
    ggplot(aes(
      y = value,
      x = reorder(hts_modality, value, sum),
      fill = resultstatus
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