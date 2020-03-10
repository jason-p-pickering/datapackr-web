PSNUxIM_pivot<-function(d){
  
  pivot<- d  %>% 
    purrr::pluck("data") %>% 
    purrr::pluck("analytics") %>% 
    dplyr::select(indicator,
                  dataelement_name,
                  psnu,
                  mechanism_code,
                  partner= partner_desc,
                  agency = funding_agency,
                  value = target_value) %>% 
    dplyr::group_by(indicator,dataelement_name,psnu,mechanism_code,partner,agency) %>% 
    dplyr::summarise(value = sum(value)) %>% 
    dplyr::ungroup() %>% 
    dplyr::mutate(mechanism_code = ifelse(mechanism_code == "HllvX50cXC0","default",mechanism_code))
  
  rpivotTable(data =   pivot   ,  rows = c( "dataelement_name"),
              vals = "value", aggregatorName = "Integer Sum", rendererName = "Table"
              , width="70%", height="700px")
}

modalitySummaryChart <- function(df) {
  
  df %>% 
    dplyr::filter(!is.na(hts_modality)) %>%
    dplyr::filter(resultstatus_specific != "Known at Entry Positive") %>% 
    dplyr::group_by(resultstatus_inclusive, hts_modality) %>%
    dplyr::summarise(value = sum(target_value)) %>%
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

modalitySummaryTable<-function(df){
  
  hts<- df %>% 
    dplyr::filter(!is.na(hts_modality)) %>%
    dplyr::filter(resultstatus_specific != "Known at Entry Positive") %>% 
    dplyr::group_by(resultstatus_inclusive, hts_modality) %>%
    dplyr::summarise(value = sum(target_value)) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(resultstatus_inclusive, desc(resultstatus_inclusive)) %>% 
    dplyr::mutate(resultstatus_inclusive = factor(resultstatus_inclusive, c("Unknown","Negative", "Positive"))) %>% 
    tidyr::pivot_wider(names_from = resultstatus_inclusive, values_from = value ) %>% 
    dplyr::mutate(yield = Positive/(Negative + Positive) * 100,
                  modality_share = Positive / sum(Positive) * 100 ,
                  Total = Positive + Negative) %>% 
    dplyr::select(hts_modality,Positive,Total,yield,modality_share)
  
  hts_total<- hts %>% 
    dplyr::select(Positive,Total) %>% 
    dplyr::mutate(hts_modality = "Total") %>% 
    dplyr::group_by(hts_modality) %>% 
    dplyr::summarise_all(sum) %>% 
    dplyr::mutate(yield = Positive/Total * 100,
                  modality_share = 100)
  
  dplyr::bind_rows(hts,hts_total)
  
}

modalityYieldChart <- function(df) {
  
  df <- modalitySummaryTable(df)
  
  x_lim <- max(df$yield)
  
  df %>%
    ggplot(aes(
      y = yield,
      x = reorder(hts_modality, yield)
    )) +
    geom_col(fill="#67A9CF") +
    geom_text(aes(label = percent(yield,accuracy=0.1,scale=1),hjust=-0.25)) +
    scale_y_continuous(limits = c(0,x_lim*1.1),labels = percent_format(accuracy=1,scale=1)) +
    coord_flip() +
    scale_fill_manual(values = c("#2166AC")) +
    labs(y = "", x = "",
         title = "COP20/FY21 Testing Yields",
         subtitle = "Modalities ordered by yield rates") +
    theme(legend.position = "bottom",
          legend.title = element_blank(),
          text = element_text(color = "#595959", size = 14),
          plot.title = element_text(face = "bold"),
          axis.ticks = element_blank(),
          panel.background = element_blank(),
          panel.grid.major.x = element_line(color = "#595959"),
          panel.grid.minor.y = element_blank())
  
}

recencyComparison <- function(d) {
  hts_mechs <-
    structure(
      .Data = list(
        indicator_code = c(
          "HTS_INDEX_COM.N.Age_Sex_Result.T.NewPos",
          "HTS_INDEX_COM.N.Age_Sex_Result.T.NewNeg",
          "HTS_INDEX_FAC.N.Age_Sex_Result.T.NewPos",
          "HTS_INDEX_FAC.N.Age_Sex_Result.T.NewNeg",
          "HTS_TST_Inpat.N.Age_Sex_Result.T.Positive",
          "HTS_TST_Inpat.N.Age_Sex_Result.T.Negative",
          "HTS_TST_Pediatric.N.Age_Sex_Result.T.Positive",
          "HTS_TST_Pediatric.N.Age_Sex_Result.T.Negative",
          "HTS_TST_Malnutrition.N.Age_Sex_Result.T.Positive",
          "HTS_TST_Malnutrition.N.Age_Sex_Result.T.Negative",
          "TB_STAT.N.Age_Sex_KnownNewPosNeg.T.NewPos",
          "TB_STAT.N.Age_Sex_KnownNewPosNeg.T.NewNeg",
          "PMTCT_STAT.N.Age_Sex_KnownNewResult.T.NewPos",
          "PMTCT_STAT.N.Age_Sex_KnownNewResult.T.NewNeg",
          "HTS_TST_PMTCTPostANC1.N.Age_Sex_Result.T.Positive",
          "HTS_TST_PMTCTPostANC1.N.Age_Sex_Result.T.Negative",
          "VMMC_CIRC.N.Age_Sex_HIVStatus.T.Positive",
          "VMMC_CIRC.N.Age_Sex_HIVStatus.T.Negative",
          "HTS_TST_STIClinic.N.Age_Sex_Result.T.Positive",
          "HTS_TST_STIClinic.N.Age_Sex_Result.T.Negative",
          "HTS_TST_EmergencyWard.N.Age_Sex_Result.T.Positive",
          "HTS_TST_EmergencyWard.N.Age_Sex_Result.T.Negative",
          "HTS_TST_OtherPITC.N.Age_Sex_Result.T.Positive",
          "HTS_TST_OtherPITC.N.Age_Sex_Result.T.Negative",
          "HTS_TST_VCT.N.Age_Sex_Result.T.Positive",
          "HTS_TST_VCT.N.Age_Sex_Result.T.Negative",
          "HTS_TST_MobileMod.N.Age_Sex_Result.T.Positive",
          "HTS_TST_MobileMod.N.Age_Sex_Result.T.Negative",
          "HTS_TST_OtherMod.N.Age_Sex_Result.T.Positive",
          "HTS_TST_OtherMod.N.Age_Sex_Result.T.Negative",
          "HTS_RECENT_IndexMod.N.Age_Sex_Result.T",
          "HTS_RECENT_Index.N.Age_Sex_Result.T",
          "HTS_RECENT_Inpat.N.Age_Sex_Result.T",
          "HTS_RECENT_TB.N.Age_Sex_Result.T",
          "HTS_RECENT_PMTCT.N.Age_Sex_Result.T",
          "HTS_RECENT_PMTCTPostANC1.N.Age_Sex_Result.T",
          "HTS_RECENT_VMMC.N.Age_Sex_Result.T",
          "HTS_RECENT_STIClinic.N.Age_Sex_Result.T",
          "HTS_RECENT_Emergency.N.Age_Sex_Result.T",
          "HTS_RECENT_OtherPITC.N.Age_Sex_Result.T",
          "HTS_RECENT_VCT.N.Age_Sex_Result.T",
          "HTS_RECENT_MobileMod.N.Age_Sex_Result.T",
          "HTS_RECENT_OtherMod.N.Age_Sex_Result.T"
        ),
        hts_recency_compare = c(
          "Community - Index",
          "Community - Index",
          "Facility - Index",
          "Facility - Index",
          "Facility - Inpatient",
          "Facility - Inpatient",
          "Facility - Pediatric",
          "Facility - Pediatric",
          "Facility - Malnutrition",
          "Facility - Malnutrition",
          "Facility - TB Clinic",
          "Facility - TB Clinic",
          "Facility - PMTCT ANC1 Only",
          "Facility - PMTCT ANC1 Only",
          "Facility - PMTCT Post ANC1",
          "Facility - PMTCT Post ANC1",
          "Facility - VMMC",
          "Facility - VMMC",
          "Facility - STI Clinic",
          "Facility - STI Clinic",
          "Facility - Emergency Ward",
          "Facility - Emergency Ward",
          "Facility - Other PITC",
          "Facility - Other PITC",
          "Facility - VCT",
          "Facility - VCT",
          "Community - Mobile",
          "Community - Mobile",
          "Community - Other Services",
          "Community - Other Services",
          "Community - Index",
          "Facility - Index",
          "Facility - Inpatient",
          "Facility - TB Clinic",
          "Facility - PMTCT ANC1 Only",
          "Facility - PMTCT Post ANC1",
          "Facility - VMMC",
          "Facility - STI Clinic",
          "Facility - Emergency Ward",
          "Facility - Other PITC",
          "Facility - VCT",
          "Community - Mobile",
          "Community - Other Services"
        )
      ),
      names = c("indicator_code", "hts_recency_compare"),
      row.names = c(NA, 43L),
      class = "data.frame"
    )
  
  indicator_map <-
    datapackr::map_DataPack_DATIM_DEs_COCs[, c("dataelement", "indicator_code")] %>%
    dplyr::distinct() %>%
    dplyr::rename(dataelement_id = dataelement)
  
  hts_recency_map <- dplyr::inner_join(indicator_map, hts_mechs) %>%
    dplyr::select(dataelement_id, hts_recency_compare)
  
  df <- d %>%
    purrr::pluck(., "data") %>%
    purrr::pluck(., "analytics") %>%
    dplyr::inner_join(hts_recency_map , by = "dataelement_id") %>%
    dplyr::filter(resultstatus_inclusive == "Positive") %>%
    dplyr::filter(!(
      resultstatus_specific %in% c("Known at Entry Positive", "Status Unknown")
    )) %>%
    dplyr::group_by(hts_recency_compare, indicator) %>%
    dplyr::summarise(value = sum(target_value)) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(indicator, desc(indicator)) %>%
    dplyr::mutate(indicator = ifelse(indicator == "HTS_RECENT", "HTS_RECENT", "HTS_TST")) %>%
    dplyr::mutate(indicator = factor(
      indicator,
      c(
        "HTS_TST",
        "HTS_INDEX",
        "HTS_RECENT",
        "PMTCT_STAT",
        "TB_STAT"
      )
    )) %>%
    dplyr::rename(technical_area = indicator) %>%
    tidyr::pivot_wider(names_from = technical_area, values_from = value,
                       values_fill = list(value = 0))
  
  can_proceed <- NROW(df) > 0 & 
    dplyr::setequal(names(df),c("hts_recency_compare","HTS_TST","HTS_RECENT"))
  
  if ( !can_proceed ) {
    return(NULL)
  } else  {
    df %>%
      dplyr::select("Modality" = hts_recency_compare,
                    HTS_RECENT,
                    "HTS_TST_POS" = HTS_TST) %>%
      dplyr::arrange(Modality) %>%
      dplyr::mutate("HTS_RECENT (%)" = HTS_RECENT / HTS_TST_POS * 100) %>%
      dplyr::mutate(
        HTS_RECENT = format(HTS_RECENT , big.mark = ',', scientific = FALSE),
        HTS_TST_POS = format(HTS_TST_POS , big.mark = ',', scientific = FALSE),
        `HTS_RECENT (%)` = format(round(`HTS_RECENT (%)`, 2), nsmall = 2)
      )
  } 
}

subnatPyramidsChart <- function(d,epi_graph_filter_results){
  
  indicator_map<- datapackr::map_DataPack_DATIM_DEs_COCs[,c("dataelement","indicator_code")] %>% 
    dplyr::distinct() %>% 
    dplyr::rename(dataelement_id = dataelement)
  
  df <- d %>%
    purrr::pluck(.,"data") %>%
    purrr::pluck(.,"analytics") 
  
  if (is.null(df)) {return(NULL)}
  
  if( length(epi_graph_filter_results) > 0 & !is.null(epi_graph_filter_results)) {
    df %<>% dplyr::filter(snu1 %in% epi_graph_filter_results )
  }
  
  if ( NROW(df) == 0 ) { return(NULL) }
  
  df %<>%
    dplyr::inner_join( indicator_map , by = "dataelement_id") %>% 
    dplyr::filter(indicator_code == "TX_CURR.N.Age_Sex_HIVStatus.T" | 
                    indicator_code == "TX_PVLS.N.Age_Sex_Indication_HIVStatus.T.Routine"  | 
                    indicator_code == "PLHIV.NA.Age/Sex/HIVStatus.T") %>%
    dplyr::select(age,sex,indicator_code,target_value) %>% 
    dplyr::group_by(age,sex,indicator_code) %>%
    dplyr::summarise(value = sum(target_value)) %>%
    dplyr::ungroup() %>%
    dplyr::rename(Age = age,
                  Sex = sex) %>% 
    dplyr::arrange(indicator_code, desc(indicator_code)) %>%
    dplyr::mutate(indicator_code = ifelse(
      indicator_code == "PLHIV.NA.Age/Sex/HIVStatus.T","PLHIV",ifelse(
        indicator_code == "TX_CURR.N.Age_Sex_HIVStatus.T","TX_CURR",ifelse(
          indicator_code == "TX_PVLS.N.Age_Sex_Indication_HIVStatus.T.Routine","TX_PVLS",NA
        )
      ) 
    )
    ) 
  
  if ( NROW(df) == 0 ) {return(NULL)}
  
  y_lim<-max(df$value)
  
  df %>%
    ggplot(aes(x = Age, y = value, fill = indicator_code)) +
    geom_bar(data = df %>% dplyr::filter( Sex == "Female") %>% dplyr::arrange(indicator_code),
             stat = "identity",
             position = "identity") +
    geom_bar(data = df %>% dplyr::filter(Sex == "Male") %>% dplyr::arrange(indicator_code),
             stat = "identity",
             position = "identity",
             mapping = aes(y = -value)) +
    coord_flip() +
    labs( x = "", y = "\u2190 Males | Females \u2192",
          title = "COP20/FY21 Epidemic Cascade Age & Sex Pyramid",
          subtitle = "Comparison of Population with HIV, on Treatment, and Virally Suppressed") +
    geom_hline(yintercept = 0, size=1) +
    scale_fill_manual(values = c(	"#B2182B", "#EF8A62","#67A9CF")) +
    scale_y_continuous(limits = c(-y_lim,y_lim), labels = function(x){scales::comma(abs(x))}) +
    theme(legend.position = "bottom",
          legend.title = element_blank(),
          text = element_text(color = "#595959", size =14),
          plot.title = element_text(face = "bold"),
          axis.ticks = element_blank(),
          panel.background = element_blank(),
          panel.grid.major.x = element_line(color = "#595959"),
          panel.grid.minor.y = element_blank())
  
}

kpCascadeChart <- function(d,kpCascadeInput_filter){
  
  df <- d %>%
    purrr::pluck(.,"data") %>%
    purrr::pluck(.,"analytics") 
  
  if (is.null(df)) {return(NULL)}
  
  if( length(kpCascadeInput_filter) > 0 & !is.null(kpCascadeInput_filter)) {
    df %<>% dplyr::filter(snu1 %in% kpCascadeInput_filter )
  }
  
  if ( NROW(df) == 0 ) { return(NULL) }
  
  
  df %<>%
    dplyr::filter(dataelement_name == "IMPATT.PLHIV (N, SUBNAT, Age/Sex/HIVStatus) TARGET:" | 
                    dataelement_name == "KP_ESTIMATES (N, SUBNAT, PositiveEstimate/HIVStatus) TARGET: Estimated Key Pop" |
                    dataelement_name == "TX_CURR (N, DSD, Age/Sex/HIVStatus) TARGET: Receiving ART" | 
                    dataelement_name == "TX_CURR (N, DSD, KeyPop/HIVStatus) TARGET: Receiving ART" |
                    dataelement_name == "TX_PVLS (N, DSD, Age/Sex/Indication/HIVStatus) TARGET: Viral Load Documented"  | 
                    dataelement_name == "TX_PVLS (N, DSD, KeyPop/HIVStatus) TARGET: Viral Load Documented"
    ) %>%
    dplyr::mutate(indicator = ifelse(indicator == "KP_ESTIMATES","PLHIV",indicator)) %>%
    dplyr::mutate(kp = ifelse(is.na(key_population),"GenPop","KeyPop")) %>%
    dplyr::select(indicator,kp,target_value) %>%
    dplyr::group_by(indicator,kp) %>%
    dplyr::summarise(value = sum(target_value)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(lbl = paste0(indicator,".",kp))
  
  if ( NROW(df) == 0 ) {return(NULL)}
  
  y_lim<-max(df$value)
  
  df %>%
    ggplot(aes(x = indicator, y = value, fill = lbl)) +
    geom_bar(data = df,
             stat = "identity",
             position = "identity") +
    geom_text(aes(label = scales::comma(value),vjust=-0.25)) +
    labs( x = "", y = "",
          title = "COP20/FY21 Epidemic Cascade Age & Sex Pyramid",
          subtitle = "Comparison of General and Key Populations with HIV, on Treatment, and Virally Suppressed") +
    geom_hline(yintercept = 0, size=1) +
    scale_fill_manual(values = c("#ceb966","#9cb084","#6bb1c9","#6585cf","#7e6bc9","#a379bb")) +
    scale_y_continuous(limits = c(0,y_lim*1.1), labels = scales::comma) +
    theme(legend.position = "bottom",
          legend.title = element_blank(),
          text = element_text(color = "#595959", size =14),
          plot.title = element_text(face = "bold"),
          axis.ticks = element_blank(),
          panel.background = element_blank(),
          panel.grid.minor.x = element_blank(),
          panel.grid.major.y = element_line(color = "#595959"))
  
}

vlsTestingChart <- function(df) {
  
  if (is.null(df)) {return(NULL)}
  
  df %<>%
    dplyr::filter(indicator == "TX_CURR" | 
                    indicator == "TX_PVLS") %>%
    dplyr::select(SNU1 = snu1,indicator,numerator_denominator,target_value) %>% 
    dplyr::mutate(indicator = ifelse(
      indicator == "TX_CURR","TX_CURR",ifelse(
        indicator == "TX_PVLS" & numerator_denominator == "Numerator","TX_PVLS (N)",ifelse(
          indicator == "TX_PVLS" & numerator_denominator == "Denominator","TX_PVLS (D)",NA
        )))) %>%
    dplyr::mutate(SNU1 = ifelse(substr(SNU1,0,9)=="_Military","Military",SNU1)) %>%
    dplyr::group_by(SNU1,indicator) %>%
    dplyr::summarise(value = sum(target_value)) %>%
    dplyr::mutate(freq = value/max(value)) %>%
    dplyr::mutate(sort_col = min(freq)) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(desc(sort_col),indicator)
  
  if ( NROW(df) == 0 ) {return(NULL)}
  
  y_lim <- (min(df$freq)%/%.1)/10
  
  df %>%
    ggplot(aes(x = reorder(SNU1,sort_col), y = freq, fill = indicator)) +
    geom_bar(data = df,
             stat = "identity",
             position = "identity") +
    coord_flip(ylim=c(y_lim,1)) +
    labs( x = "", y = "",
          title = "COP20/FY21 Viral Load Testing Coverage",
          subtitle = "Percentage of Population Currently on Treatment Eligible and Targeted for VLS Testing") +
    geom_hline(yintercept = 0, size=1) +
    scale_fill_manual(values = c(	"#B2182B", "#EF8A62","#67A9CF")) +
    scale_y_continuous(labels = percent) +
    theme(legend.position = "bottom",
          legend.title = element_blank(),
          text = element_text(color = "#595959", size =14),
          plot.title = element_text(face = "bold"),
          axis.ticks = element_blank(),
          panel.background = element_blank(),
          panel.grid.major.x = element_line(color = "#595959"),
          panel.grid.minor.y = element_blank())
  
}

snuSelector <- function(df){
  
  if (!inherits(df,"error") & !is.null(df)){
    df  %>% 
      purrr::pluck(.,"data") %>%
      purrr::pluck(.,"analytics") %>%
      purrr::pluck(.,"snu1") %>% 
      unique()
    
  } else {
    NULL
  }
  
}
