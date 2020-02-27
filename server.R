
library(shiny)
library(shinyjs)
require(magrittr)
require(dplyr)
require(datimvalidation)
require(ggplot2)
require(futile.logger)
require(paws)
require(datapackr)
require(scales)
require(DT)
require(config)
require(purrr)
require(praise)

source("./utils.R")

shinyServer(function(input, output, session) {
  
  ready <- reactiveValues(ok = FALSE)
  
  observeEvent(input$file1, {
    shinyjs::show("validate")
    shinyjs::enable("validate")
    ready$ok <- FALSE
  }) 
  
  observeEvent(input$validate, {
    shinyjs::disable("file1")
    shinyjs::disable("validate")
    ready$ok <- TRUE
  })  
  
  observeEvent(input$reset_input, {
    shinyjs::reset("side-panel")
    shinyjs::enable("file1")
    shinyjs::disable("validate")
    shinyjs::hide("downloadFlatPack")
    shinyjs::hide("download_messages")
    shinyjs::hide("send_paw")
    ready$ok<-FALSE
  })
  
  observeEvent(input$send_paw, {
    d <- validation_results()
    r<-saveTimeStampLogToS3(d)
    timestampUploadUI(r)
    r<-sendMERDataToPAW(d,config)
    archiveDataPackErrorUI(r)
    r<-sendValidationSummary(d,config)
    validationSummaryUI(r)
    r<-saveDATIMExportToS3(d)
    datimExportUI(r)
  })
  
  observeEvent(input$login_button, {
    is_logged_in<-FALSE
    user_input$authenticated <-DHISLogin(input$server,input$user_name,input$password)
    flog.info(paste0("User ",input$user_name, " logged in."), name="datapack")
  })  
  
  output$ui <- renderUI({
    
    if (user_input$authenticated == FALSE) {
      ##### UI code for login page
      fluidPage(
        fluidRow(
          column(width = 2, offset = 5,
                 br(), br(), br(), br(),
                 uiOutput("uiLogin"),
                 uiOutput("pass")
          )
        )
      )
    } else {
      fluidPage(
        tags$head(tags$style(".shiny-notification {
                             position: fixed;
                             top: 10%;
                             left: 33%;
                             right: 33%;}")),
        sidebarLayout(
          sidebarPanel(
            shinyjs::useShinyjs(),
            id = "side-panel",
            fileInput(
              "file1",
              "Choose DataPack (Must be XLSX!):",
              accept = c(
                "application/xlsx",
                ".xlsx"
              )
            ),
            tags$hr(),
            actionButton("validate","Validate"),
            actionButton("reset_input", "Reset inputs"),
            tags$hr(),
            downloadButton("downloadFlatPack", "Download FlatPacked DataPack"),
            downloadButton("download_messages","Download validation messages"),
            tags$hr(),
            actionButton("send_paw", "Send to PAW")
            #downloadButton("downloadDataPack","Regenerate PSNUxIM")
          ),
          mainPanel(tabsetPanel(
            id = "main-panel",
            type = "tabs",
            tabPanel("Messages", tags$ul(uiOutput('messages'))),
            tabPanel("Indicator summary", dataTableOutput("indicator_summary")),
            tabPanel("Validation rules", dataTableOutput("vr_rules")),
            tabPanel("HTS Summary Chart", plotOutput("modality_summary")),
            tabPanel("HTS Summary Table",dataTableOutput("modality_table"))
            
          ))
        ))
    }
  })
  
  user_input <- reactiveValues(authenticated = FALSE, status = "")
  
  # password entry UI componenets:
  #   username and password text fields, login button
  output$uiLogin <- renderUI({
    
    wellPanel(fluidRow(
      img(src='pepfar.png', align = "center"),
      h4("Welcome to the  COP20 DataPack Validation App. Please login with your DATIM credentials:")
    ),
    fluidRow(
      textInput("user_name", "Username: ",width = "600px"),
      passwordInput("password", "Password:",width = "600px"),
      actionButton("login_button", "Log in!")
    ))
  })
  
  validate<-function() {
    
    shinyjs::hide("downloadFlatPack")
    shinyjs::hide("downloadDataPack")
    shinyjs::hide("download_messages")
    shinyjs::hide("send_paw")
    shinyjs::hide("vr_rules")
    shinyjs::hide("modality_summary")
    shinyjs::hide("modality_table")
    
    
    if (!ready$ok) {
      shinyjs::disable("validate")
      return(NULL)
    }
    
    inFile <- input$file1
    messages<-""
    
    if ( is.null( inFile ) ) return( NULL )
    
    messages<-list()
    
    withProgress(message = 'Validating file', value = 0,{
      
      shinyjs::disable("file1")
      shinyjs::disable("validate")
      incProgress(0.1, detail = ("Validating your DataPack"))
      
      d<-tryCatch({
        datapackr::unPackTool(inFile$datapath)},
        error = function(e){
          return(e)
        })
      
      if (!inherits(d,"error") & !is.null(d)) {
        
        #We do not have a great way of dealing with datapacks with multiple country ids...
        d$info$country_uids<-substr(paste0(d$info$country_uids,sep="",collapse="_"),0,25)
        d$info$sane_name<-paste0(stringr::str_extract_all(d$info$datapack_name,"[A-Za-z0-9_]",
                                                          simplify = TRUE),sep="",collapse="")
        #Keep this until we can change the schema
        
        flog.info(paste0("Initiating validation of ",d$info$datapack_name, " DataPack."), name="datapack")
        
        if ( d$info$has_psnuxim ) {
          flog.info("Datapack with PSNUxIM tab found.")
          
          incProgress(0.1, detail = ("Checking validation rules"))
          Sys.sleep(0.5)
          d <- validatePSNUData(d)
          incProgress(0.1,detail="Validating mechanisms")
          Sys.sleep(0.5)
          d <- validateMechanisms(d)
          incProgress(0.1, detail = ("Saving a copy of your submission to the archives"))
          Sys.sleep(0.5)
          r<-archiveDataPacktoS3(d,inFile$datapath,config)
          archiveDataPackErrorUI(r)
          incProgress(0.1, detail = (praise()))
          Sys.sleep(1)
          d<-prepareFlatMERExport(d)
          
          shinyjs::show("downloadFlatPack")
          shinyjs::show("download_messages")
          shinyjs::show("vr_rules")
          shinyjs::show("modality_summary")
          shinyjs::show("modality_table")
          shinyjs::show("send_paw")
          shinyjs::enable("send_paw")
          #shinyjs::show("downloadDataPack")
          #shinyjs::enable("downloadDataPack")
        }
      }
    })
    
    return(d)
    
  }
  
  validation_results <- reactive({ validate() })
  
  output$modality_summary <- renderPlot({ 
    
    vr<-validation_results()
    
    if (!inherits(vr,"error") & !is.null(vr)){
      vr  %>% 
        purrr::pluck(.,"data") %>%
        purrr::pluck(.,"analytics") %>%
        modalitySummaryChart()
      
    } else {
      NULL
    }
    
  },height = 400,width = 600)
  
  output$modality_table<-DT::renderDataTable({
    
    vr<-validation_results()
    
    if (!inherits(vr,"error") & !is.null(vr)){
      
      table_formatted<-modalitySummaryTable(vr$data$analytics) %>%
        dplyr::mutate(
          Positive = format( Positive ,big.mark=',', scientific=FALSE),
          Total = format( Total ,big.mark=',', scientific=FALSE),
          yield = format(round(yield, 2), nsmall = 2),
          modality_share = format(round(modality_share, 2), nsmall = 2)) %>%
        dplyr::select(Modality = hts_modality,
                      Positive,
                      Total,
                      "Yield (%)"= yield,
                      "Percent of HTS_POS" = modality_share)
      
      DT::datatable(table_formatted,
                    options = list(pageLength = 25,columnDefs = list(list(
                      className = 'dt-right', targets = 2),
                      list(
                        className = 'dt-right', targets = 3),
                      list(
                        className = 'dt-right', targets = 4),
                      list(
                        className = 'dt-right', targets = 5)
                    )))
    } else {
      NULL
    }
  })
  
  output$indicator_summary<-DT::renderDataTable({
    
    vr<-validation_results()
    
    if (!inherits(vr,"error") & !is.null(vr)){
      
      vr  %>%
        purrr::pluck(.,"data") %>%
        purrr::pluck(.,"MER") %>%
        dplyr::group_by(indicator_code) %>%
        dplyr::summarise(value = format( round(sum(value)) ,big.mark=',', scientific=FALSE)) %>%
        dplyr::arrange(indicator_code)
      
      
    } else {
      NULL
    }
  })
  
  output$vr_rules<-DT::renderDataTable({
    
    vr<-validation_results()
    
    if ( is.null(vr)) {
      return(NULL)
    }
    
    if (!inherits(vr,"error")  & !is.null(vr)){
      
      vr_results <- vr %>%
        purrr::pluck(.,"datim") %>%
        purrr::pluck(.,"vr_rules_check")
      
    }  else {
      return(NULL)
    }
    
    if (NROW(vr_results) == 0 ) {
      return(data.frame(message="Congratulations! No validation rule issues found!"))
    }
    
    vr_results
    
  })
  
  output$downloadDataPack <- downloadHandler(
    filename = function() {
      
      d<-validation_results()
      prefix <-d$info$sane_name
      date<-format(Sys.time(),"%Y%m%d_%H%M%S")
      paste0(paste(prefix,date,sep="_"),".xlsx")
      
    },
    content = function(file) {
      
      d <- validation_results()
      
      
      flog.info(
        paste0("Regeneration of Datapack requested for ", d$info$datapack_name)
        ,
        name = "datapack")
      d <- writePSNUxIM(d,snuxim_model_data_path = config$snuxim_model )
      flog.info(
        paste0("Datapack reloaded for for ", d$info$datapack_name) ,
        name = "datapack")
      openxlsx::saveWorkbook(wb = d$tool$wb, file = file, overwrite = TRUE)
    }
  )
  
  
  output$downloadFlatPack <- downloadHandler(
    filename = function() {
      
      prefix <- "flatpack"
      
      date<-format(Sys.time(),"%Y%m%d_%H%M%S")
      
      paste0(paste(prefix,date,sep="_"),".xlsx")
    },
    content = function(file) {
      
      #Create a new workbook
      wb <- openxlsx::createWorkbook()
      
      d<-validation_results()
      mer_data <- d %>% 
        purrr::pluck(.,"data") %>% 
        purrr::pluck(.,"MER")
      
      subnat_impatt <- d %>% 
        purrr::pluck(.,"data") %>% 
        purrr::pluck(.,"SUBNAT_IMPATT")
      
      mer_data<-dplyr::bind_rows(mer_data,subnat_impatt)
      openxlsx::addWorksheet(wb,"MER Data")
      openxlsx::writeDataTable(wb = wb,
                               sheet = "MER Data",x = mer_data)
      
      has_psnu<-d %>% 
        purrr::pluck(.,"info") %>% 
        purrr::pluck(.,"has_psnuxim")
      
      if (has_psnu) {
        
        
        openxlsx::addWorksheet(wb,"Distributed MER Data")
        openxlsx::writeDataTable(wb = wb,
                                 sheet = "Distributed MER Data",x = d$data$analytics)
        
        validation_rules<- d %>% 
          purrr::pluck(.,"datim") %>% 
          purrr::pluck(.,"vr_rules_check") 
        
        openxlsx::addWorksheet(wb,"Validation rules")
        openxlsx::writeData(wb = wb,
                            sheet = "Validation rules",x = validation_rules)
        
        d$datim$MER$value<-as.character(d$datim$MER$value)
        d$datim$subnat_impatt$value<-as.character(d$datim$subnat_impatt$value)
        datim_export<-dplyr::bind_rows(d$datim$MER,d$datim$subnat_impatt)
        
        openxlsx::addWorksheet(wb,"DATIM export")
        openxlsx::writeData(wb = wb,
                            sheet = "DATIM export",x = datim_export)
        
        openxlsx::addWorksheet(wb,"Rounding diffs")
        openxlsx::writeData(wb = wb,
                            sheet = "Rounding diffs",x = d$tests$PSNUxIM_rounding_diffs)
        
        openxlsx::addWorksheet(wb,"HTS Summary")
        openxlsx::writeData(wb = wb,
                            sheet = "HTS Summary", x = modalitySummaryTable(d$data$analytics))
        
      }
      
      datapack_name <-d$info$datapack_name
      
      flog.info(
        paste0("Flatpack requested for ", datapack_name) 
        ,
        name = "datapack"
      )
      
      openxlsx::saveWorkbook(wb,file=file,overwrite = TRUE)
      
    }
  )
  
  output$messages <- renderUI({
    
    vr<-validation_results()
    messages<-NULL
    
    if ( is.null(vr)) {
      return(NULL)
    }
    
    if ( inherits(vr,"error") ) {
      return( paste0("ERROR! ",vr$message) )
      
    } else {
      
      messages <- validation_results() %>%
        purrr::pluck(., "info") %>%
        purrr::pluck(., "warning_msg")
      
      if (!is.null(messages))  {
        lapply(messages, function(x)
          tags$li(x))
      } else
      {
        tags$li("No Issues with Integrity Checks: Congratulations!")
      }
    }
  })
  
  
  output$download_messages <- downloadHandler(
    filename = function(){
      paste("DataPack_Validation_Messages_", Sys.Date(), ".txt", sep = "")
    },
    content = function(file) {
      vr<-validation_results()
      
      writeLines(vr$info$warning_msg, file)
    }
  )
})
