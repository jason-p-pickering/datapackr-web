library(shiny)
library(shinyjs)
require(magrittr)
require(purrr)
require(dplyr)
require(datimvalidation)
require(ggplot2)


source("./utils.R")

shinyServer(function(input, output, session) {
  
  ready <- reactiveValues(ok = FALSE)
  
  observeEvent(input$file1, {
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
    shinyjs::reset("message-tab")
    shinyjs::enable("file1")
    shinyjs::disable("validate")
  })
  
  observeEvent(input$login_button, {
    is_logged_in<-FALSE
    user_input$authenticated <-DHISLogin(input$server,input$user_name,input$password)
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
            downloadButton("downloadFlatPack", "Download FlatPacked DataPack")
          ),
          mainPanel(tabsetPanel(
            id = "main-panel",
            type = "tabs",
            tabPanel("Messages", id="message-tab",   tags$ul(uiOutput('messages'))),
            tabPanel("Indicator summary", id="indicator-summary-tab", dataTableOutput("indicator_summary")),
            tabPanel("HTS Modality Summary", plotOutput("modality_summary")),
            tabPanel("Validation rules", dataTableOutput("vr_rules"))
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
      h4("Welcome to the DataPack Validation App. Please login with your DATIM credentials:")
    ),
    fluidRow(
      textInput("user_name", "Username: ",width = "600px"),
      passwordInput("password", "Password:",width = "600px"),
      actionButton("login_button", "Log in!")
    ))
  })
  
  validate<-function() {
    
    shinyjs::hide("downloadFlatPack")
    
    if (!ready$ok) {return(NULL)}
  
    inFile <- input$file1
    messages<-""
    
    if ( is.null( inFile ) ) return( NULL )
    
    messages<-list()
    
    withProgress(message = 'Validating file', value = 0,{
      
      shinyjs::disable("file1")
      incProgress(0.1, detail = ("Validating your DataPack"))
      d<-tryCatch({
        datapackr::unPackData(inFile$datapath)},
        error = function(e){
          return(e)
        })
      
      if (!inherits(d,"error") & !is.null(d)) {
        
        d <- filterZeros(d)
        incProgress(0.1, detail = ("Checking validation rules"))
        d <- validatePSNUData(d)
        incProgress(0.1,detail="Validating mechanisms")
        d <- validateMechanisms(d)
        incProgress(0.1, detail = ("Making mechanisms prettier"))
        d$data$distributedMER %<>% adornMechanisms()
        d$data$SNUxIM %<>% adornMechanisms()
        Sys.sleep(0.5)
        incProgress(0.1, detail = ("Running dimensional transformation"))
        d$data$MER %<>% adornMERData()
        d$data$distributedMER  %<>%  adornMERData()
        Sys.sleep(0.5)
        
        shinyjs::show("downloadFlatPack")
      }
    })
    return(d)
    
  }
  
  validation_results <- reactive({ validate() })
  
  output$indicator_summary<-renderDataTable({
    
    vr<-validation_results()
    
    if (!inherits(vr,"error") & !is.null(vr)){
      vr  %>% 
        purrr::pluck(.,"data") %>%
        purrr::pluck(.,"distributedMER") %>%
        dplyr::group_by(technical_area,disagg_type,agency, numerator_denominator) %>% 
        dplyr::summarise(value = format( round(sum(value)) ,big.mark=',', scientific=FALSE)) %>%
        dplyr::arrange(technical_area,disagg_type,agency, numerator_denominator) 
      
    } else {
      NULL
    }
  })
  
  output$modality_summary <- renderPlot({ 
    
    vr<-validation_results()
    
    if (!inherits(vr,"error") & !is.null(vr)){
      vr  %>% 
        purrr::pluck(.,"data") %>%
        purrr::pluck(.,"MER") %>%
        modalitySummaryChart()
      
    } else {
      NULL
    }
    
  },height = 400,width = 600)
  
  output$vr_rules <- renderDataTable({ 
    
    vr<-validation_results()
    
    if (!inherits(vr,"error")  & !is.null(vr)){
      
      vr %>%
        purrr::pluck(.,"datim") %>%
        purrr::pluck(.,"vr_rules_check")  
      
      } else {
          NULL
        }
    
  })
  
  output$downloadFlatPack <- downloadHandler(
    filename = function() {
      
      prefix <- "flatpack"
    
      date<-format(Sys.time(),"%Y%m%d_%H%M%S")
      
      paste0(paste(prefix,date,sep="_"),".xlsx")
    },
    content = function(file) {
      
      download_data <- validation_results() %>% 
        purrr::pluck(.,"data")
      
      merColumnsToKeep<-function(x) {
  
        columns_to_keep<-c(
          "psnuid",
          "PSNU",
          "technical_area",
          "indicatorCode", 
          "disagg_type",
          "Age",
          "Sex",
          "KeyPop",
          "resultstatus",
          "resultstatus_inclusive",
          "numerator_denominator",
          "hts_modality",
          "mechanismCode",
          "partner",
          "agency",
          "value"
        )
        
        columns_existing <-which(columns_to_keep %in% names(x))
        columns_to_keep[columns_existing]
      }
      
      download_data$MER %<>% dplyr::select(.,merColumnsToKeep(.))
      download_data$distributedMER %<>%  dplyr::select(.,merColumnsToKeep(.))
      
      vr_rules<-validation_results() %>% 
        purrr::pluck(.,"datim") %>%
        purrr::pluck(.,"vr_rules_check")
      
      download_data$validation_rules <- vr_rules
      openxlsx::write.xlsx(download_data, file = file)
      
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
        purrr::pluck(., "warningMsg")
      
      if (!is.null(messages))  {
        lapply(messages, function(x)
          tags$li(x))
      } else
      {
        tags$li("No Issues with Integrity Checks: Congratulations!")
      }
    }
  })
})
