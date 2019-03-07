library(shiny)
library(shinyjs)
require(magrittr)
require(purrr)
require(dplyr)
require(datimvalidation)


source("./utils.R")

shinyServer(function(input, output, session) {
  
  ready <- reactiveValues(ok = FALSE)
  
  observeEvent(input$file1, {
    shinyjs::enable("validate")
    ready$ok <- FALSE
  }) 
  
  observeEvent(input$validate, {
    shinyjs::disable("validate")
    ready$ok <- TRUE
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
            fileInput(
              "file1",
              "Choose data file:",
              accept = c(
                "application/xlsx",
                ".xlsx"
              )
            ),
            tags$hr(),
            actionButton("validate","Validate"),
            tags$hr(),
            downloadButton("downloadData", "Download SUBNATT results"),
            downloadButton("downloadFlatPack", "Download FlatPacked DataPack")
          ),
          mainPanel(tabsetPanel(
            type = "tabs",
            tabPanel("Messages",   tags$ul(uiOutput('messages'))),
            tabPanel("MER Summary", dataTableOutput("contents")),
            tabPanel("Validation rules", dataTableOutput("vr_rules"))
          ))
        ))
  }
})
  
  
  user_input <- reactiveValues(authenticated = FALSE, status = "")
  
  observeEvent(input$login_button, {
    is_logged_in<-FALSE
    user_input$authenticated <-DHISLogin(input$server,input$user_name,input$password)
  })   
  
  # password entry UI componenets:
  #   username and password text fields, login button
  output$uiLogin <- renderUI({
    
    wellPanel(fluidRow(
      img(src='pepfar.png', align = "center"),
      h4("Welcome to the DataPack Validation tool. Please login with your DATIM credentials:")
    ),
    fluidRow(
      textInput("user_name", "Username: ",width = "600px"),
      passwordInput("password", "Password:",width = "600px"),
      actionButton("login_button", "Log in!")
    ))
  })
  
  
  validate<-function() {
    
    shinyjs::hide("downloadData")
    shinyjs::hide("downloadFlatPack")
    
    if (!ready$ok) {return(NULL)}
  
    inFile <- input$file1
    messages<-""
    
    if (is.null(inFile)) return(NULL)
    
    messages<-list()
    
    withProgress(message = 'Validating file', value = 0,{
      
      incProgress(0.1, detail = ("Validating your DataPack"))
      d<-tryCatch({
        datapackr::unPackData(inFile$datapath)},
        error = function(e){
          return(e)
        })
      
    })
    if (!inherits(d,"error") ) {
      
      #Filter any zeros
      d$data$MER %<>% filter(.,value != 0)
      d$data$SUBNAT_IMPATT %<>% filter(.,value != 0)
      d$data$SNUxIM %<>% filter(., distribution != 0)
      d$data$distributedMER %<>% filter(.,value != 0)
      
      incProgress(0.1, detail = ("Checking validation rules"))
      
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
    
    shinyjs::show("downloadData")
    shinyjs::show("downloadFlatPack")
    
    }

    return(d)
    
  }
  
  
  validation_results <- reactive({ validate() })
  
  output$contents <- renderDataTable({ 
    
    vr<-validation_results()
     if (!inherits(vr,"error")){
    vr %>%
      purrr::pluck(.,"data") %>%
      purrr::pluck(.,"MER") %>%
      group_by(indicatorCode) %>% 
      summarize(value = sum(value)) %>% 
      arrange(indicatorCode) } else {
        NULL
      }
  
    })
  
  
  output$vr_rules <- renderDataTable({ 
    
    vr<-validation_results()
    
    if (!inherits(vr,"error")){
      
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
      
      ou < -validation_results() %>% 
        purrr::pluck(.,"info") %>%
        purrr::pluck(.,"datapack_name")
      
      date<-format(Sys.time(),"%Y%m%d_%H%M%S")
      
      paste0(paste(prefix,ou,date,sep="_"),".xlsx")
    },
    content = function(file) {
      
      download_data <- validation_results() %>% 
        purrr::pluck(.,"data") 
      
      openxlsx::write.xlsx(download_data, file = file)
      
    })
    
    output$downloadData <- downloadHandler(
      filename = "SUBNAT_IMPATT.csv",
      content = function(file) {
        
        download_data <- validation_results() %>% 
          purrr::pluck(.,"datim") %>%
          purrr::pluck(.,"SUBNAT_IMPATT")
     
         write.table(download_data, file = file, sep=",",row.names = FALSE,col.names = TRUE,quote=TRUE)
      }

  )
  
  output$messages <- renderUI({
    
    vr<-validation_results()
    messages<-NULL
    
    if (inherits(vr,"error")) {
      messages <- paste0("ERROR! ",vr$message)
    } else {
      
      messages <- validation_results() %>%
        purrr::pluck(., "info") %>%
        purrr::pluck(., "warningMsg")      
    }
    
    if (!is.null(messages))  {
      lapply(messages, function(x)
        tags$li(x))
    } else
    {
      NULL
    }
    
  })
  
})
