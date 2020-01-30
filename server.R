library(shiny)
library(shinyjs)
require(magrittr)
require(purrr)
require(dplyr)
require(datimvalidation)
require(ggplot2)
require(futile.logger)

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
    shinyjs::hide("validate")
    shinyjs::hide("downloadFlatPack")
    ready$ok<-FALSE
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
            downloadButton("downloadFlatPack", "Download FlatPacked DataPack")
          ),
          mainPanel(tabsetPanel(
            id = "main-panel",
            type = "tabs",
            tabPanel("Messages", tags$ul(uiOutput('messages'))),
            tabPanel("Indicator summary", dataTableOutput("indicator_summary"))

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
    
    if (!ready$ok) {return(NULL)}
  
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
        flog.info(paste0("Initiating validation of ",d$info$country_uids, " DataPack."), name="datapack")
       d <- filterZeros(d)
       # incProgress(0.1, detail = ("Checking validation rules"))
       # d <- validatePSNUData(d)
      #  incProgress(0.1,detail="Validating mechanisms")
      #  d <- validateMechanisms(d)
      #  incProgress(0.1, detail = ("Making mechanisms prettier"))
      #  d$data$distributedMER %<>% adornMechanisms()
      #  d$data$SNUxIM %<>% adornMechanisms()
      #  Sys.sleep(0.5)
      #  incProgress(0.1, detail = ("Running dimensional transformation"))
      # d$data$MER %<>% adornMERData()
      #  d$data$distributedMER  %<>%  adornMERData()
      #  Sys.sleep(0.5)
        
        shinyjs::show("downloadFlatPack")
      }
    })

    return(d)
    
  }
  
  validation_results <- reactive({ validate() })
  
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
   

  output$downloadFlatPack <- downloadHandler(
    filename = function() {
      
      prefix <- "flatpack"
    
      date<-format(Sys.time(),"%Y%m%d_%H%M%S")
      
      paste0(paste(prefix,date,sep="_"),".xlsx")
    },
    content = function(file) {
      
      mer_data <- validation_results() %>% 
        purrr::pluck(.,"data") %>% 
        purrr::pluck(.,"MER")

      subnat_impatt <- validation_results() %>% 
                purrr::pluck(.,"data") %>% 
                purrr::pluck(.,"SUBNAT_IMPATT")
                                            
      download_data<-dplyr::bind_rows(mer_data,subnat_impatt)
    
      datapack_name <-
        validation_results() %>% 
        purrr::pluck(.,"info") %>%
        purrr::pluck(.,"country_uids") %>% 
        getCountryNameFromUID()
      
      flog.info(
        paste0("Flatpack requested for ", datapack_name) 
        ,
        name = "datapack"
      )
      
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
})
