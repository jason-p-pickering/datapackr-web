library(shiny)
library(shinyjs)
require(magrittr)
require(purrr)

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
                "application/xlsx"
              )
            ),
            tags$hr(),
            actionButton("validate","Validate"),
            downloadButton("downloadData", "Download validation results")
          ),
          mainPanel(tabsetPanel(
            type = "tabs",
            tabPanel("Output", dataTableOutput("contents")),
            tabPanel("Messages",   verbatimTextOutput('messages'))
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
    wellPanel(
      textInput("user_name", "User Name:",width = "600px"),
      passwordInput("password", "Password:",width = "600px"),
      actionButton("login_button", "Log in")
    )
  })
  
  
  validate<-function() {
    
    shinyjs::hide("downloadData")
    if (!ready$ok) {return(NULL)}
  
    inFile <- input$file1
    messages<-""
    
    if (is.null(inFile)) return(NULL)
    
    messages<-list()
    
    withProgress(message = 'Validating file', value = 0,{
      
      incProgress(0.1, detail = ("Validating your DataPack"))
      d<-datapackr::unPackData(inFile$datapath)
      
    })
    shinyjs::show("downloadData")
    return(d)
    
  }
  
  
  validation_results <- reactive({ validate() })
  
  output$contents <- renderDataTable({ 
    validation_results() %>% 
      purrr::pluck(.,"datim") %>%
      purrr::pluck(.,"SUBNAT_IMPATT")
  })
  
  output$downloadData <- downloadHandler(
    filename = "SUBNAT_IMPATT.csv",
    content = function(file) {
      
      download_data <- validation_results() %>% 
        purrr::pluck(.,"datim") %>%
        purrr::pluck(.,"SUBNAT_IMPATT")
      
      write.table(download_data, file = file, sep=",",row.names = FALSE,col.names = TRUE,quote=TRUE)
    }
    
    output$messages<- validation_results() %>% 
      purrr::pluck(.,"info") %>%
      purrr::pluck(.,"warningMsg")
  )
  })
