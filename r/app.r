library(leaflet)
library(tidyverse)
library(googleVis)

rawparking <- read.csv('anonfile.csv')
parkingdata <- rawparking %>% select(c("Infraction.Datetime", "Infraction.Text", "Street.Address", "Latitude", "Longitude", "violation.Code"))
parkingdata$Infraction.Datetime <- lubridate::mdy_hm(parkingdata$Infraction.Datetime)
datemax <- as.Date(parkingdata[which.max(as.Date(parkingdata$Infraction.Datetime)), ]$Infraction.Datetime)
datemin <- as.Date(parkingdata[which.min(as.Date(parkingdata$Infraction.Datetime)), ]$Infraction.Datetime)

towPrettyNames <- list(
  "48 Hour Violation",
  "Abandoned",
  "Blocking Garage/Driveway",
  "Commercial Vehicle (Obstructing)",
  "Commercial Vehicle (Over 20k lbs)",
  "Commercial Vehicle (Under 20k lbs)",
  "Expired Tags",
  "In  Taxicab Stand",
  "In  Transit Zone/Stop",
  "In Bike Lane",
  "In Bus Stop/Bus Lane",
  "In Fire Lane",
  "In Hanicapped ",
  "In no stop/stand/park (event)",
  "In no stop/stand/park (no towaway)",
  "In no stop/stand/park (towaway)",
  "In Passenger/Truck Loading Zone",
  "In Streetcleaning Zone",
  "Less than 15 ft from Fire Hydrant",
  "Obstrcting Pedestrian Traffic",
  "Obstructing Intersection/X-Walk",
  "Obstructing Intersection/X-Walk (School)",
  "Obstructing Traffic",
  "Parked in RPP Zone",
  "Parking Meter",
  "Other"
)

towOfficalNames <- list(
  "EXCEEDING 48 HOURS",
  "ABANDONED VEHICLE",
  "BLOCKING GARAGE OR DRIVEWAY",
  "COMM VEH OBSTRUCT/IMPEDE FREE FLOW",
  "COMMERICAL VEHICLE OVER 20,000 LB",
  "COMMERICAL VEHICLE UNDER 20,000 LB",
  "EXPIRED TAGS",
  "IN TAXICAB STAND",
  "IN TRANSIT ZONE/STOP",
  "NO PARK/STAND IN BIKE LANES",
  "NO PARK/STAND IN BUS STOP/BUS LANE",
  "FIRE LANE",
  "NO STOP/PARK HANDICAP",
  "NO STOPPING/PARKING STADIUM EVENT CAMDEN",
  "NO STOP/STAND/PARK NO TOW-AWAY ZONE",
  "NO STOP/STAND/PARK TOWAWAY ZONE",
  "PASSENGER/ TRUCK LOADING ZONE",
  "NO STOP/PARK STREETCLEANING TOWAWAY ZONE",
  "LESS THAN 15 FEET FROM FIRE HYDRANT",
  "OBSTRUCT/IMPEDING MOVEMENT OF PEDESTRIAN",
  "OBST/IMPEDE TRAFFIC INTERSECT / X-WALK",
  "OBSTRUCT/IMPEDE TRAFFIC /XWALK/INTER/SCHOOL",
  "OBSTRUCT/IMPEDING FREE FLOW OF TRAFFIC",
  "RESIDENTIAL PARKING PERMIT ONLY",
  "ALL PARKING METER VIOLATIONS",
  "ALL OTHER PARKING VIOLATIONS"
)

ui <- fluidPage(
  title = "ParkingStat",
  width="100%",
  tabsetPanel(
    tabPanel(
      "Parking Citation Heatmap", 
      div(class="outer",
          
          tags$head(
            # Include our custom CSS
            includeCSS("styles.css"),
            includeScript("gomap.js")
          ),
          
          leafletOutput("map", width="100%", height="100%"),
          
          absolutePanel(
            id = "controls", 
            class = "panel panel-default", 
            fixed = TRUE,
            draggable = TRUE, 
            top = 60, 
            left = "auto", 
            right = 20, 
            bottom = "auto",
            width = 330, 
            height = "auto",
            
            h2("ParkingStat"),
            actionLink("selectallmap","Select All/None"),
            checkboxGroupInput(
              "violationtype",
              "Violation Type",
              width="95%",
              choiceNames = towPrettyNames,
              choiceValues = towOfficalNames,
              selected = towOfficalNames
            ),
            sliderInput(
              "mapslider", "Date",
              min = datemin, 
              max = datemax,
              value = c(datemax - 60,
                        datemax), 
              animate = TRUE,
              width = '90%'
            ),
          )
      )
    ), 
    tabPanel(
      "Parking Citations Over Time",
      sidebarPanel(
        actionLink("selectallgraph","Select All/None"),
        checkboxGroupInput(
          "parkingcitationscb",
          h3("Parking Citation Types"), 
          choiceNames = towPrettyNames,
          choiceValues = towOfficalNames,
          selected = towOfficalNames
        ),
        width='2'
      ),
      mainPanel(
        h4("ParkingStat"),
        fluidRow(
          sliderInput(
            "graphslider", "Date",
            min = datemin, 
            max = datemax,
            value = c(datemax - 180,
                      datemax), 
            animate = TRUE,
            width = '100%'
          )
        ),
        htmlOutput(
          "quantityview",
          width="100%"
        )
      ), 
      position="left",
      fluid=TRUE,
      width="100%"
    )
  )
)

server <- function(input, output, session) {
  
  ##################
  # Create the map #
  ##################
  # This creates the 'select all/select none' link which checks all of the checkboxes
  observe({
    if(input$selectallmap == 0) return(NULL) 
    else if (input$selectallmap%%2 == 0)
    {
      updateCheckboxGroupInput(
        session,
        "violationtype",
        choiceNames = towPrettyNames,
        choiceValues = towOfficalNames,
      )
    }
    else
    {
      updateCheckboxGroupInput(
        session,
        "violationtype",
        choiceNames = towPrettyNames,
        choiceValues = towOfficalNames,
        selected = towOfficalNames
      )
    }
  })
  
  output$map <- renderLeaflet({
    leaflet() %>%
      addTiles(
        urlTemplate = "//{s}.tiles.mapbox.com/v3/jcheng.map-5ebohr46/{z}/{x}/{y}.png",
        attribution = 'Maps by <a href="http://www.mapbox.com/">Mapbox</a>'
      ) %>% 
      setView(lng = -76.605141, lat = 39.304533, zoom = 14)
  })
  
  filtereddata <- reactive({
    x <- parkingdata %>% 
      filter((as.Date(Infraction.Datetime) >= as.Date(input$mapslider[1])) & 
               (as.Date(input$mapslider[2]) >= as.Date(Infraction.Datetime)) &
               (Infraction.Text %in% input$violationtype)) %>%
      select('Latitude', 'Longitude', 'Street.Address') %>%
      group_by(Latitude, Longitude, Street.Address) %>%
      summarize(Freq=n())
  })
  
  colorpal <- reactive({
    colorNumeric("RdYlBu", domain=NULL)
  })
  
  observe({
    pal <- colorNumeric("RdYlBu", domain = NULL)
    
    leafletProxy("map", data = filtereddata()) %>%
      clearShapes() %>%
      addCircles(
        lat = ~Latitude,
        lng = ~Longitude,
        radius=~Freq*40, 
        weight = 1, 
        color = "#777777",
        fillColor = ~pal(Freq), 
        fillOpacity = 0.7, 
        popup = ~paste("Block:", Street.Address, "<br>Tickets:", Freq))
  })
  
  observe({
    proxy <- leafletProxy("map", data=filtereddata())
    
    proxy %>% clearControls()
    pal <- colorpal()
    proxy %>% addLegend(position = "bottomright", pal = pal, values=~Freq)
  })
  
  #####################
  # Create line graph #
  #####################
  observe({
    if(input$selectallgraph == 0) return(NULL) 
    else if (input$selectallgraph%%2 == 0)
    {
      updateCheckboxGroupInput(
        session,
        "parkingcitationscb",
        choiceNames = towPrettyNames,
        choiceValues = towOfficalNames
      )
    }
    else
    {
      updateCheckboxGroupInput(
        session,
        "parkingcitationscb",
        choiceNames = towPrettyNames,
        choiceValues = towOfficalNames,
        selected = towOfficalNames)
    }
  })
  
  output$quantityview <- renderGvis({
    data <- reactive({
      talliedparkingdata <- parkingdata %>% 
        mutate(Infraction.Datetime=as.Date(Infraction.Datetime)) %>% 
        filter(as.Date(Infraction.Datetime) >= as.Date(input$graphslider[1]) & as.Date(input$graphslider[2]) >= as.Date(Infraction.Datetime)) %>%
        group_by(Infraction.Datetime, Infraction.Text) %>% 
        arrange(Infraction.Datetime) %>%
        tally() %>%
        ungroup()
      
      df = data.frame(datetime=seq(from=as.Date(input$graphslider[1]), to=as.Date(input$graphslider[2]), by='day'))

      for (type in input$parkingcitationscb){
        x <- talliedparkingdata %>% 
          filter(Infraction.Text==type) %>%
          complete(Infraction.Datetime=seq.Date(as.Date(input$graphslider[1]), as.Date(as.Date(input$graphslider[2])), by="day"), fill=list(Infraction.Text=type, n=0))
        
        df[type] = x$n
      }
      df
    })
    
    gvisLineChart(
      data(), 
      xvar='datetime', 
      options=list(
        legend="{  maxLines: 3, }",
        theme="maximized",
        vAxes="[{title:'# of citations'}]", 
        width="100%",
        height=450
      )
    )
  })
  
}

shinyApp(ui, server)