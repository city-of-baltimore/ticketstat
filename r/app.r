library(leaflet)
library(tidyverse)

rawparking <- read.csv('outfile2.csv')
parkingdata <- rawparking %>% select(c("Infraction.Datetime", "Infraction.Text", "Street.Address", "Latitude", "Longitude"))
datemax <- as.Date(parkingdata[which.max(as.Date(parkingdata$Infraction.Datetime, '%m/%d/%Y')), ]$Infraction.Datetime, '%m/%d/%Y')
datemin <- as.Date(parkingdata[which.min(as.Date(parkingdata$Infraction.Datetime, '%m/%d/%Y')), ]$Infraction.Datetime, '%m/%d/%Y')

ui <- fluidPage(
  title = "ParkingStat",

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
                  
      checkboxGroupInput(
        "violationtype",
        "Violation Type",
        width="95%",
        choiceNames = list(
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
        ),
        choiceValues = list(
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
        ),
        selected = "ALL PARKING METER VIOLATIONS"
      ),
      sliderInput(
        "date", "Date",
        min = datemin, 
        max = datemax,
        value = c(datemax - 180,
                  datemax), 
        animate = TRUE,
        width = '90%'
      ),
    )
  )
)

server <- function(input, output, session) {

  # Create the map
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
      filter((as.Date(Infraction.Datetime, '%m/%d/%Y') >= as.Date(input$date[1])) & 
             (as.Date(input$date[2]) >= as.Date(Infraction.Datetime, '%m/%d/%Y')) &
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
        radius=~Freq*2, 
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
}

shinyApp(ui, server)