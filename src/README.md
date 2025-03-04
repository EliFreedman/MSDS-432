# Requirements:

## Requirement 1:
The business intelligence reports are geared toward tracking and forecasting events that have direct or indirect negative or positive impacts on businesses and neighborhoods in different zip codes within the city of Chicago. The business intelligence reports will be used to send alerts to taxi drivers about the state of COVID-19 in the different zip codes in order to avoid taxi drivers to be the super spreaders in the different zip codes and neighborhoods. For this report, the taxi trips and daily COVID19 datasets for the city of Chicago will be used.

The City of Chicago is also interested to forecast COVID-19 alerts (Low, Medium, High) on daily/weekly basis to the residents of the different neighborhoods considering the counts of the taxi trips and COVID-19 positive test cases.

    Tables: 
        Covid Cases: Group by date and zip code and determine severity
        Taxi Trips: Join the severity to this table
            - Need to transform latitude and longitude into zip code
        Transportation Trips: Union with Taxi Trips
            - Need to transform latitude and longitude into zip code

## Requirement 2:
There are two major airports within the city of Chicago: O’Hare and Midway. And the City of Chicago is interested to track trips from these airports to the different zip codes and the reported COVID-19 positive test cases. The city of Chicago is interested to monitor the traffic of the taxi trips from these airports to the different neighborhoods and zip codes.

    Tables:
        Covid Cases: Group by date and zip code and determine severity
        Taxi Trips: Join the severity to this table and filter by pickups near the airport
        Transportation Trips: Union with Taxi Trips

## Requirement 3:
The city of Chicago has created the COVID-19 Community Vulnerability Index (CCVI) to identify communities that have been disproportionately affected by COVID-19 and are vulnerable to barriers to COVID-19 vaccine uptake. The city of Chicago is interested to track the number of taxi trips from/to the neighborhoods that have CCVI Category with value HIGH

    Tables:
        Covid VI: Select the CCVI category
        Taxi Trips: Join the VI to this table by either community area number or community area name
        Transportation Trips: Union with Taxi Trips

## Requirement 4:
For streetscaping investment and planning, the city of Chicago is interested to forecast daily, weekly, and monthly traffic patterns utilizing the taxi trips for the different zip codes.

    Tables:
        Taxi Trips: Aggregate by pickup and dropoff areas and get the count by day, week, and month.
        Transportation Trips: Union with Taxi Trips

## Requirement 5:
For industrial and neighborhood infrastructure investment, the city of Chicago is interested to invest in top 5 neighborhoods with highest unemployment rate and poverty rate and waive the fees for building permits in those neighborhoods in order to encourage businesses to develop and invest in those neighborhoods. Both, building permits and unemployment, datasets will be used in this report.

    Tables:
        Census Data: Order by Unemployment and Poverty Rate
        Building Permits: Join with Census Data by community area

## Requirement 6:
According to a report published by Crain’s Chicago Business, The “little guys”, small businesses, have trouble competing with the big players like Amazon and Walmart for warehouse
spaces. To help small business, assume a new imaginary program has been piloted with the name Illinois Small Business Emergency Loan Fund Delta to offer small businesses low interest loans of up to $250,000 for those applicants with PERMIT_TYPE of PERMIT - NEW CONSTRUCTION in the zip code that has the lowest number of PERMIT - NEW CONSTRUCTION applications and PER CAPITA INCOME is less than 30,000 for the planned construction site. Both, building permits and unemployment, datasets will be used in this report.

    Tables:
        Census Data: Filter where Income is less than $30,000
        Building Permits: Filter by Permit Type, Aggregate by Zip Code ascending, Take the first row, and Filter Total Fee

# Tables and Required Fields:

    Building Permits:
        - id
        - application_start_date
        - issue_date
        - permit_status
        - permit_type
        - work_type
        - review_type
        - total_fee
        - reported_cost
        - community_area (number)
        - latitude
        - longitude

        - Zip code
    
    Census Data:
        - community_area_number
        - community_area_name
        - percent_households_below_poverty
        - percent_aged_16_unemployed
        - per_capita_income

    Covid Cases:
        - All

    Covid Vulnerability Index:
        - community_area_or_zip
        - community_area_name
        - ccvi_category

    Public Health Statistics:
        - community_area
        - community_area_name
        - below_poverty_level
        - unemployment
        - per_capita_income
    
    Taxi Trips:
        - All

        - Zip code
    
    Transportation Trips:
        - All
        
        - Zip code