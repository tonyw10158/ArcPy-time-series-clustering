# %% Imports & libraries; declare directories & relevant files.
import arcpy
from os import getcwd

# Set the directories; read the relevant layers into variables.
in_dir = getcwd()
arcpy.env.workspace = in_dir + "\\Working"
arcpy.env.overwriteOutput = True
# Collection of feature layers.
all_territories = "Longitude_Graticules_and_World_Countries_Boundaries.shp"
sl_admin_regions = "sle_admbnda_adm2_1m_gov_ocha_20161017.shp"

# %% Data preprocessing, cleaning, and preparation.
# Use select analysis on the all territories dataset to select Guinea, and Liberia.
arcpy.Select_analysis(all_territories, "Guinea-Liberia.shp", where_clause="CNTRY_NAME IN ('Guinea', 'Liberia')")
# From the xlsx conflict file, extract table to points.
arcpy.XYTableToPoint_management("sll-ledsierre-leonelocalsource1991-2001.csv", "Events_init.shp", "LONGITUDE",
                                "LATITUDE")


# Due to incorrect entries, events should not be clipped. Instead, define a function to fix or subset the events data.
def fix_events(field_name, field_value, action):
    '''
    This function is used to clean the initial events data based on the input, provided as a dictionary.

    Parameters
    ------
    field_name:
        Takes an attribute field name.
    field_value (optional):
        The value inside the field name to be operated upon, as the exact value presented inside the table.
    action:
        The action to be performed on the row, taken as a single string value.
        Values can be 'delete' or 'switch_sign'.
    ------
    Outputs a modified shapefile of events with an updated attribute table.
    '''
    # Convert the action string to lower and field name string to upper.
    action = action.lower()
    field_name = field_name.upper()
    # Test the method parameter inputs.
    try:
        # Create the column to change and attach it as a cursor.
        with arcpy.da.UpdateCursor("Events_init.dbf", field_name) as cursor:
            # Loop through each row in the cursor; find the values to be adjusted.
            for row in cursor:
                # Check if the 'delete' action has been specified and the row has reached the field name.
                if field_value is not None and action == 'delete' and row[0] == field_value:
                    cursor.deleteRow()
                    continue
                # Check if the action is set to 'switch_sign'.
                elif field_value is None and action == 'switch_sign' and row[0] > 0:
                    # Switch incorrectly entered positive double datatype to a negative.
                    row[0] = -row[0]
                    continue
                # Update the cursor after an action has been performed.
                cursor.updateRow(row)
    # Raise an exception if a method parameter input is violated.
    except NameError:
        print("Parameter value not found for the specific attribute table.")


# Call the fix events method to remove events in Nigeria and fix incorrectly entered locations.
fix_events('country', 'Nigeria', 'Delete')
fix_events('longitude', None, 'switch_sign')

# Re-read the fixed table set back into points.
arcpy.CopyRows_management("Events_init.shp", "Events.csv")
arcpy.XYTableToPoint_management("Events.csv", "Events.shp", "LONGITUDE", "LATITUDE")

# Use select analysis to isolate RUF-established headquarter sites.
arcpy.Select_analysis("Events.shp", "RUF_headquarters.shp", where_clause="ACTOR1 LIKE '%RUF: Revolutionary United "
                                                                         "Front%' And EVENT_TYPE IN ('Headquarters or"
                                                                         " base established')")

# %% Construct possible regions with RUF influence.

# Begin by generating a hexagon tessellation which covers all of Sierra Leone with Liberia & Guinea border.
# This algorithm aggregates nearby activity into polygons with varying intensity.
sl_extent = arcpy.Describe(sl_admin_regions).extent
arcpy.GenerateTessellation_management("SLtessellation.shp", sl_extent, "HEXAGON", "36 SquareMiles")

# Only select polygons that include an event; then join the layer with RUF headquarters.
selection = arcpy.SelectLayerByLocation_management("SLtessellation.shp", "INTERSECT", "RUF_headquarters.shp")
arcpy.SpatialJoin_analysis(selection, "RUF_headquarters.shp", "Headquarter_counts.shp")

# Generate a report on the significance of the aggregations based on spatial autocorrelations and kNN.
arcpy.SpatialAutocorrelation_stats("Headquarter_counts.shp", "Join_Count", "GENERATE_REPORT", "INVERSE_DISTANCE",
                                   "EUCLIDEAN_DISTANCE", "ROW")
arcpy.AverageNearestNeighbor_stats("RUF_headquarters.shp", "EUCLIDEAN_DISTANCE", "GENERATE_REPORT")

# Use the DBSCAN algorithm to forge clusters.
arcpy.DensityBasedClustering_stats("RUF_headquarters.shp", "RUF_headquarters_DBSCAN.shp", "DBSCAN", 10)

# Now select all RUF-related activities; transform this dataset to the Africa_Sinusoidal projection, which covers
# much of Africa.
arcpy.Select_analysis("Events.shp", "RUF_activities.shp", where_clause="ACTOR1 LIKE '%RUF: Revolutionary United%'")
arcpy.Project_management("RUF_activities.shp", "RUF_activities_Project.shp", arcpy.SpatialReference(102011))

# Declare a dictionary to hold months in digit format.
month_dictionary = {'January': '01', 'February': '02', 'March': '03', 'April': '04', 'May': '05', 'June': '06',
                    'July': '07', 'August': '08', 'September': '09', 'October': '10', 'November': '11',
                    'December': '12'}

# Attach the headquarter data as a cursor; adjust the date field for compatibility.
with arcpy.da.UpdateCursor("RUF_activities_Project.dbf", "EVENT_DATE") as cursor:
    # Loop through each row in the cursor and convert the string months into their numerical counterparts.
    for row in cursor:
        row[0] = list(row[0].split(" "))
        # If the date field is a single digit, fit a 0 to the beginning.
        if len(row[0][0]) == 1:
            row[0][0] = '0' + row[0][0]
        row[0][1] = month_dictionary.get(row[0][1])
        row[0] = "/".join(row[0])
        cursor.updateRow(row)

# Convert the time field for RUF headquarters.
arcpy.ConvertTimeField_management("RUF_activities_Project.dbf", "EVENT_DATE", "dd/MM/yyyy", "CONV_DATE")

# Create a space-time cube.
arcpy.CreateSpaceTimeCube_stpm("RUF_activities_Project.shp", "RUF_activities.nc", "CONV_DATE",
                               time_step_interval="1 Months", distance_interval="8 Kilometers",
                               aggregation_shape_type="HEXAGON_GRID")

# Deploy the time-series clustering algorithm on the space-time cube.
arcpy.TimeSeriesClustering_stpm("RUF_activities.nc", "COUNT", "RUF_activities_TSCluster.shp", "PROFILE_FOURIER",
                                shape_characteristic_to_ignore="RANGE", enable_time_series_popups="CREATE_POPUP")


# %% Delete redundant files.
def delete(layers):
    for layer in layers:
        if arcpy.Exists(layer):
            arcpy.Delete_management(layer)


layers = ['Events_init.shp', 'Events.shp', 'Headquarter_counts.shp', 'RUF_activities', 'RUF_activities_Project',
          'RUF_headquarters.shp', 'SLtessellation.shp']
delete(layers)
