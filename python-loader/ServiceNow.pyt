# -*- coding: utf-8 -*-
# Copyright 2019 Esri.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

import arcpy
import requests
import json
import sys
from urllib.parse import urlparse
import urllib.parse

class AuthError(Exception):
    pass

class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "ServiceNow"
        self.alias = "ServiceNow"

        # List of tool classes associated with this toolbox
        self.tools = [ServiceNowLocationLoader]

class ServiceNowLocationLoader(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "ServiceNow Location Loader"
        self.description = "ServiceNow Location Loader"
        self.canRunInBackground = False
        self.data = {}

        # AIIM details

        # NOTE: Some of these fields do not exist in the default Facilities fc so must be added before running this tool.
        # self.facilities_fields = ["NAME", "ADDRESS", "LOCALITY", "PROVINCE", "POSTAL_CODE", "COUNTRY"]
        self.facilities_fields = ["NAME"]
        self.facilities_optional_fields = ["ADDRESS", "LOCALITY", "PROVINCE", "POSTAL_CODE", "COUNTRY"]

        # NOTE: The field(s) "FACILITY_NAME" does/do not exist in the default Levels fc so we will have to look up the NAME in Facilities using the FACILITY_ID in the Levels fc.
        self.levels_fields = ["NAME", "FACILITY_ID", "LEVEL_ID"]

        # NOTE: The field(s) "LEVEL_NAME","FACILITY_NAME" does/do not exist in the default Units fc so we will have to look them up in their respective fc's using LEVEL_ID in Units and using FACILITY_ID in Facilities
        self.units_fields = ["NAME", "LEVEL_ID"]

        # e.g. "1000.US01.MAIN.O": "RED O"
        self.facility_id_to_name_lookup = {}

        # e.g. "1000.US01.MAIN.O1": "O1"
        self.level_id_to_name_lookup = {}

        # e.g. "1000.US01.MAIN.O1": "RED O"
        self.level_id_to_facility_name_lookup = {}


        # Note:
        #   Polygons and lines don't support the 'SHAPE@Z' field since they have many vertices, potentially with different Z values.
        #   In order to get any meaningful Z, one could use the centroid.  This is done by using the 'SHAPE@' field which returns a
        #   geometry object with a centroid.Z property. Also, if control over the z value units (e.g. meters) is needed, a vertical
        #   coordinate system would need to be included in arcpy.SpatialReference, e.g. 115700.
        self.shape_fields = ["SHAPE@X", "SHAPE@Y"]
        self.facilities_fc = "Facilities"
        self.levels_fc = "Levels"
        self.units_fc = "Units"

        # Other params
        self.spatial_reference_id = 4326
        # Cursor sort by field index for facility (this is by order of the shape_fields followed by the facilities_fields)
        self.fac_sort_index = 2
        # Cursor sort by field index for levels/units (this is by order of the shape_fields followed by the levels_fields and units_fields)
        self.other_sort_index = 3
        self.address_list = []

        # ServiceNow parameters
        self.query_param = "sysparm_query"
        self.field_param = "sysparm_fields"
        self.limit_param = "sysparm_limit"
        self.limit_value = "10000"
        self.name_field = "name"
        self.full_name_field = "full_name"
        # self.parent_field = "parent" # UNUSED
        self.sys_id_field = "sys_id"
        self.delimiter = "/"
        # ServiceNow cmn_location table fields
        self.longitude = "longitude"
        self.latitude = "latitude"
        self.name = "name"
        self.parent = "parent"
        self.street = "street"
        self.city = "city"
        self.state = "state"
        self.zip = "zip"
        self.country = "country"
        self.level_id = "u_level_id" # Custom field required to be added to ServiceNow. This represents Indoors LEVEL_ID.

        # Validation Messages
        self.api_error = "Unable to connect to ServiceNow Rest API"
        self.invalid_input = "Input layer or feature class does not exist"

    def getParameterInfo(self):

        """Define parameter definitions"""
        facility_layer = arcpy.Parameter(
            displayName="Facilities Layer",
            name="in_facility_layer",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )
        facility_layer.filter.list = ["Polygon"]
        level_layer = arcpy.Parameter(
            displayName="Levels Layer",
            name="in_level_layer",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )
        level_layer.filter.list = ["Polygon"]
        unit_layer = arcpy.Parameter(
            displayName="Units Layer",
            name="in_unit_layer",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )
        unit_layer.filter.list = ["Polygon"]

        keep_duplicate = arcpy.Parameter(
            displayName="Keep Duplicate Values",
            name="keepDuplicates",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input"
        )
        keep_duplicate.filter.list = ["KEEP_DUPLICATE_VALUES", "NO_DUPLICATE_VALUES"]
        keep_duplicate.value = "NO_DUPLICATE_VALUES"

        servicenow_url = arcpy.Parameter(
            displayName="ServiceNow Rest URL",
            name="in_servicenow_url",
            datatype="GPString",
            parameterType="Required",
            direction="Input"
        )
        user_id = arcpy.Parameter(
            displayName="ServiceNow Username",
            name="in_username",
            datatype="GPString",
            parameterType="Optional",
            direction="Input"
        )
        pwd = arcpy.Parameter(
            displayName="ServiceNow Password",
            name="in_password",
            datatype="GPStringHidden",
            parameterType="Optional",
            direction="Input"
        )
        return [facility_layer, level_layer, unit_layer, keep_duplicate, servicenow_url, user_id, pwd]

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""

        # Workspace, Feature Class, Fields and No record Validation
        facilities_layer = parameters[0].value
        if facilities_layer:
            self.validateInput(facilities_layer, parameters[0], self.facilities_fc, self.facilities_fields)

        levels_layer = parameters[1].value
        if levels_layer:
            self.validateInput(levels_layer, parameters[1], self.levels_fc, self.levels_fields)

        units_layer = parameters[2].value
        if units_layer:
            self.validateInput(units_layer, parameters[2], self.units_fc, self.units_fields)

        # ServiceNow URL validation
        servicenow_url = parameters[4].valueAsText
        if servicenow_url:
            parse_result = urlparse(servicenow_url)
            scheme = parse_result[0]
            netloc = parse_result[1]
            if not scheme or not netloc:
                parameters[4].setErrorMessage("Invalid URL")
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        try:
            # ServiceNow Details
            servicenow_url = parameters[4].valueAsText
            user_id = parameters[5].valueAsText
            pwd = parameters[6].valueAsText

            # Overwrite/Keep duplicates
            keep_duplicate = parameters[3].value

            facilities_layer = parameters[0].value
            levels_layer = parameters[1].value
            units_layer = parameters[2].value

            # NOTE: Future enhancement: Make facilities_parent a user variable
            facilities_parent = ""
            # facilities_parent = "Americas"

            arcpy.AddMessage(f"Facilities and descendent locations will be added into the hierarchy underneath '{facilities_parent}'" + (" (root)" if facilities_parent == "" else ""))

            # Generate lookup dictionaries for use in generateJSON
            self.generateLookups(facilities_layer, levels_layer, units_layer)

            # Processing Facilities
            add_fields = []
            for field_name in self.facilities_optional_fields:
                if self.fieldExists(facilities_layer, field_name):
                    add_fields.append(field_name)

            self.generateJSON(facilities_layer, self.shape_fields + self.facilities_fields + add_fields, servicenow_url, user_id, pwd, keep_duplicate, facilities_parent)

            # Processing Levels
            self.generateJSON(levels_layer, self.shape_fields + self.levels_fields, servicenow_url, user_id, pwd, keep_duplicate, facilities_parent)

            # Processing Units
            self.generateJSON(units_layer, self.shape_fields + self.units_fields, servicenow_url, user_id, pwd, keep_duplicate, facilities_parent)

        except Exception as ex:
            arcpy.AddError(str(ex))
            sys.exit(0)

    # Post (create) records in servicenow
    def postData(self, servicenow_url, user_id, pwd, json_data):
        # arcpy.AddMessage(f"Posting data: {json_data}")
        try:
            # Setting Header and post request
            headers = {"Content-Type": "application/json", "Accept": "application/json"}
            response = requests.post(servicenow_url, auth=(user_id, pwd), headers=headers, data=json_data)

            if response.status_code and response.status_code != 201:
                raise AuthError
            return

        except AuthError as ex:
            pymsg = "Status: " + str(response.status_code) + "\n Error: " + str(response.json()["error"]["message"])
            arcpy.AddError(pymsg)
            sys.exit(0)
        except Exception as ex:
            arcpy.AddError(self.api_error)
            arcpy.AddError(str(ex))
            sys.exit(0)

    # Get records from servicenow
    def getData(self, servicenow_url, user_id, pwd):
        try:
            # Setting Header and get request
            headers = {"Content-Type": "application/json", "Accept": "application/json"}
            response = requests.get(servicenow_url, auth=(user_id, pwd), headers=headers)

            if response.status_code and response.status_code != 200:
                raise AuthError
            data = response.json()
            return data

        except AuthError as ex:
            pymsg = "Status: " + str(response.status_code) + "\n Error: " + str(response.json()["error"]["message"])
            arcpy.AddError(pymsg)
            sys.exit(0)
        except Exception as ex:
            arcpy.AddError(self.api_error)
            arcpy.AddError(str(ex))
            sys.exit(0)

    # Update records in servicenow. Using PATCH request instead of PUT to avoid passing the entire payload
    def updateData(self, servicenow_url, user_id, pwd, json_data):
        # arcpy.AddMessage(f"Updating data: {json_data}")
        try:
            # Setting Header and Patch request.
            headers = {"Content-Type": "application/json", "Accept": "application/json"}
            response = requests.patch(servicenow_url, auth=(user_id, pwd), headers=headers, data=json_data)

            if response.status_code and response.status_code != 200:
                raise AuthError
            return

        except AuthError as ex:
            pymsg = "Status: " + str(response.status_code) + "\n Error: " + str(response.json()["error"]["message"])
            arcpy.AddError(pymsg)
            sys.exit(0)
        except Exception as ex:
            arcpy.AddError(self.api_error)
            arcpy.AddError(str(ex))
            sys.exit(0)

    def generateLookups(self, facilities_layer, levels_layer, units_layer):

        # Populate Facilities lookup
        with arcpy.da.SearchCursor(facilities_layer, ["FACILITY_ID", "NAME"]) as cursor:
            self.facility_id_to_name_lookup = {feature[0]: feature[1] for feature in cursor}

        # Populate Levels lookups
        with arcpy.da.SearchCursor(levels_layer, ["LEVEL_ID", "NAME", "FACILITY_ID"]) as cursor:
            for feature in cursor:
                # level id (feature[0]) maps to level name (feature[1])
                self.level_id_to_name_lookup[feature[0]] = feature[1]

                # level id (feature[0]) maps to facility name through facility id (feature[2])
                self.level_id_to_facility_name_lookup[feature[0]] = self.facility_id_to_name_lookup[feature[2]] if feature[2] in self.facility_id_to_name_lookup else None

    def generateJSON(self, layer, fields, servicenow_url, user_id, pwd, keep_duplicate, facilities_parent):
        try:
            query_data = False
            parent_facility = ""
            parent_level = ""
            node_full_name = ""
            desc_layer = arcpy.Describe(layer)
            arcpy.AddMessage(f"\nProcessing {desc_layer.name}")
            # arcpy.AddMessage(f"\nFields {(','.join(fields))}")

            # Constructing Get Request
            # Outfields
            query_param = "?" + self.field_param + "=" + self.full_name_field + "," + self.sys_id_field + "&" + self.limit_param + "=" + self.limit_value
            # Encoding URL
            encoded_query = urllib.parse.quote(query_param, safe="?&=")

            # This is being done here to support the Facilities layer call for this method.
            arcpy.AddMessage(f"Layer {desc_layer.name}: Querying ServiceNow location data")
            # Querying ServiceNow location data
            get_data = self.getData(servicenow_url + encoded_query, user_id, pwd)
            # Returning value for result key in dict data
            get_data_result = get_data["result"]

            # Set spatial reference based on declared wkid
            spatial_reference = arcpy.SpatialReference(self.spatial_reference_id)

            with arcpy.da.SearchCursor(layer, fields, None, spatial_reference) as cursor:

                # Sorting cursor elements based on fields derived to sort
                # sort_feature is a tuple where 0 is a X/longitude (float), 1 is Y/latitude (float), 2 to n-1 are the fields
                cursor = sorted(
                    cursor,
                    key=lambda sort_feature: (
                        sort_feature[self.fac_sort_index] if desc_layer.name.lower() == self.facilities_fc.lower() else sort_feature[self.other_sort_index]
                    ),
                    reverse=False
                )

                if len(cursor) > 0:

                    arcpy.AddMessage(f"Processing {len(cursor)} {desc_layer.name}")
                    for feature in cursor:
                        # arcpy.AddMessage(feature)
                        address_dict = {}

                        # Initialize hierarchy list for each feature
                        hierarchy_list = []
                        if facilities_parent:
                            hierarchy_list.append(facilities_parent)

                        # Code block for Facilities
                        if desc_layer.name.lower() == self.facilities_fc.lower():

                            field_names = [field_name.upper() for field_name in fields]
                            # arcpy.AddMessage(f"field_names = {','.join(field_names)}")

                            # Get optional fields indices
                            street_i = field_names.index("ADDRESS") if "ADDRESS" in field_names else None
                            city_i = field_names.index("LOCALITY") if "LOCALITY" in field_names else None
                            state_i = field_names.index("PROVINCE") if "PROVINCE" in field_names else None
                            zip_code_i = field_names.index("POSTAL_CODE") if "POSTAL_CODE" in field_names else None
                            country_i = field_names.index("COUNTRY") if "COUNTRY" in field_names else None

                            facility_name = feature[2]
                            street = feature[street_i] if street_i else ""
                            city = feature[city_i] if city_i else ""
                            state = feature[state_i] if state_i else ""
                            zip_code = feature[zip_code_i] if zip_code_i else ""
                            country = feature[country_i] if country_i else ""

                            address_dict["NAME"] = facility_name
                            address_dict["ADDRESS"] = [street, city, state, zip_code, country]
                            self.address_list.append(address_dict)

                            hierarchy_list.append(facility_name)

                            parent_name = facilities_parent
                            parent_full_name = ""
                            if facilities_parent:
                                parent_full_name = facilities_parent
                            full_name = self.delimiter.join(filter(None, hierarchy_list[0 : len(hierarchy_list)]))

                            # Default to full name is facility name and no parent
                            node_full_name = facility_name
                            self.data[self.parent] = ""

                            # If a parent to the facility was specified, use it
                            if parent_full_name:
                                # Check if parent exists
                                # Parameters: queryParent(name, parent_name, parent_full_name, full_name, get_data_result, layer)
                                node_full_name = self.queryParent(facility_name, parent_name, parent_full_name, full_name, get_data_result, layer)

                            # arcpy.AddMessage(f"Location: {node_full_name}")
                            arcpy.AddMessage(f"--Processing Facility '{node_full_name}'")
                            self.createDict(feature, address_dict["ADDRESS"])


                        # Code block for Levels
                        elif desc_layer.name.lower() == self.levels_fc.lower():
                            level_name = feature[2]
                            facility_id = feature[3]
                            level_id = feature[4]

                            # Update: Since the facility name is unavailable in Levels now, instead of doing this, look this up in Facilities using FACILITY_ID
                            # facility_name = feature[3]
                            facility_name = self.facility_id_to_name_lookup[facility_id]

                            # arcpy.AddMessage(f"Level Name: {level_name}")
                            # arcpy.AddMessage(f"Facility Name: {facility_name}")

                            hierarchy_list.append(facility_name)
                            hierarchy_list.append(level_name)

                            parent_name = facility_name
                            parent_full_name = self.delimiter.join(filter(None, hierarchy_list[0 : len(hierarchy_list) - 1]))
                            full_name = self.delimiter.join(filter(None, hierarchy_list[0 : len(hierarchy_list)]))

                            arcpy.AddMessage(f"--Processing Level '{full_name}'")
                            # arcpy.AddMessage(f"----Level Name: '{level_name}', Level ID: '{level_id}', Facility Name: '{facility_name}'")

                            # Get latest ServiceNow location data
                            if not query_data:
                                # arcpy.AddMessage("Levels: Querying ServiceNow location data")
                                # Querying ServiceNow location data
                                get_data = self.getData(servicenow_url + encoded_query, user_id, pwd)
                                # Returning value for result key in dict data
                                get_data_result = get_data["result"]
                                query_data = True

                            # Check if parent exists
                            # Parameters: queryParent(name, parent_name, parent_full_name, full_name, get_data_result, layer)
                            node_full_name = self.queryParent(level_name, parent_name, parent_full_name, full_name, get_data_result, layer)
                            # arcpy.AddMessage(f"Location: {node_full_name}")

                            # Adding Address Information
                            if self.address_list:
                                address = [item for item in self.address_list if item["NAME"] == facility_name]
                                if address:
                                    full_address = (address[0])["ADDRESS"]
                                    self.createDict(feature, full_address)
                                else:
                                    self.createDict(feature, address="")
                            else:
                                self.createDict(feature, address="")

                            # Set the LEVEL_ID for this Level
                            self.data[self.level_id] = level_id

                        # Code Block for Units
                        elif desc_layer.name.lower() == self.units_fc.lower():
                            level_id = feature[3]
                            # facility_name = feature[4]
                            facility_name = self.level_id_to_facility_name_lookup[level_id]

                            # level_name = feature[3]
                            level_name = self.level_id_to_name_lookup[level_id]

                            unit_name = feature[2]

                            # arcpy.AddMessage(f"Unit Name: {unit_name}")
                            # arcpy.AddMessage(f"Level Name: {level_name}")
                            # arcpy.AddMessage(f"Facility Name: {facility_name}")

                            hierarchy_list.append(facility_name)
                            hierarchy_list.append(level_name)
                            hierarchy_list.append(unit_name)

                            parent_name = level_name if level_name else facility_name
                            parent_full_name = self.delimiter.join(filter(None, hierarchy_list[0 : len(hierarchy_list) - 1]))
                            full_name = self.delimiter.join(filter(None, hierarchy_list[0 : len(hierarchy_list)]))

                            # Displaying processing message on facility change only
                            if facility_name and parent_facility != facility_name:
                                arcpy.AddMessage(f"--Processing Facility '{facility_name}' Units")
                            # Displaying processing message on level change only
                            if level_name and parent_level != level_name:
                                arcpy.AddMessage(f"----Processing Level '{level_name}' Units")

                            # These are only used to avoid over-displaying processing messages above
                            parent_facility = facility_name
                            parent_level = level_name

                            # arcpy.AddMessage(f"Parent Facility: {parent_facility}")
                            # arcpy.AddMessage(f"Parent Level: {parent_level}")
                            # arcpy.AddMessage(f"Full Name: {full_name}")
                            # arcpy.AddMessage(f"Hierarchy List: {str(hierarchy_list)}")

                            arcpy.AddMessage(f"------Processing Unit '{full_name}'")

                            # Get latest ServiceNow location data
                            if not query_data:
                                # arcpy.AddMessage("Units: Querying ServiceNow location data")
                                # Querying ServiceNow location data
                                get_data = self.getData(servicenow_url + encoded_query, user_id, pwd)
                                # Returning value for result key in dict data
                                get_data_result = get_data["result"]
                                query_data = True

                            # Check if parent exists
                            node_full_name = self.queryParent(unit_name, parent_name, parent_full_name, full_name, get_data_result, layer)
                            # arcpy.AddMessage(f"Location: {node_full_name}")

                            # Adding Address Information
                            if self.address_list:
                                address = [item for item in self.address_list if item["NAME"] == facility_name]
                                if address:
                                    full_address = (address[0])["ADDRESS"]
                                    self.createDict(feature, full_address)
                                else:
                                    self.createDict(feature, address="")
                            else:
                                self.createDict(feature, address="")

                            # Set the LEVEL_ID for this Unit
                            self.data[self.level_id] = level_id

                        json_data = json.dumps(self.data)

                        # If this is the Levels or Units layer, print out the pending JSON data (for debugging purposes only)
                        if desc_layer.name.lower() == self.levels_fc.lower() or desc_layer.name.lower() == self.units_fc.lower():
                            pass
                            # arcpy.AddMessage(json_data)

                        if keep_duplicate is False:

                            # Adding validation for dict keys if location table is empty
                            if self.full_name_field in get_data_result[0] and self.sys_id_field in get_data_result[0]:
                                result = [item for item in get_data_result if item[self.full_name_field] == node_full_name]
                                if result:
                                    sys_id = (result[0])["sys_id"]
                                    self.updateData(servicenow_url + "/" + sys_id, user_id, pwd, json_data)
                                else:
                                    self.postData(servicenow_url, user_id, pwd, json_data)
                            else:
                                self.postData(servicenow_url, user_id, pwd, json_data)
                        else:
                            self.postData(servicenow_url, user_id, pwd, json_data)
                    self.data = {}
                else:
                    arcpy.AddWarning(f"No records found in {desc_layer.name}")
            return

        except Exception as ex:
            arcpy.AddError(str(ex))
            sys.exit(0)

    # Constructing response dict
    def createDict(self, feature, address):
        # arcpy.AddMessage(feature)
        self.data[self.longitude] = str(feature[0])
        self.data[self.latitude] = str(feature[1])
        self.data[self.name] = feature[2]
        if address:
            self.data[self.street] = address[0]
            self.data[self.city] = address[1]
            self.data[self.state] = address[2]
            self.data[self.zip] = address[3]
            self.data[self.country] = address[4]
        return

    # To Validate parent items to build/maintain location hierarchy in ServiceNow
    def queryParent(self, name, parent_name, parent_full_name, full_name, get_data_result, layer):

        try:
            if parent_full_name:
                # Checking if parent feature exists on ServiceNow
                if self.full_name_field in get_data_result[0]:
                    result = [item for item in get_data_result if item[self.full_name_field] == parent_full_name]
                    if result:
                        self.data[self.parent] = parent_name
                        return full_name
                    else:
                        self.data[self.parent] = ""
                        arcpy.AddWarning(f"{full_name} does not have a parent feature in ServiceNow")
                        return name
                else:
                    self.data[self.parent] = ""
                    arcpy.AddWarning(f"{full_name} does not have a parent feature in ServiceNow")
                    return name
            else:
                self.data[self.parent] = ""
                # Displaying warning if parent missing
                arcpy.AddWarning(f"{full_name} does not have a parent feature in {arcpy.Describe(layer).name} input layer")
                return name

        except Exception as ex:
            arcpy.AddError(str(ex))

    # To validate input fields
    def validateInput(self, layer, parameter, layer_fc, layer_fields):
        try:
            if arcpy.Exists(layer):
                if arcpy.Describe(layer).name.lower() == layer_fc.lower():
                    if self.fieldsExist(layer, layer_fields, parameter) is True:
                        # Setting up warning if zero records in feature layer
                        feature_count = arcpy.GetCount_management(layer)
                        if feature_count == 0:
                            parameter.setWarningMessage(f"No records in {layer_fc}.")
                else:
                    parameter.setErrorMessage(f"Input {layer_fc} layer or feature class.")
            else:
                parameter.setErrorMessage(self.invalid_input)
            return

        except Exception as ex:
            arcpy.AddError(str(ex))

    # To validate if all the fields in a list exist in the supplied layer or feature class
    def fieldsExist(self, layer, field_list, parameter):
        try:
            if field_list is not None:
                fields = arcpy.ListFields(layer)
                field_names = [field.name.lower() for field in fields]
                for field_name in field_list:
                    if field_name.lower() not in field_names:
                        parameter.setErrorMessage(f"{field_name} field not found in {arcpy.Describe(layer).name}.")
                        return False
            return True
        except Exception as ex:
            arcpy.AddError(str(ex))

    # To validate if a field exists in the supplied layer or feature class. This is a helper to help identify if optionally-supplied fields exist.
    def fieldExists(self, layer, field_name):
        try:
            fields = arcpy.ListFields(layer)
            field_names = [field.name.lower() for field in fields]
            return field_name.lower() in field_names
        except Exception as ex:
            arcpy.AddError(str(ex))
