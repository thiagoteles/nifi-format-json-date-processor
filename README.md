# nifi-format-json-date-processor
Processor to easily format a json field from a specific format to a another format. You can provide the fields to change by a string with comma separator.

Input fields:

JSON Properties: A single or a list of JSON property names. You can specify a list of properties with a comma separator.
Current Date Format: Specify the current date format of the field. E.g: d/M/yyyy
New Date Format: Specify the new date format of the field. E.g: dd/MM/yyyy

Output:
Json flowfile with date fields with a new format.
