# nifi-format-json-date-processor
Processor to easily format a json fields from a specific format to a another format. You can provide a list of fields providing a string splitted by comma-separator.

## Input fields:

<b>JSON Properties</b>: A single or a list of JSON property names. You can specify a list of properties with a comma separator. <br />

<b>Current Date Format</b>: Specify the current date format of the field. E.g: d/M/yyyy <br />

<b>New Date Format</b>: Specify the new date format of the field. E.g: dd/MM/yyyy <br />

## Output:
Json flowfile with date fields with a new format.
